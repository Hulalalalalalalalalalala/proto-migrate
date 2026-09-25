"""Cross-group, reference-integrity migration of linked JSONL log groups.

This builds on :mod:`proto_migrate.group_migration` (one all-or-nothing
group of logs) and adds *references between groups*: records of one
group name a record of another group by a key field -- for example a
detail record whose ``order_id`` points at an order record in the
orders group.  :func:`migrate_linked_logs` migrates every group to the
current record version **and** enforces referential integrity in one
atomic unit: either every group is migrated with every reference
resolving to an existing, complete target record, or every group stays
exactly as it was.

Link declarations
-----------------
References are declared by the caller; nothing is guessed from file
contents.  ``links`` is a sequence of
``(src_group, dst_group, src_field, dst_field)`` tuples: a record in
group ``src_group`` whose string field ``src_field`` has value ``k``
*references* the record(s) of group ``dst_group`` whose string field
``dst_field`` equals ``k``.  Group indices refer to the ``groups``
argument order.  A record with several declared source fields carries
one reference per field; a non-string field value is not a reference.

Bad references
--------------
Four classes of bad references are recognised:

  1. **dangling** -- no live record in the target group has the
     referenced key;
  2. **target skipped** -- the referenced key belongs to a line that
     was skipped as a bad record (or to a record dropped because its
     own references are bad);
  3. **cyclic** -- the reference sits on a record-level reference
     cycle (a record on a cycle has all of its outgoing references
     rejected);
  4. **illegal target version** -- the referenced key belongs to a
     line whose version key is missing, non-integer or unsupported.

In ``"strict"`` mode the first bad line *or* bad reference of the whole
run (groups in list order, members in list order, lines in file order)
raises :class:`LinkedBadReferenceError` / :class:`GroupBadRecordError`
(both :class:`ValueError`) before any rename, leaving every original
untouched: bad lines and bad references are compared in that global
order and the earliest one decides which exception is raised, so a bad
reference that sorts before a later member's bad line is reported
first.  In ``"skip"`` mode bad lines are skipped exactly as in the
single-group migration and records holding bad references are dropped
from the migrated output; each dropped record is audited to the audit
stream as ``<filename>:<lineno>:<first 32 raw bytes>``.  The summary
counts bad records (``records_skipped``) and bad references
(``references_bad``) separately, and both counters only cover records
this invocation newly migrated and references it newly resolved.

Multi-instance coordination
---------------------------
Several migration instances may run concurrently, over overlapping or
disjoint group sets.  Every member is held under a non-blocking lease
-- an exclusive ``flock`` on its ``<path>.migrate.lock`` file, the same
lock the single-file and single-group migrators wait on -- so
overlapping members are mutually exclusive while disjoint group sets
advance in parallel (each group set owns a distinct
``.migrate-linked-tmp-<hash>`` work directory next to its first
member).  A lease held by a live instance raises
:class:`MigrationLockedError` instead of waiting; a holder that
finishes or disappears (its process exits, however abruptly) releases
its leases via the OS, and the next instance reclaims them and takes
over from the durable checkpoints without rescanning prepared members.
Takeover and resume leave no partial migration shape: the outcome is
byte-for-byte identical to running the same instances serially.  Lease
and index state lives only in local files and can always be rebuilt.

Durability, resume and snapshots
--------------------------------
Every member is prepared with the same segmented temp output and
durable checkpoints as a single-group run, plus a durable per-line
index (``lines`` in the member work directory) classifying every
consumed source line.  Reference resolution is streaming: keys and
edges live in an SQLite spill file inside the group work directory, so
no group is ever loaded into memory wholesale.  A killed run resumes
prepared members from their checkpoints and line indexes without
rescanning them, rebuilds the reference index from those durable
indexes, and finishes byte-for-byte identical to one uninterrupted
run.  The group commit marker ``linked-committed`` is the single
success/failure border; durability or cleanup failures after it are
warnings (exit 0), never failures.  A recovery run that finds the
commit marker converges appends still living on a member's backup
inode *before* the backup is removed, so no record appended across the
crash is lost.

:func:`read_linked_logs` returns one version-consistent snapshot of
every group in a single call -- old and new field shapes never mix
between groups, between members or inside a member, including while a
path-reopening appender writes old records during wrap-up.
:func:`read_linked_logs_stream` serves the same snapshot in
cursor-resumable batches (group by group, member by member), pinned at
the first call and never loading a whole group into memory.
"""

from __future__ import annotations

import argparse
import contextlib
import fcntl
import hashlib
import itertools
import json
import os
import shutil
import sqlite3
import struct
import sys
import threading
import time
from collections.abc import Sequence
from typing import NamedTuple

from . import CURRENT_VERSION, loads, migrate
from .log_migration import (
    DEFAULT_QUIESCE,
    DEFAULT_SEGMENT_SIZE,
    EXIT_BAD_RECORD,
    EXIT_ERROR,
    EXIT_OK,
    EXIT_USAGE,
    LINE_BAD_VERSION,
    LINE_GOOD,
    BadRecordError,
    MigrationResult,
    _audit_line,
    _commit_marker_path,
    _converge,
    _convert,
    _crash_point,
    _emit,
    _fsync_dir,
    _read_lines,
)
from .group_migration import (
    GroupBadRecordError,
    _AuditPrefix,
    _prepare_member,
    _probe_members,
    _publish_member_marker,
    _read_checkpoint_log,
    _read_member_gate,
)

__all__ = [
    "LinkedBadReferenceError",
    "LinkedMigrationResult",
    "MigrationLockedError",
    "migrate_linked_logs",
    "read_linked_logs",
    "read_linked_logs_stream",
    "close_linked_logs_stream",
    "run_linked_cli",
]

_LINKED_DIR_NAME = ".migrate-linked-tmp"
_LINKED_COMMITTED = "linked-committed"
_LINKED_COMMITTED_TMP = "linked-committed.tmp"
_MANIFEST = "manifest.json"
_LINKED_PREPARED = "linked-prepared"
_LINKED_FINAL = "linked-final"
_LINES_NAME = "lines"
_REFS_DB = "refs.sqlite3"
_FINAL = "final"
# Online compaction state: the durable reference-resolution record in
# the group work directory, and the per-member compact receipt in each
# member work directory.  Both are declared in README.md ("Linked work
# files"); they live only in local files and can always be rebuilt.
_LINKED_RESOLVED = "linked-resolved"
_COMPACTED = "compacted"

_MEMBER_TMP_SUFFIX = ".migrate-linked-tmp"
_BACKUP_SUFFIX = ".migrate-linked-backup"
_STAGED_SUFFIX = ".migrate-linked-staged"

# Mirrors log_migration._FAULT_ENV; post-border cleanup faults injected
# through this hook are warnings rather than failures.
_FAULT_ENV = "PROTO_MIGRATE_FAULT_AT"

_REASON_MISSING = 1
_REASON_TARGET_SKIPPED = 2
_REASON_BAD_VERSION = 3
_REASON_CASCADE = 4
_REASON_CYCLE = 5

_REASON_TEXT = {
    _REASON_MISSING: "dangling reference",
    _REASON_TARGET_SKIPPED: "reference target was skipped as a bad record",
    _REASON_BAD_VERSION: "reference target has an illegal version key",
    _REASON_CASCADE: "reference target was dropped with its own references",
    _REASON_CYCLE: "reference forms a cycle",
}

_LINE_HEADER = struct.Struct("<qBBI")
_BATCH = 8192


class LinkedBadReferenceError(ValueError):
    """Strict-mode bad reference, annotated with the source position."""

    def __init__(self, path, lineno, raw, cause):
        self.path = path
        self.lineno = lineno
        self.raw = raw
        self.cause = cause
        super().__init__(f"{path}: line {lineno}: bad reference: {cause}")


class MigrationLockedError(RuntimeError):
    """A member lease is held by another live migration instance.

    Leases are non-blocking: while an active instance holds the lease
    of a member this run needs, the run fails fast instead of waiting.
    Once the holder finishes or disappears (its process exits, however
    abruptly), the lease is released by the OS and the next run
    reclaims it, taking over from the durable checkpoints.
    """


class LinkedMigrationResult(NamedTuple):
    groups: tuple
    records_migrated: int
    records_skipped: int
    references_bad: int
    records_salvaged: int
    replaced: bool
    members: tuple
    post_commit_error: str | None = None


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def _normalize_groups(groups):
    """Validate the caller-given list of member lists.

    A non-sequence (including a single path string) is a TypeError; an
    empty group list, an empty group, or a path repeated anywhere
    across the groups is a ValueError.
    """
    if isinstance(groups, (str, bytes, os.PathLike)) or not isinstance(
        groups, Sequence
    ):
        raise TypeError("groups must be a sequence of member sequences")
    out = []
    seen = set()
    for group in groups:
        if isinstance(group, (str, bytes, os.PathLike)) or not isinstance(
            group, Sequence
        ):
            raise TypeError("each group must be a sequence of members")
        members = []
        for item in group:
            path = os.fspath(item)
            if isinstance(path, bytes):
                path = os.fsdecode(path)
            absolute = os.path.abspath(path)
            if absolute in seen:
                raise ValueError(f"duplicate member path: {path!r}")
            seen.add(absolute)
            members.append(path)
        if not members:
            raise ValueError("group member list is empty")
        out.append(members)
    if not out:
        raise ValueError("group list is empty")
    return out


def _normalize_links(links, group_count):
    """Validate link declarations; return a list of 4-tuples."""
    if links is None:
        return []
    if isinstance(links, (str, bytes)) or not isinstance(links, Sequence):
        raise TypeError(
            "links must be a sequence of "
            "(src_group, dst_group, src_field, dst_field)"
        )
    out = []
    seen = set()
    for link in links:
        if isinstance(link, (str, bytes)) or not isinstance(
            link, Sequence
        ) or len(link) != 4:
            raise ValueError(f"malformed link declaration: {link!r}")
        src_g, dst_g, src_field, dst_field = link
        for index in (src_g, dst_g):
            if not isinstance(index, int) or isinstance(index, bool) \
                    or not 0 <= index < group_count:
                raise ValueError(
                    f"link group index out of range: {link!r}"
                )
        for field in (src_field, dst_field):
            if not isinstance(field, str) or not field:
                raise ValueError(
                    f"link field must be a non-empty string: {link!r}"
                )
        key = (src_g, dst_g, src_field, dst_field)
        if key in seen:
            raise ValueError(f"duplicate link declaration: {link!r}")
        seen.add(key)
        out.append(key)
    return out


# ---------------------------------------------------------------------------
# Work-directory layout
# ---------------------------------------------------------------------------


def _linked_group_dir(groups):
    """The group work directory, scoped to the exact member set.

    The directory is keyed by a hash of the absolute member lists so
    that several migration instances with *disjoint* group sets anchored
    in the same directory advance in parallel, while instances over the
    same members deterministically find (and resume) each other's
    durable state.  The bad-record policy and the link set stay in the
    manifest, so a policy/link change reuses -- and validates -- the
    same directory.
    """
    first_dir = os.path.dirname(os.path.abspath(groups[0][0]))
    key = json.dumps(
        [[os.path.abspath(p) for p in g] for g in groups],
        separators=(",", ":"),
    )
    digest = hashlib.sha1(key.encode("utf-8")).hexdigest()[:16]
    return os.path.join(first_dir, f"{_LINKED_DIR_NAME}-{digest}")


def _acquire_member_leases(paths):
    """Take a non-blocking lease on every member (list order).

    The lease is an exclusive ``flock`` on the member's
    ``<path>.migrate.lock`` file -- the same lock file the single-file
    and single-group migrators wait on, so every migration flavour
    mutually excludes on a shared member.  A lease held by a live
    instance raises :class:`MigrationLockedError` immediately; a holder
    that disappeared released its locks via the OS, so the next
    instance reclaims them and takes over from the durable checkpoints.
    Already-taken leases are released before the error propagates.
    """
    fhs = []
    try:
        for path in paths:
            fh = open(path + ".migrate.lock", "a+b")
            try:
                fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                fh.close()
                raise MigrationLockedError(
                    f"member {path!r} is leased by an active migration "
                    f"instance"
                )
            fhs.append(fh)
    except BaseException:
        for fh in fhs:
            try:
                fcntl.flock(fh, fcntl.LOCK_UN)
            finally:
                fh.close()
        raise
    return fhs


@contextlib.contextmanager
def _member_leases(paths):
    """Hold every member's lease for the duration of the run."""
    fhs = _acquire_member_leases(paths)
    try:
        yield fhs
    finally:
        for fh in fhs:
            try:
                fcntl.flock(fh, fcntl.LOCK_UN)
            finally:
                fh.close()


def _linked_member_dir(path):
    return path + _MEMBER_TMP_SUFFIX


def _linked_debris(path):
    marker = path + ".committed"
    return {
        "staged": path + _STAGED_SUFFIX,
        "backup": path + _BACKUP_SUFFIX,
        "marker": marker,
        "marker_tmp": marker + ".tmp",
    }


def _remove(path):
    try:
        os.remove(path)
    except FileNotFoundError:
        pass


def _sweep_linked(paths, group_dir):
    for path in paths:
        shutil.rmtree(_linked_member_dir(path), ignore_errors=True)
    shutil.rmtree(group_dir, ignore_errors=True)


def _write_linked_manifest(group_dir, groups, links, on_bad):
    data = {
        "version": 1,
        "on_bad": on_bad,
        "groups": [[os.path.abspath(p) for p in g] for g in groups],
        "links": [list(link) for link in links],
    }
    tmp = os.path.join(group_dir, _MANIFEST + ".tmp")
    with open(tmp, "wb") as f:
        f.write(json.dumps(data).encode("ascii"))
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, os.path.join(group_dir, _MANIFEST))
    _fsync_dir(group_dir)


def _read_linked_manifest(group_dir):
    try:
        with open(os.path.join(group_dir, _MANIFEST), "rb") as f:
            return json.loads(f.read())
    except (OSError, ValueError):
        return None


def _write_linked_prepared(member_dir, info):
    path = os.path.join(member_dir, _LINKED_PREPARED)
    with open(path + ".tmp", "wb") as f:
        f.write(json.dumps(info).encode("ascii"))
        f.flush()
        os.fsync(f.fileno())
    os.replace(path + ".tmp", path)
    _fsync_dir(member_dir)


def _read_linked_prepared(member_dir):
    try:
        with open(os.path.join(member_dir, _LINKED_PREPARED), "rb") as f:
            data = json.loads(f.read())
        for key in ("dirty", "offset", "lineno"):
            if key not in data:
                return None
        return data
    except (OSError, ValueError):
        return None


# ---------------------------------------------------------------------------
# Durable reference-resolution record and per-member compaction
# ---------------------------------------------------------------------------
#
# Online compaction (compact_linked_logs) shrinks the durable work state
# of an uncommitted run to a deterministic compact form whose
# bookkeeping grows with the member count, not with the total line
# count:
#
#   * the group work directory gains ``linked-resolved`` -- the full
#     resolution outcome (per-member bad lines and dropped records with
#     their source offsets, the globally first bad reference, the bad
#     reference count), written before any member is compacted;
#   * each member's checkpoint log is collapsed to its header plus one
#     record over a single merged segment, and the per-line index
#     (``lines``) is removed -- the resolution record replaces it.
#
# A member carrying a valid ``compacted`` receipt is never rescanned:
# the migration resume path and compaction reruns both take the member
# straight from its receipt plus the collapsed checkpoint.  Everything
# is rebuildable local state: losing the resolution record simply
# forces a fresh prepare.


def _resolution_key(groups, links, on_bad):
    """Identity of the configuration a resolution record belongs to."""
    key = json.dumps({
        "groups": [[os.path.abspath(p) for p in g] for g in groups],
        "links": [list(link) for link in links],
        "on_bad": on_bad,
    }, separators=(",", ":"), sort_keys=True)
    return hashlib.sha1(key.encode("utf-8")).hexdigest()


def _write_resolution(group_dir, record):
    path = os.path.join(group_dir, _LINKED_RESOLVED)
    with open(path + ".tmp", "wb") as f:
        f.write(json.dumps(record).encode("ascii"))
        f.flush()
        os.fsync(f.fileno())
    os.replace(path + ".tmp", path)
    _fsync_dir(group_dir)


def _read_resolution(group_dir):
    try:
        with open(os.path.join(group_dir, _LINKED_RESOLVED), "rb") as f:
            data = json.loads(f.read())
        if not isinstance(data, dict) or data.get("version") != 1:
            return None
        if not isinstance(data.get("members"), list):
            return None
        return data
    except (OSError, ValueError):
        return None


def _write_compacted(member_dir, info):
    path = os.path.join(member_dir, _COMPACTED)
    with open(path + ".tmp", "wb") as f:
        f.write(json.dumps(info).encode("ascii"))
        f.flush()
        os.fsync(f.fileno())
    os.replace(path + ".tmp", path)
    _fsync_dir(member_dir)


def _read_compacted(member_dir):
    try:
        with open(os.path.join(member_dir, _COMPACTED), "rb") as f:
            data = json.loads(f.read())
        for key in ("offset", "lineno", "dirty", "seg_size"):
            if key not in data:
                return None
        return data
    except (OSError, ValueError):
        return None


def _compact_member_state(member_dir):
    """Compact one prepared member's durable work state, idempotently.

    Collapses the checkpoint log to its header plus a single record
    over one merged segment, writes the ``compacted`` receipt, then
    removes the per-line index (the group-level resolution record,
    already durable, replaces it).  Returns True when this call did
    compacting work, False when the member was already compact.

    Every step is ordered so a kill leaves a state the rerun either
    resumes directly or redoes without rescanning the member:

      1. retained segments are truncated to their recorded sizes and
         concatenated (record order) into ``compact.tmp``;
      2. ``compact.tmp`` is atomically renamed over the first retained
         segment (the checkpoint's existing first record stays valid
         throughout: the merged file is never shorter than recorded);
      3. the checkpoint log is atomically replaced by header + one
         record naming the merged segment;
      4. the other segments are deleted;
      5. the ``compacted`` receipt is published;
      6. the line index is deleted (last, so a kill before the receipt
         keeps the index and the member stays resumable the old way).
    """
    receipt = _read_compacted(member_dir)
    if receipt is not None:
        # Already compact: only the index-deletion mop-up may remain.
        try:
            os.remove(os.path.join(member_dir, _LINES_NAME))
            _fsync_dir(member_dir)
        except FileNotFoundError:
            pass
        return False
    prepared = _read_linked_prepared(member_dir)
    if prepared is None:
        return False
    inode, mode, records, _good = _read_checkpoint_log(member_dir)
    offset = int(prepared["offset"])
    lineno = int(prepared["lineno"])
    dirty = bool(prepared["dirty"])
    if records:
        # One merged segment named after the first retained segment.
        merged_name = records[0][4]
        tmp = os.path.join(member_dir, "compact.tmp")
        total = 0
        with open(tmp, "wb") as out:
            for _off, _count, _skipped, _d, name, size in records:
                seg = os.path.join(member_dir, name)
                if os.path.getsize(seg) != size:
                    with open(seg, "r+b") as f:
                        f.truncate(size)
                with open(seg, "rb") as part:
                    remaining = size
                    while remaining:
                        chunk = part.read(min(1 << 20, remaining))
                        if not chunk:
                            break
                        out.write(chunk)
                        remaining -= len(chunk)
                        total += len(chunk)
            out.flush()
            os.fsync(out.fileno())
        os.replace(tmp, os.path.join(member_dir, merged_name))
        last = records[-1]
        cp_tmp = os.path.join(member_dir, "checkpoint.tmp")
        with open(cp_tmp, "wb") as f:
            f.write(f"src {inode} {mode}\n".encode("ascii"))
            f.write(
                f"{last[0]} {last[1]} {last[2]} {last[3]} "
                f"{merged_name} {total}\n".encode("ascii")
            )
            f.flush()
            os.fsync(f.fileno())
        os.replace(cp_tmp, os.path.join(member_dir, "checkpoint"))
        _fsync_dir(member_dir)
        for name in os.listdir(member_dir):
            if name.startswith("seg-") and name != merged_name:
                os.remove(os.path.join(member_dir, name))
        _fsync_dir(member_dir)
        seg_size = total
    else:
        # An empty member has no segments and no checkpoint records.
        seg_size = 0
    _write_compacted(member_dir, {
        "version": 1,
        "offset": offset,
        "lineno": lineno,
        "dirty": dirty,
        "seg_size": seg_size,
    })
    try:
        os.remove(os.path.join(member_dir, _LINES_NAME))
        _fsync_dir(member_dir)
    except FileNotFoundError:
        pass
    return True


def _resume_compacted_member(index, group_index, path, member_dir, on_bad):
    """Resume a compacted member from its receipt; never rescans."""
    receipt = _read_compacted(member_dir)
    if receipt is None:
        return None
    inode, mode, records, _good = _read_checkpoint_log(member_dir)
    try:
        current_inode = os.stat(path).st_ino
    except OSError:
        return None
    if inode != current_inode or mode != on_bad:
        return None
    offset = int(receipt["offset"])
    lineno = int(receipt["lineno"])
    dirty = bool(receipt["dirty"])
    if records:
        off, count, skipped, dirty_flag, _name, size = records[-1]
        if len(records) != 1 or off != offset \
                or count + skipped != lineno or bool(dirty_flag) != dirty \
                or size != int(receipt["seg_size"]):
            return None
    elif offset != 0 or lineno != 0:
        return None
    if dirty and not os.path.isfile(os.path.join(member_dir, _FINAL)):
        return None
    try:
        src = open(path, "rb")
    except OSError:
        return None
    return _LinkedMember(
        index, group_index, path, member_dir, src, dirty, offset,
        lineno, 0, 0, lineno,
    )


# ---------------------------------------------------------------------------
# Rename debris reconciliation (crash recovery)
# ---------------------------------------------------------------------------


def _reconcile_linked_member(path, committed):
    """Resolve one member's interrupted rename state, deterministically."""
    d = _linked_debris(path)
    parent = os.path.dirname(os.path.abspath(path))
    staged = os.path.exists(d["staged"])
    backup = os.path.exists(d["backup"])
    live = os.path.exists(path)
    touched = staged or backup

    if committed:
        if staged and live and not backup:
            os.replace(path, d["backup"])
            os.replace(d["staged"], path)
            _fsync_dir(parent)
        elif staged and backup and not live:
            os.replace(d["staged"], path)
            _fsync_dir(parent)
        _remove(d["marker_tmp"])
        _fsync_dir(parent)
        return

    if staged and backup and not live:
        os.replace(d["backup"], path)
        _fsync_dir(parent)
        _remove(d["staged"])
    elif staged and live and not backup:
        _remove(d["staged"])
    elif backup and live and not staged:
        os.replace(path, d["staged"])
        os.replace(d["backup"], path)
        _fsync_dir(parent)
        _remove(d["staged"])
    elif backup and not live and not staged:
        os.replace(d["backup"], path)
        _fsync_dir(parent)
    if touched:
        _remove(d["marker_tmp"])
        _fsync_dir(parent)


# ---------------------------------------------------------------------------
# Per-member durable line index
# ---------------------------------------------------------------------------


def _project_payload(kind, payload):
    """Project one consumed line to its string fields for the index.

    *kind* is a ``LINE_*`` class; *payload* is the migrated output bytes
    for a good line, the raw source bytes otherwise.  Only string fields
    survive -- the projection exists to resolve references, and a
    non-string field value is never a reference.
    """
    try:
        if kind == LINE_GOOD:
            # The migrated, canonical output bytes.
            obj = json.loads(payload)
        else:
            # A skipped bad line: parse leniently (NaN/Infinity
            # tolerated) so its key fields still identify it as a
            # reference target.
            obj = json.loads(
                payload.decode("utf-8"),
                parse_constant=lambda _c: None,
            )
    except (ValueError, UnicodeDecodeError):
        return {}
    if not isinstance(obj, dict):
        return {}
    return {k: v for k, v in obj.items() if isinstance(v, str)}


class _LineIndex:
    """Durable map of every source line a member's prepare consumed.

    One variable-length record per consumed line: a ``<qBBI`` header
    (source offset, kind, reserved, projection length) followed by the
    projection -- a JSON object of every string field of the *migrated*
    record (or, for a skipped line, of a lenient parse of the raw
    line).  Projecting all string fields -- not just the fields one
    link set touches -- keeps the index reusable by a different
    instance taking the member over with a different link set.  The
    index is fsynced before every checkpoint record, so a trusted
    checkpoint never covers unindexed lines.
    """

    def __init__(self, member_dir):
        self._path = os.path.join(member_dir, _LINES_NAME)
        self._fh = None

    def begin(self, total_lines, resumed):
        """Open the index; on resume truncate it to the checkpoint prefix."""
        if resumed and os.path.exists(self._path):
            keep = self._prefix_bytes(total_lines)
            with open(self._path, "r+b") as f:
                f.truncate(keep)
                f.flush()
                os.fsync(f.fileno())
            self._fh = open(self._path, "ab")
        else:
            # Fresh start -- or the index vanished while checkpoints
            # survived; the caller's post-prepare consistency check
            # detects the mismatch and restarts the member.
            self._fh = open(self._path, "wb")

    def _prefix_bytes(self, total):
        count = 0
        pos = 0
        try:
            f = open(self._path, "rb")
        except OSError:
            return 0
        with f:
            while count < total:
                header = f.read(_LINE_HEADER.size)
                if len(header) < _LINE_HEADER.size:
                    break
                _off, _kind, _reserved, plen = _LINE_HEADER.unpack(header)
                f.seek(plen, os.SEEK_CUR)
                pos = f.tell()
                count += 1
        return pos

    def record(self, offset, kind, payload):
        proj = _project_payload(kind, payload)
        blob = json.dumps(
            proj, ensure_ascii=True, separators=(",", ":"), sort_keys=True
        ).encode("ascii")
        self._fh.write(_LINE_HEADER.pack(offset, kind, 0, len(blob)))
        self._fh.write(blob)

    def sync(self):
        self._fh.flush()
        os.fsync(self._fh.fileno())

    def close(self):
        if self._fh is not None:
            try:
                self._fh.close()
            finally:
                self._fh = None


def _stream_line_entries(member_dir):
    """Yield ``(lineno0, offset, kind, projection)`` in source line order."""
    path = os.path.join(member_dir, _LINES_NAME)
    with open(path, "rb") as f:
        lineno0 = 0
        while True:
            header = f.read(_LINE_HEADER.size)
            if len(header) < _LINE_HEADER.size:
                return
            offset, kind, _reserved, plen = _LINE_HEADER.unpack(header)
            blob = f.read(plen)
            if len(blob) < plen:
                return
            proj = json.loads(blob) if plen else {}
            yield lineno0, offset, kind, proj
            lineno0 += 1


def _count_line_entries(member_dir):
    try:
        return sum(1 for _ in _stream_line_entries(member_dir))
    except OSError:
        return -1


# ---------------------------------------------------------------------------
# Member state
# ---------------------------------------------------------------------------


class _LinkedMember:
    def __init__(self, index, group_index, path, member_dir, src, dirty,
                 offset, lineno, run_migrated, run_skipped, fresh_from):
        self.index = index
        self.group_index = group_index
        self.path = path
        self.member_dir = member_dir
        self.src = src
        self.dirty = dirty
        self.offset = offset
        self.lineno = lineno
        # Records this invocation newly migrates; salvaged is added
        # after the border.  Lines before ``fresh_from`` were migrated
        # by an earlier (killed) run and are not counted again.
        self.run_migrated = run_migrated
        self.run_skipped = run_skipped
        self.fresh_from = fresh_from
        self.salvaged = 0
        self.dropped = set()
        # Post-resolution per-line decisions, held in memory so the
        # filtering/audit/error paths never need the durable line index
        # (which online compaction removes).  Populated either from the
        # freshly built reference index or from the durable resolution
        # record a compaction left behind.
        self.bad_lines = []          # sorted lineno0 of non-good lines
        self.bad_line_offsets = {}   # lineno0 -> source offset
        self.dropped_offsets = {}    # lineno0 -> source offset


def _prepare_linked_member(index, group_index, path, member_dir, on_bad,
                           segment_size, quiesce, audit_stream):
    """Prepare one member with its durable line index.

    In strict mode bad lines are deferred (recorded in the line index,
    never audited, never raised) so the run can compare every member's
    first bad line against the first bad reference and report the
    globally first problem in group/member/line order.
    """
    member = _prepare_member(
        path, member_dir, on_bad, segment_size, quiesce,
        _AuditPrefix(audit_stream, path),
        line_index=_LineIndex(member_dir),
        defer_bad=(on_bad == "strict"),
    )
    if _count_line_entries(member_dir) != member.lineno:
        # The line index and the checkpoints disagree (e.g. the index
        # file was lost between runs): restart the member from scratch
        # so both are rebuilt consistently.
        try:
            member.src.close()
        except OSError:
            pass
        shutil.rmtree(member_dir, ignore_errors=True)
        member = _prepare_member(
            path, member_dir, on_bad, segment_size, quiesce,
            _AuditPrefix(audit_stream, path),
            line_index=_LineIndex(member_dir),
            defer_bad=(on_bad == "strict"),
        )
    _write_linked_prepared(member_dir, {
        "dirty": member.dirty, "offset": member.offset,
        "lineno": member.lineno,
    })
    fresh_from = member.lineno - (member.run_migrated + member.run_skipped)
    return _LinkedMember(
        index, group_index, path, member_dir, member.src, member.dirty,
        member.offset, member.lineno, member.run_migrated,
        member.run_skipped, fresh_from,
    )


def _resume_linked_prepared(index, group_index, path, member_dir, on_bad):
    """Fast path: a member fully prepared before the kill is not redone."""
    info = _read_linked_prepared(member_dir)
    if info is None:
        return None
    inode, mode, _records, _good = _read_checkpoint_log(member_dir)
    try:
        current_inode = os.stat(path).st_ino
    except OSError:
        return None
    if inode != current_inode or mode != on_bad:
        return None
    if _count_line_entries(member_dir) != int(info["lineno"]):
        # The line index no longer matches the checkpointed line count:
        # restart this member from scratch (its checkpoints are
        # discarded with the work directory).
        shutil.rmtree(member_dir, ignore_errors=True)
        return None
    if info["dirty"] and not os.path.isfile(
        os.path.join(member_dir, _FINAL)
    ):
        # The final was consumed by a staging that got rolled back; the
        # member re-assembles from its checkpoints (still no rescan).
        return None
    try:
        src = open(path, "rb")
    except OSError:
        return None
    lineno = int(info["lineno"])
    return _LinkedMember(
        index, group_index, path, member_dir, src, bool(info["dirty"]),
        int(info["offset"]), lineno, 0, 0, lineno,
    )


def _collect_member_decisions(member, dropped):
    """Populate a member's in-memory decisions from its line index.

    Used on the freshly resolved path: the durable line index is
    streamed once and the offsets of exactly the lines the later
    filtering/audit/error paths need (bad lines, dropped records) are
    kept; nothing per-line is retained for good, kept lines.
    """
    wanted = dropped.get(member.index, set())
    for lineno0, offset, kind, _proj in _stream_line_entries(
        member.member_dir
    ):
        if kind != LINE_GOOD:
            member.bad_lines.append(lineno0)
            member.bad_line_offsets[lineno0] = offset
        elif lineno0 in wanted:
            member.dropped_offsets[lineno0] = offset


def _apply_resolution_record(members, resolution):
    """Populate in-memory decisions from a durable resolution record."""
    for member in members:
        entry = resolution["members"][member.index]
        member.bad_lines = sorted(
            int(lineno0) for lineno0, _offset in entry["bad_lines"]
        )
        member.bad_line_offsets = {
            int(lineno0): int(offset)
            for lineno0, offset in entry["bad_lines"]
        }
        member.dropped_offsets = {
            int(lineno0): int(offset)
            for lineno0, offset in entry["dropped"]
        }


# ---------------------------------------------------------------------------
# Reference index (SQLite spill file: streaming, bounded memory)
# ---------------------------------------------------------------------------


class _RefsDb:
    """Disk-backed reference graph over all prepared members.

    Tables:
      nodes(node_id, mi, lineno0)      every good record
      node_key(link_id, value, node)   target keys of good records
      skipped_key(link_id, value)      keys of skipped bad lines
      badv_key(link_id, value)         keys of illegal-version lines
      edges(edge_id, src, link, value, dst, reason, fresh)
    """

    def __init__(self, path):
        self._db = sqlite3.connect(path)
        # The database is rebuilt from the durable line indexes on every
        # uncommitted rerun, so crash-safety of the file itself is
        # unnecessary; keep it fast and strictly bounded in memory.
        self._db.execute("PRAGMA journal_mode=OFF")
        self._db.execute("PRAGMA synchronous=OFF")
        self._db.executescript(
            """
            CREATE TABLE nodes(
                node_id INTEGER PRIMARY KEY, mi INT, lineno INT);
            CREATE TABLE node_key(link_id INT, value TEXT, node INT);
            CREATE INDEX node_key_idx ON node_key(link_id, value);
            CREATE TABLE skipped_key(link_id INT, value TEXT);
            CREATE INDEX skipped_idx ON skipped_key(link_id, value);
            CREATE TABLE badv_key(link_id INT, value TEXT);
            CREATE INDEX badv_idx ON badv_key(link_id, value);
            CREATE TABLE edges(
                edge_id INTEGER PRIMARY KEY, src INT, link_id INT,
                value TEXT, dst INT, reason INT, fresh INT);
            CREATE INDEX edges_src ON edges(src);
            """
        )

    def close(self):
        self._db.close()

    def build_member(self, member, dst_links, src_links):
        """Index one member from its durable line index (streaming)."""
        entries = (
            (lineno0, kind, proj)
            for lineno0, _offset, kind, proj
            in _stream_line_entries(member.member_dir)
        )
        self._build_entries(member.index, entries, dst_links, src_links,
                            member.fresh_from, projected=True)

    def build_scanned_member(self, index, entries, dst_links, src_links,
                             fresh_from=0):
        """Index one member from in-memory ``(lineno0, kind, payload)``.

        Used by the read-only rehearsal: *payload* is the migrated
        output bytes for a good line, the raw source bytes otherwise --
        exactly the payloads the durable line index records.
        """
        self._build_entries(index, entries, dst_links, src_links,
                            fresh_from, projected=False)

    def _build_entries(self, member_index, entries, dst_links, src_links,
                       fresh_from, projected):
        db = self._db
        cur = db.cursor()
        keys, skips, badvs, edge_rows = [], [], [], []

        def flush():
            if keys:
                cur.executemany(
                    "INSERT INTO node_key(link_id, value, node) "
                    "VALUES (?,?,?)", keys)
                keys.clear()
            if skips:
                cur.executemany(
                    "INSERT INTO skipped_key(link_id, value) "
                    "VALUES (?,?)", skips)
                skips.clear()
            if badvs:
                cur.executemany(
                    "INSERT INTO badv_key(link_id, value) VALUES (?,?)",
                    badvs)
                badvs.clear()
            if edge_rows:
                cur.executemany(
                    "INSERT INTO edges(src, link_id, value, fresh) "
                    "VALUES (?,?,?,?)", edge_rows)
                edge_rows.clear()

        for lineno0, kind, payload in entries:
            proj = payload if projected else _project_payload(kind, payload)
            if kind == LINE_GOOD:
                cur.execute(
                    "INSERT INTO nodes(mi, lineno) VALUES (?,?)",
                    (member_index, lineno0),
                )
                node_id = cur.lastrowid
                for link_id, field in dst_links:
                    value = proj.get(field)
                    if value is not None:
                        keys.append((link_id, value, node_id))
                for link_id, field in src_links:
                    value = proj.get(field)
                    if value is not None:
                        edge_rows.append((
                            node_id, link_id, value,
                            1 if lineno0 >= fresh_from else 0,
                        ))
            else:
                rows = badvs if kind == LINE_BAD_VERSION else skips
                for link_id, field in dst_links:
                    value = proj.get(field)
                    if value is not None:
                        rows.append((link_id, value))
            if len(keys) + len(skips) + len(badvs) + len(edge_rows) \
                    >= _BATCH:
                flush()
        flush()
        db.commit()

    def _cascade(self):
        """Propagate badness: a reference to a bad record is bad itself."""
        while True:
            cur = self._db.execute(
                "UPDATE edges SET reason=? WHERE reason IS NULL "
                "AND dst IN (SELECT DISTINCT src FROM edges "
                "WHERE reason IS NOT NULL)",
                (_REASON_CASCADE,),
            )
            self._db.commit()
            if cur.rowcount == 0:
                return

    def _mark_cycles(self):
        """Reject every outgoing reference of records on a cycle."""
        db = self._db
        db.execute("DROP TABLE IF EXISTS temp.visited")
        db.execute("DROP TABLE IF EXISTS temp.incycle")
        db.execute("CREATE TEMP TABLE visited(node INTEGER PRIMARY KEY)")
        db.execute("CREATE TEMP TABLE incycle(node INTEGER PRIMARY KEY)")
        page = 4096
        while True:
            roots = [row[0] for row in db.execute(
                "SELECT DISTINCT src FROM edges "
                "WHERE reason IS NULL AND dst IS NOT NULL "
                "AND src NOT IN (SELECT node FROM visited) "
                "ORDER BY src LIMIT ?", (page,))]
            if not roots:
                break
            for root in roots:
                self._dfs(root)
        db.execute(
            "UPDATE edges SET reason=? WHERE reason IS NULL "
            "AND src IN (SELECT node FROM incycle)",
            (_REASON_CYCLE,),
        )
        db.commit()

    def _dfs(self, root):
        """Iterative DFS over clean edges; records back-edge cycles."""
        db = self._db
        if db.execute(
            "SELECT 1 FROM visited WHERE node=?", (root,)
        ).fetchone():
            return
        path = [root]
        in_path = {root: 0}
        offsets = {root: 0}
        done = []
        while path:
            node = path[-1]
            row = db.execute(
                "SELECT dst FROM edges WHERE src=? AND reason IS NULL "
                "AND dst IS NOT NULL ORDER BY dst LIMIT 1 OFFSET ?",
                (node, offsets[node]),
            ).fetchone()
            offsets[node] += 1
            if row is None:
                path.pop()
                in_path.pop(node)
                done.append(node)
                continue
            target = row[0]
            if target in in_path:
                db.executemany(
                    "INSERT OR IGNORE INTO incycle(node) VALUES (?)",
                    [(n,) for n in path[in_path[target]:]],
                )
            elif target not in in_path and not db.execute(
                "SELECT 1 FROM visited WHERE node=?", (target,)
            ).fetchone():
                in_path[target] = len(path)
                offsets[target] = 0
                path.append(target)
        db.executemany(
            "INSERT OR IGNORE INTO visited(node) VALUES (?)",
            [(n,) for n in done],
        )
        db.commit()

    def validate(self):
        """Resolve every edge and classify all bad references."""
        db = self._db
        db.execute(
            "UPDATE edges SET dst=(SELECT MIN(node) FROM node_key "
            "WHERE node_key.link_id=edges.link_id "
            "AND node_key.value=edges.value)"
        )
        db.execute(
            "UPDATE edges SET reason=? WHERE dst IS NULL AND EXISTS "
            "(SELECT 1 FROM badv_key k WHERE k.link_id=edges.link_id "
            "AND k.value=edges.value)",
            (_REASON_BAD_VERSION,),
        )
        db.execute(
            "UPDATE edges SET reason=? WHERE dst IS NULL "
            "AND reason IS NULL AND EXISTS "
            "(SELECT 1 FROM skipped_key k WHERE k.link_id=edges.link_id "
            "AND k.value=edges.value)",
            (_REASON_TARGET_SKIPPED,),
        )
        db.execute(
            "UPDATE edges SET reason=? WHERE dst IS NULL "
            "AND reason IS NULL",
            (_REASON_MISSING,),
        )
        db.commit()
        self._cascade()
        self._mark_cycles()
        self._cascade()

    def fresh_bad_edges(self):
        """Bad references among references this invocation newly parsed."""
        return self._db.execute(
            "SELECT COUNT(*) FROM edges WHERE reason IS NOT NULL "
            "AND fresh=1"
        ).fetchone()[0]

    def dropped_lines(self):
        """``{member_index: {lineno0, ...}}`` of records to drop."""
        dropped = {}
        for mi, lineno in self._db.execute(
            "SELECT DISTINCT nodes.mi, nodes.lineno FROM nodes "
            "JOIN edges ON edges.src=nodes.node_id "
            "WHERE edges.reason IS NOT NULL ORDER BY nodes.mi, nodes.lineno"
        ):
            dropped.setdefault(mi, set()).add(lineno)
        return dropped

    def first_bad(self):
        """The globally first bad reference, or None."""
        return self._db.execute(
            "SELECT nodes.mi, nodes.lineno, edges.reason, edges.link_id,"
            " edges.value FROM edges JOIN nodes ON nodes.node_id=edges.src"
            " WHERE edges.reason IS NOT NULL"
            " ORDER BY nodes.mi, nodes.lineno, edges.reason LIMIT 1"
        ).fetchone()


# ---------------------------------------------------------------------------
# Filtering: build the linked-final output without bad-reference records
# ---------------------------------------------------------------------------


def _write_linked_final(member):
    """Write the member's output minus its bad-reference records.

    Dirty members are filtered from their assembled ``final`` (which
    holds exactly the good lines, in source order); clean members
    (canonical bytes, never re-encoded, hence no bad lines) are
    filtered straight from the prepared source prefix.  Both are
    byte-level operations driven by the in-memory per-line decisions --
    nothing is rescanned or re-decoded, and the durable line index is
    not needed (online compaction removes it).
    """
    out_path = os.path.join(member.member_dir, _LINKED_FINAL)
    if member.dirty:
        final = os.path.join(member.member_dir, _FINAL)
        # The final's nth line is the nth *good* source line; bad
        # source lines never reached it, so its source line number is
        # advanced past the member's bad lines before each mapping.
        bad = sorted(member.bad_lines)
        bad_count = len(bad)
        with open(final, "rb") as fin, open(out_path, "wb") as out:
            src_no = 0
            bad_i = 0
            for line in fin:
                while bad_i < bad_count and bad[bad_i] == src_no:
                    src_no += 1
                    bad_i += 1
                if src_no not in member.dropped:
                    out.write(line)
                src_no += 1
            out.flush()
            os.fsync(out.fileno())
    else:
        with open(member.path, "rb") as src, open(out_path, "wb") as out:
            for lineno0 in range(member.lineno):
                line = src.readline()
                if not line:
                    break
                if lineno0 not in member.dropped:
                    out.write(line)
            out.flush()
            os.fsync(out.fileno())
    _fsync_dir(member.member_dir)


def _audit_dropped(member, audit_stream):
    """One ``<file>:<lineno>:<first 32 bytes>`` entry per dropped record."""
    prefix = _AuditPrefix(audit_stream, member.path)
    with open(member.path, "rb") as src:
        for lineno0 in sorted(member.dropped):
            src.seek(member.dropped_offsets[lineno0])
            prefix.write(_audit_line(lineno0 + 1, src.readline()))
    audit_stream.flush()


# ---------------------------------------------------------------------------
# Two-phase rename
# ---------------------------------------------------------------------------


def _stage_linked_member(member):
    """Phase 1: promote this member's output over its path; backup held."""
    path = member.path
    parent = os.path.dirname(os.path.abspath(path))
    d = _linked_debris(path)
    src_name = _LINKED_FINAL if member.dropped else _FINAL
    os.replace(os.path.join(member.member_dir, src_name), d["staged"])
    _publish_member_marker(path)
    os.replace(path, d["backup"])
    os.replace(d["staged"], path)
    _fsync_dir(parent)


def _undo_staged_members(members):
    failed = False
    for member in members:
        try:
            _reconcile_linked_member(member.path, committed=False)
        except OSError:
            failed = True
    return failed


# ---------------------------------------------------------------------------
# Post-border finishing
# ---------------------------------------------------------------------------


def _copy_complete_tail(src_fd, pos, out):
    """Copy ``src_fd[pos:]`` through its last complete line into *out*.

    Returns the source offset of that last newline; a torn tail still
    being appended is left for the caller's convergence pass to drain
    from the source inode once the record completes.
    """
    src_fd.seek(pos)
    carry = b""
    boundary = pos
    while True:
        chunk = src_fd.read(1 << 20)
        if not chunk:
            break
        data = carry + chunk
        cut = data.rfind(b"\n")
        if cut == -1:
            carry = data
            continue
        out.write(data[:cut + 1])
        boundary += cut + 1
        carry = data[cut + 1:]
    return boundary


def _insert_into_live(path, pos, data, member_dir, parent):
    """Atomically splice *data* into the live file at byte *pos*.

    Returns ``(fd, offset)``: the pre-replace live inode and the offset
    up to which its complete lines were copied.  Bytes a racing writer
    lands on that inode afterwards are drained by the caller's
    convergence pass, so nothing appended during the replace is lost.
    """
    os.makedirs(member_dir, exist_ok=True)
    tmp = os.path.join(member_dir, "salvage-final")
    old = open(path, "rb")
    try:
        with open(tmp, "wb") as out:
            old.seek(0)
            remaining = pos
            while remaining:
                chunk = old.read(min(1 << 20, remaining))
                if not chunk:
                    break
                out.write(chunk)
                remaining -= len(chunk)
            out.write(data)
            boundary = _copy_complete_tail(old, pos, out)
            out.flush()
            os.fsync(out.fileno())
        os.replace(tmp, path)
        _fsync_dir(parent)
    except BaseException:
        old.close()
        raise
    return old, boundary


def _salvage_backup_tail(path, backup_path, offset, lineno, on_bad,
                         audit_stream, member_dir, quiesce):
    """Converge records that exist only on the backup inode into the live
    file, before the backup is removed.

    A crash between the group commit marker and the end of convergence
    leaves the pre-rename inode behind as the backup; records appended
    to it after the prepare drain (or after the crashed run's last
    convergence round) exist nowhere else.  Migrated backup-tail
    records already forming a prefix of the live tail (a partially
    completed convergence) are recognised and not duplicated; the rest
    is spliced in ahead of any younger post-commit content, one atomic
    replace per round, and the backup is re-drained until it stays
    empty for a whole round, so an appender still holding the backup
    inode finishes its in-flight records first.

    Returns ``(salvaged, skipped, gens, complete)``: only records this
    invocation actually moves are counted; *gens* is a list of
    ``(fd, offset)`` older live inodes (replaced by the splices) whose
    late appends the caller's convergence must still drain; *complete*
    is false when a non-stop appender hit the backstop -- the caller
    keeps the backup and reports a warning instead of dropping it.
    """
    parent = os.path.dirname(os.path.abspath(path))
    follow = max(1.0, quiesce * 20)
    deadline = time.monotonic() + max(5.0, quiesce * 200)
    src = open(backup_path, "rb")
    cur_off = offset
    cur_lineno = lineno
    live_pos = None
    salvaged = skipped = 0
    gens = []
    complete = True
    try:
        while True:
            # Drain the backup's complete lines from cur_off, waiting
            # out one quiesce window at EOF for in-flight records.
            tail = []
            for raw in _read_lines(src, quiesce, offset=cur_off,
                                   follow=follow):
                tail.append(raw)
                cur_off += len(raw)
            if not tail:
                break
            if time.monotonic() >= deadline:
                complete = False
                break
            if live_pos is None:
                # Byte offset just past the migrated prefix: the live
                # file's first `lineno` lines are the migrated image of
                # the backup's first `offset` bytes.
                pos = 0
                with open(path, "rb") as live:
                    for _ in range(lineno):
                        line = live.readline()
                        if not line:
                            break
                        pos += len(line)
                live_pos = pos
            insert = bytearray()
            idx = 0
            with open(path, "rb") as live:
                live.seek(live_pos)
                while idx < len(tail):
                    raw = tail[idx]
                    cur_lineno += 1
                    try:
                        out = _convert(raw)
                    except ValueError as exc:
                        if on_bad == "strict":
                            raise BadRecordError(
                                cur_lineno, raw, exc) from exc
                        audit_stream.write(_audit_line(cur_lineno, raw))
                        audit_stream.flush()
                        skipped += 1
                        idx += 1
                        continue
                    here = live.readline()
                    if here == out:
                        # Already converged by the crashed run.
                        live_pos += len(here)
                        idx += 1
                        continue
                    break
            for raw in tail[idx:]:
                cur_lineno += 1
                out, bad = _emit(audit_stream, on_bad, raw, cur_lineno)
                if bad:
                    skipped += 1
                    continue
                insert.extend(out)
                salvaged += 1
            if insert:
                gens.append(_insert_into_live(
                    path, live_pos, insert, member_dir, parent))
                live_pos += len(insert)
    finally:
        src.close()
    return salvaged, skipped, gens, complete


def _converge_and_finalize(staged_members, paths, group_dir, on_bad,
                           quiesce, audit_stream):
    """Converge racing appenders, delete backups, sweep work dirs."""
    warnings = []
    salvaged_total = skipped_total = 0
    for member in staged_members:
        try:
            salvaged, conv_skipped, lineno, warning = _converge(
                member.path,
                os.path.dirname(os.path.abspath(member.path)),
                member.member_dir, member.src, member.offset,
                member.lineno, quiesce,
                _AuditPrefix(audit_stream, member.path), on_bad,
            )
        except (OSError, ValueError) as exc:
            # Border already crossed: convergence faults -- even a bad
            # tail record in strict mode -- are warnings; rerun with
            # --skip to migrate past the record.
            warnings.append(f"{member.path}: post-commit convergence "
                            f"incomplete, rerun to finish: {exc}")
            continue
        member.salvaged = salvaged
        member.run_migrated += salvaged
        member.run_skipped += conv_skipped
        member.lineno = lineno
        salvaged_total += salvaged
        skipped_total += conv_skipped
        if warning:
            warnings.append(f"{member.path}: {warning}")
        try:
            _remove(member.path + _BACKUP_SUFFIX)
            _fsync_dir(os.path.dirname(os.path.abspath(member.path)))
        except OSError as exc:
            warnings.append(f"{member.path}: backup removal failed: {exc}")

    _crash_point("linked-backups")
    try:
        _crash_point("linked-cleanup")
        if os.environ.get(_FAULT_ENV) == "linked-cleanup":
            raise OSError("injected linked cleanup fault")
        _sweep_linked(paths, group_dir)
    except OSError as exc:
        warnings.append(f"cleanup failed: {exc}")
    return salvaged_total, skipped_total, warnings


def _finish_committed_linked(paths, group_dir, on_bad,
                             segment_size, quiesce, audit_stream):
    """Finish a run whose commit marker already exists (a rerun).

    The border was crossed, so every fault here is a warning.  Each
    member is finished exactly like an idempotent single-group rerun --
    except that a surviving backup is *converged before it is removed*:
    records a crashed run left only on the pre-rename inode (appends
    that landed after its prepare drain or its last convergence round)
    are spliced into the live file first, so no appended record is
    lost.  Then the current inode is rescanned (re-encoding a canonical
    file changes no bytes), any old-format tail a path-reopening
    appender landed after wrap-up is migrated through a fresh atomic
    replacement, and racing appenders are converged.  Counters count
    only records this invocation newly moves.
    """
    warnings = []
    member_results = []
    salvaged_total = skipped_total = migrated_total = 0

    for path in paths:
        parent = os.path.dirname(os.path.abspath(path))
        d = _linked_debris(path)
        member_dir = _linked_member_dir(path)
        member_migrated = member_skipped = 0
        member_replaced = False
        extra_gens = []
        try:
            if os.path.exists(d["backup"]):
                # The backup is the pre-rename inode.  The durable
                # prepared marker names where its drain stopped, so the
                # not-yet-converged tail can be told apart from the
                # migrated prefix and salvaged before the backup goes.
                info = _read_linked_prepared(member_dir)
                if info is None:
                    warnings.append(
                        f"{path}: prepared state lost; cannot converge "
                        f"the backup, dropping it"
                    )
                    _remove(d["backup"])
                    _fsync_dir(parent)
                else:
                    salv, skip, gens, complete = _salvage_backup_tail(
                        path, d["backup"], int(info["offset"]),
                        int(info["lineno"]), on_bad,
                        _AuditPrefix(audit_stream, path), member_dir,
                        quiesce,
                    )
                    member_migrated += salv
                    member_skipped += skip
                    extra_gens.extend(gens)
                    if complete:
                        _remove(d["backup"])
                        _fsync_dir(parent)
                    else:
                        # A non-stop appender hit the backstop: keep the
                        # backup AND the prepared state that describes
                        # it, and leave this member to the next rerun
                        # instead of finishing it now.
                        warnings.append(
                            f"{path}: backup still receiving appends at "
                            f"the backstop; rerun to finish"
                        )
                        for fd, _off in extra_gens:
                            try:
                                fd.close()
                            except OSError:
                                pass
                        migrated_total += member_migrated
                        skipped_total += member_skipped
                        salvaged_total += member_migrated
                        member_results.append(MigrationResult(
                            path=path, records_migrated=member_migrated,
                            records_skipped=member_skipped,
                            records_salvaged=member_migrated,
                            replaced=False,
                        ))
                        continue

            member_dir = _linked_member_dir(path)
            shutil.rmtree(member_dir, ignore_errors=True)
            member = _prepare_member(
                path, member_dir, on_bad, segment_size, quiesce,
                _AuditPrefix(audit_stream, path),
            )
            try:
                if member.dirty:
                    staged_path = os.path.join(member_dir, _FINAL)
                    os.replace(staged_path, path + _STAGED_SUFFIX)
                    _publish_member_marker(path)
                    os.replace(path, path + _BACKUP_SUFFIX)
                    os.replace(path + _STAGED_SUFFIX, path)
                    _fsync_dir(parent)
                    member_replaced = True
                    extra_gens.append((member.src, member.offset))
                if extra_gens:
                    # _converge takes the youngest generation as its
                    # source; older generations go first.
                    src_fd, src_off = extra_gens[-1]
                    salvaged, conv_skipped, _lineno, warning = _converge(
                        path, parent, member_dir, src_fd, src_off,
                        member.lineno, quiesce,
                        _AuditPrefix(audit_stream, path), on_bad,
                        extra_gens=extra_gens[:-1],
                    )
                    member_migrated += salvaged
                    member_skipped += conv_skipped
                    if warning:
                        warnings.append(f"{path}: {warning}")
                    _remove(path + _BACKUP_SUFFIX)
                    _fsync_dir(parent)
            finally:
                try:
                    member.src.close()
                except OSError:
                    pass
                for fd, _off in extra_gens:
                    try:
                        fd.close()
                    except OSError:
                        pass
            migrated_total += member_migrated
            skipped_total += member_skipped
            salvaged_total += member_migrated
            member_results.append(MigrationResult(
                path=path, records_migrated=member_migrated,
                records_skipped=member_skipped,
                records_salvaged=member_migrated,
                replaced=member_replaced,
            ))
        except (OSError, ValueError) as exc:
            warnings.append(
                f"{path}: post-commit finishing incomplete, rerun to "
                f"finish: {exc}"
            )
            try:
                shutil.rmtree(member_dir, ignore_errors=True)
            except OSError:
                pass

    try:
        _sweep_linked(paths, group_dir)
    except OSError as exc:
        warnings.append(f"cleanup failed: {exc}")
    return migrated_total, skipped_total, salvaged_total, \
        tuple(member_results), warnings


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


def _linked_group_state(groups, links, on_bad, paths, group_dir):
    """Validate/refresh the manifest and reconcile interrupted renames.

    Shared by the migration and the online compaction.  Returns whether
    the group commit marker exists.  Raises ValueError when the durable
    commit marker belongs to a different configuration.
    """
    manifest = _read_linked_manifest(group_dir)
    committed = os.path.exists(os.path.join(group_dir, _LINKED_COMMITTED))
    abs_groups = [[os.path.abspath(p) for p in g] for g in groups]
    if manifest is not None:
        same_group = (
            manifest.get("groups") == abs_groups
            and manifest.get("links") == [list(l) for l in links]
            and manifest.get("on_bad") == on_bad
        )
        if not same_group:
            if committed:
                raise ValueError(
                    "linked commit marker exists for a different "
                    "group list, link set or policy; refusing to "
                    "mix runs"
                )
            # An earlier uncommitted attempt for a different
            # configuration used the same anchor directory:
            # reverse its renames, then sweep its work state.
            old_paths = [
                p for p in (p for g in manifest.get("groups", [])
                            for p in g)
                if os.path.exists(p)
                or os.path.exists(p + _BACKUP_SUFFIX)
                or os.path.exists(p + _STAGED_SUFFIX)
            ]
            for old_path in old_paths:
                _reconcile_linked_member(old_path, committed=False)
            _sweep_linked(old_paths, group_dir)
            manifest = None
    os.makedirs(group_dir, exist_ok=True)
    if manifest is None:
        _write_linked_manifest(group_dir, groups, links, on_bad)
    _remove(os.path.join(group_dir, _LINKED_COMMITTED_TMP))

    # Resolve interrupted renames before touching content.
    for path in paths:
        _reconcile_linked_member(path, committed)
    return committed


def migrate_linked_logs(groups, *, links=(), on_bad="strict",
                        segment_size=DEFAULT_SEGMENT_SIZE,
                        quiesce=DEFAULT_QUIESCE, audit=None):
    """Migrate linked groups of JSONL logs with referential integrity.

    Every group is migrated to the current record version and every
    declared reference is enforced, as one all-or-nothing unit: either
    all groups finish at the current version with no dangling, skipped,
    cyclic or illegal-version reference targets, or all groups stay
    exactly as they were.  Members may be appended to by different
    processes; a killed run resumes prepared members from durable
    checkpoints and line indexes without rescanning them and finishes
    byte-identical to one uninterrupted run.

    ``groups`` is a non-empty sequence of non-empty member path
    sequences (a path may not repeat across the whole list); ``links``
    is a sequence of ``(src_group, dst_group, src_field, dst_field)``
    declarations.  Returns a :class:`LinkedMigrationResult` whose
    counters count only records this invocation newly migrates and
    references it newly resolves; member-level and group-level counts
    always agree, and a resumed or idempotent run reports zero.

    Several instances may run concurrently: every member is held under
    a non-blocking lease (its ``<path>.migrate.lock`` file), so
    overlapping members are mutually exclusive while disjoint group
    sets advance in parallel.  A member whose lease is held by a live
    instance raises :class:`MigrationLockedError`; once the holder
    finishes or disappears the lease is reclaimed and the next run
    takes over from the durable checkpoints, byte-identical to running
    the same instances serially.

    Raises :class:`TypeError` for a non-sequence group list,
    :class:`ValueError` for an empty or duplicated list (and
    :class:`LinkedBadReferenceError` / :class:`GroupBadRecordError` on
    the globally first bad reference or bad line in strict mode --
    compared in group, member and line order), and
    :class:`FileNotFoundError` when a member is missing or unreadable.
    """
    if on_bad not in ("strict", "skip"):
        raise ValueError(f"on_bad must be 'strict' or 'skip', got {on_bad!r}")
    if segment_size <= 0:
        raise ValueError("segment_size must be positive")
    groups = _normalize_groups(groups)
    links = _normalize_links(links, len(groups))
    paths = [path for group in groups for path in group]
    _probe_members(paths)

    audit_stream = audit if audit is not None else sys.stderr.buffer
    group_dir = _linked_group_dir(groups)

    # Per-group source/target link views.
    dst_links = {}   # group index -> [(link_id, dst_field)]
    src_links = {}   # group index -> [(link_id, src_field)]
    for link_id, (src_g, dst_g, src_field, dst_field) in enumerate(links):
        dst_links.setdefault(dst_g, []).append((link_id, dst_field))
        src_links.setdefault(src_g, []).append((link_id, src_field))

    members = []
    staged_members = []
    replaced = False
    references_bad = 0
    post_commit_error = None

    with _member_leases(paths):
        try:
            _crash_point("linked-lock")

            committed = _linked_group_state(
                groups, links, on_bad, paths, group_dir
            )

            if committed:
                mig, conv_skipped, salvaged, member_res, warnings = (
                    _finish_committed_linked(
                        paths, group_dir, on_bad, segment_size,
                        quiesce, audit_stream,
                    )
                )
                return LinkedMigrationResult(
                    groups=tuple(tuple(g) for g in groups),
                    records_migrated=mig,
                    records_skipped=conv_skipped,
                    references_bad=0,
                    records_salvaged=salvaged,
                    replaced=any(m.replaced for m in member_res),
                    members=member_res,
                    post_commit_error="; ".join(warnings) or None,
                )

            # --- PREPARE every member (resumed via local checkpoints).
            try:
                resolution = _read_resolution(group_dir)
                if resolution is not None and (
                    resolution.get("key")
                    != _resolution_key(groups, links, on_bad)
                    or len(resolution["members"]) != len(paths)
                ):
                    resolution = None
                index = 0
                for group_index, group in enumerate(groups):
                    for path in group:
                        member_dir = _linked_member_dir(path)
                        member = None
                        if resolution is not None:
                            # Online compaction left a durable
                            # resolution: compacted members resume from
                            # their receipt without any line index.
                            member = _resume_compacted_member(
                                index, group_index, path, member_dir,
                                on_bad,
                            )
                        elif _read_compacted(member_dir) is not None:
                            # Compacted but the resolution record is
                            # gone: the line index is unrecoverable, so
                            # the member is prepared from scratch.
                            shutil.rmtree(member_dir, ignore_errors=True)
                        if member is None:
                            member = _resume_linked_prepared(
                                index, group_index, path, member_dir,
                                on_bad,
                            )
                        if member is None:
                            member = _prepare_linked_member(
                                index, group_index, path, member_dir,
                                on_bad, segment_size, quiesce,
                                audit_stream,
                            )
                        members.append(member)
                        index += 1
                _crash_point("linked-prepare")

                if resolution is not None:
                    # The resolution outcome is durable; nothing is
                    # resolved anew, so this run reports zero fresh bad
                    # references (exactly like a checkpoint resume).
                    references_bad = 0
                    dropped = {
                        mi: {
                            int(lineno0)
                            for lineno0, _offset
                            in resolution["members"][mi]["dropped"]
                        }
                        for mi in range(len(resolution["members"]))
                        if resolution["members"][mi]["dropped"]
                    }
                    first = resolution["first_bad_ref"]
                    if first is not None:
                        first = tuple(first)
                    _apply_resolution_record(members, resolution)
                else:
                    # --- RESOLVE references from the durable line
                    # indexes.  The spill database is rebuilt on every
                    # uncommitted run; building it reads only the
                    # per-member line indexes, never the sources.
                    refs_path = os.path.join(group_dir, _REFS_DB)
                    _remove(refs_path)
                    refs = _RefsDb(refs_path)
                    try:
                        for member in members:
                            refs.build_member(
                                member,
                                dst_links.get(member.group_index, []),
                                src_links.get(member.group_index, []),
                            )
                        refs.validate()
                        _crash_point("linked-refs")
                        references_bad = refs.fresh_bad_edges()
                        dropped = refs.dropped_lines()
                        first = refs.first_bad()
                    finally:
                        refs.close()
                    for member in members:
                        _collect_member_decisions(member, dropped)

                if on_bad == "strict":
                    # The globally first problem -- bad line or bad
                    # reference, in group/member/line order -- decides
                    # which exception is raised.  Bad lines were
                    # deferred during prepare, so every member's first
                    # bad line is on record; a bad reference that sorts
                    # before a later member's bad line is reported
                    # first.
                    bad_line = None
                    for member in members:
                        if member.bad_lines:
                            bad_line = (member, member.bad_lines[0])
                            break
                    if bad_line is not None and (
                        first is None
                        or (bad_line[0].index, bad_line[1])
                        < (first[0], first[1])
                    ):
                        member, lineno0 = bad_line
                        with open(member.path, "rb") as f:
                            f.seek(member.bad_line_offsets[lineno0])
                            raw = f.readline()
                        try:
                            _convert(raw)
                            cause = ValueError("undecodable record")
                        except ValueError as exc:
                            cause = exc
                        raise GroupBadRecordError(
                            member.path, lineno0 + 1, raw, cause
                        )
                    if first is not None:
                        mi, lineno0, reason, link_id, value = first
                        member = members[mi]
                        src_g, dst_g, src_field, dst_field = links[link_id]
                        raw = _read_source_line(member, lineno0)
                        raise LinkedBadReferenceError(
                            member.path, lineno0 + 1, raw,
                            f"{_REASON_TEXT[reason]}: "
                            f"{dst_field}={value!r} in group {dst_g}",
                        )

                # --- FILTER bad-reference records out of the output.
                for member in members:
                    member.dropped = dropped.get(member.index, set())
                    if member.dropped:
                        if on_bad == "skip":
                            _audit_dropped(member, audit_stream)
                        _write_linked_final(member)
                        fresh_drops = sum(
                            1 for n in member.dropped
                            if n >= member.fresh_from
                        )
                        member.run_migrated -= fresh_drops
                _crash_point("linked-filter")

                staged_members = [
                    m for m in members if m.dirty or m.dropped
                ]
                if not staged_members:
                    # Everything already canonical and every reference
                    # intact: nothing renamed, all work swept (mirrors
                    # single-group idempotency).
                    _sweep_linked(paths, group_dir)
                    replaced = False
                else:
                    # --- PHASE 1: stage every member.
                    staged = []
                    try:
                        for member in staged_members:
                            _stage_linked_member(member)
                            staged.append(member)
                            _crash_point("linked-stage")
                        _crash_point("linked-staged")
                    except BaseException:
                        if not _undo_staged_members(staged):
                            _sweep_linked(paths, group_dir)
                        raise

                    # --- COMMIT POINT: publish the group marker.
                    marker = os.path.join(group_dir, _LINKED_COMMITTED)
                    marker_tmp = os.path.join(
                        group_dir, _LINKED_COMMITTED_TMP
                    )
                    with open(marker_tmp, "wb") as f:
                        f.write(b"1\n")
                        f.flush()
                        os.fsync(f.fileno())
                    os.replace(marker_tmp, marker)
                    _fsync_dir(group_dir)
                    _crash_point("linked-marker")
                    replaced = True

                    # --- PHASE 2: converge, drop backups, sweep.
                    _salvaged, _conv_skipped, warnings = (
                        _converge_and_finalize(
                            staged_members, paths, group_dir, on_bad,
                            quiesce, audit_stream,
                        )
                    )
                    post_commit_error = "; ".join(warnings) or None
            except BaseException:
                # A handled pre-commit failure leaves no partial
                # intermediate in durable state, and no original is
                # renamed past this point (rollback above restores any
                # staged member).  A true kill never reaches here, so
                # durable checkpoints survive for the rerun.
                if not os.path.exists(
                        os.path.join(group_dir, _LINKED_COMMITTED)) \
                        and not any(os.path.exists(m.path + _BACKUP_SUFFIX)
                                    for m in members):
                    _sweep_linked(paths, group_dir)
                raise
        finally:
            for member in members:
                try:
                    member.src.close()
                except OSError:
                    pass

    # Counters count only records this invocation actually rewrote: a
    # member whose bytes never changed (canonical input, no dropped
    # records) reports zero, so member-level and group-level counts
    # always agree, and a resumed or idempotent run reports zero.
    def rewritten(m):
        return m.dirty or bool(m.dropped)

    totals_migrated = sum(m.run_migrated for m in members if rewritten(m))
    totals_skipped = sum(m.run_skipped for m in members if rewritten(m))
    totals_salvaged = sum(m.salvaged for m in members if rewritten(m))
    member_results = tuple(
        MigrationResult(
            path=m.path,
            records_migrated=m.run_migrated if rewritten(m) else 0,
            records_skipped=m.run_skipped if rewritten(m) else 0,
            records_salvaged=m.salvaged,
            replaced=replaced and rewritten(m),
        )
        for m in members
    )
    return LinkedMigrationResult(
        groups=tuple(tuple(g) for g in groups),
        records_migrated=totals_migrated,
        records_skipped=totals_skipped,
        references_bad=references_bad,
        records_salvaged=totals_salvaged,
        replaced=replaced,
        members=member_results,
        post_commit_error=post_commit_error,
    )


def _read_source_line(member, lineno0):
    """The raw bytes of one prepared source line (for error reports).

    Reads the prepared prefix sequentially: the source is append-only,
    so the first ``member.lineno`` lines are exactly the prepared ones.
    Works with or without the durable line index (online compaction
    removes it).
    """
    with open(member.path, "rb") as f:
        for _ in range(lineno0):
            if not f.readline():
                return b""
        return f.readline()


# ---------------------------------------------------------------------------
# Consistent cross-group snapshot
# ---------------------------------------------------------------------------


def read_linked_logs(groups, *, quiesce=DEFAULT_QUIESCE):
    """Return one version-consistent snapshot of every group at once.

    A list with one flat record list per group is returned (members in
    list order, lines in file order).  Across the whole snapshot --
    between groups, between members and inside a member -- pre-current
    and current field shapes never mix: while no member has crossed its
    commit border and every stored record is pre-current, records are
    handed back exactly as stored; otherwise every record is normalized
    to the current version in memory, which is exactly the eventual
    committed content.  The group list is validated exactly like
    :func:`migrate_linked_logs`.

    The *quiesce* argument is accepted for API symmetry; a snapshot
    never blocks on an active appender.
    """
    groups = _normalize_groups(groups)
    paths = [path for group in groups for path in group]
    _probe_members(paths)

    snapshots = [_read_member_gate(path, quiesce) for path in paths]
    any_post = any(post for _records, post in snapshots)
    all_old = all(
        not post and all(rec["v"] != CURRENT_VERSION for rec in records)
        for records, post in snapshots
    )
    flat = []
    if not any_post and all_old:
        flat = [list(records) for records, _post in snapshots]
    else:
        flat = [
            [migrate(rec, CURRENT_VERSION) for rec in records]
            for records, _post in snapshots
        ]
    out = []
    pos = 0
    for group in groups:
        out.append([
            rec
            for member_records in flat[pos:pos + len(group)]
            for rec in member_records
        ])
        pos += len(group)
    return out


# ---------------------------------------------------------------------------
# Cursor-based streaming snapshot
# ---------------------------------------------------------------------------
#
# read_linked_logs_stream serves the same single, version-consistent
# snapshot as read_linked_logs, but in batches: every call returns at
# most ``batch_records`` records plus an opaque string cursor, and the
# next call resumes exactly where the previous one stopped.  The
# snapshot is pinned at the first call -- every member's inode is held
# open and its length frozen -- so batches never repeat or lose records
# and never mix old/new field shapes, however long the stream stays
# open and whatever appenders do meanwhile.  The cursor is a plain
# string: it can be persisted and resumed later, in this process or
# (while the pinned inodes still resolve by path) in another one.

_CURSOR_VERSION = 1
_STREAM_SESSIONS = {}
_STREAM_LOCK = threading.Lock()
_STREAM_TOKENS = itertools.count(1)
# Stream sessions pin one open descriptor per member.  A cursor is
# fully self-describing (inode + frozen length + policy per member), so
# a session evicted from the registry -- by the idle reaper, the size
# cap, or an explicit close_linked_logs_stream -- is transparently
# rebuilt from the cursor on the next call; eviction never breaks a
# live cursor.
_STREAM_SESSION_TTL = 300.0       # idle seconds before a session is reaped
_STREAM_SESSION_LIMIT = 256       # registry cap; LRU eviction beyond it


def _open_path_wait(path, quiesce):
    """Open *path* for reading, riding out the mid-rename gap."""
    deadline = time.monotonic() + max(0.05, quiesce * 10)
    while True:
        try:
            return open(path, "rb")
        except FileNotFoundError:
            if time.monotonic() >= deadline:
                raise
            time.sleep(max(quiesce, 0.001))


def _probe_stream_member(path, quiesce):
    """The member the cursor points to must still exist and be readable."""
    deadline = time.monotonic() + max(0.05, quiesce * 10)
    while True:
        try:
            with open(path, "rb"):
                return
        except FileNotFoundError:
            if time.monotonic() >= deadline:
                raise FileNotFoundError(
                    f"linked stream member missing: {path!r}"
                )
            time.sleep(max(quiesce, 0.001))
        except OSError as exc:
            raise FileNotFoundError(
                f"linked stream member unreadable: {path!r}: {exc}"
            ) from exc


def _open_stream_session(paths, quiesce):
    """Pin every member's inode and freeze its snapshot length.

    Mirrors :func:`read_linked_logs`' marker gate per member: the
    descriptor pins one inode, the commit marker is rechecked after the
    open, and the snapshot covers exactly the bytes present at that
    moment.  Returns ``(fds, ends, posts)``.
    """
    fds = []
    try:
        ends = []
        posts = []
        for path in paths:
            marker = _commit_marker_path(path)
            post = os.path.exists(marker)
            fd = _open_path_wait(path, quiesce)
            try:
                st = os.fstat(fd.fileno())
            except OSError:
                fd.close()
                raise
            if not post and os.path.exists(marker):
                post = True
            fds.append(fd)
            ends.append(st.st_size)
            posts.append(post)
    except BaseException:
        for fd in fds:
            fd.close()
        raise
    return fds, ends, posts


def _stream_policy(fds, ends, posts):
    """1 = normalize every record to the current version, 0 = as stored.

    The decision mirrors :func:`read_linked_logs`: post-commit when any
    member crossed its border, else raw only when every stored record
    is pre-current.  The scan is streaming -- records are decoded one
    line at a time and never held -- so no group is loaded into memory.
    """
    if any(posts):
        return 1
    for fd, end in zip(fds, ends):
        fd.seek(0)
        pos = 0
        while pos < end:
            line = fd.readline(end - pos)
            if not line or not line.endswith(b"\n"):
                break
            pos += len(line)
            try:
                rec = loads(line)
            except ValueError:
                # Undecodable lines are not current-version records;
                # they raise when the stream reaches them, exactly like
                # the one-shot read.
                continue
            if rec["v"] == CURRENT_VERSION:
                return 1
    return 0


def _close_session_fds(session):
    for fd in session["fds"]:
        if fd is not None:
            try:
                fd.close()
            except OSError:
                pass


def _drop_stream_session(token):
    with _STREAM_LOCK:
        session = _STREAM_SESSIONS.pop(token, None)
    if session is not None:
        _close_session_fds(session)


def _reap_stream_sessions():
    """Reclaim pinned descriptors from idle/over-cap sessions.

    A cursor is self-describing, so dropping a session here is lossless:
    the next call rebuilds the pinned descriptors straight from the
    cursor (same inodes and frozen lengths).  This bounds the number of
    descriptors long-lived cursors may pin when callers abandon them
    without exhausting the stream.
    """
    now = time.monotonic()
    dead = []
    with _STREAM_LOCK:
        for token, session in list(_STREAM_SESSIONS.items()):
            if now - session.get("at", now) > _STREAM_SESSION_TTL:
                dead.append(_STREAM_SESSIONS.pop(token))
        while len(_STREAM_SESSIONS) > _STREAM_SESSION_LIMIT:
            oldest = min(
                _STREAM_SESSIONS,
                key=lambda t: _STREAM_SESSIONS[t].get("at", 0.0),
            )
            dead.append(_STREAM_SESSIONS.pop(oldest))
    for session in dead:
        _close_session_fds(session)


def close_linked_logs_stream(cursor):
    """Release the descriptors pinned for a still-open stream.

    The cursor itself stays usable afterwards: it is self-describing,
    so a later call reopens the pinned inodes and continues exactly
    where it stopped.  This lets a caller that abandons a stream early
    reclaim its descriptors immediately instead of waiting for the
    idle reaper.  A non-string cursor raises :class:`TypeError`;
    corrupt content raises :class:`ValueError`.
    """
    if not isinstance(cursor, str):
        raise TypeError("cursor must be a string")
    state = _decode_cursor(cursor)
    _drop_stream_session(state["tok"])


def _encode_cursor(token, session, mi, off):
    return json.dumps({
        "v": _CURSOR_VERSION,
        "tok": token,
        "pol": session["pol"],
        "mi": mi,
        "off": off,
        "mem": [list(pair) for pair in zip(session["inos"],
                                           session["ends"])],
    }, separators=(",", ":"))


def _decode_cursor(cursor):
    """Parse and validate a cursor string; corrupt content is a ValueError."""
    try:
        data = json.loads(cursor)
    except ValueError as exc:
        raise ValueError(f"cursor is not parseable: {exc}") from exc
    try:
        if not isinstance(data, dict) or data["v"] != _CURSOR_VERSION:
            raise ValueError("cursor is not parseable")
        token = data["tok"]
        pol = data["pol"]
        mi = data["mi"]
        off = data["off"]
        mem = data["mem"]
        if not isinstance(token, str) or pol not in (0, 1):
            raise ValueError("cursor is not parseable")
        for value in (mi, off):
            if not isinstance(value, int) or isinstance(value, bool) \
                    or value < 0:
                raise ValueError("cursor is not parseable")
        if not isinstance(mem, list):
            raise ValueError("cursor is not parseable")
        parsed = []
        for entry in mem:
            if not isinstance(entry, list) or len(entry) != 2:
                raise ValueError("cursor is not parseable")
            ino, end = entry
            for value in (ino, end):
                if not isinstance(value, int) or isinstance(value, bool) \
                        or value < 0:
                    raise ValueError("cursor is not parseable")
            parsed.append((ino, end))
        if mi >= len(parsed) or off > parsed[mi][1]:
            raise ValueError("cursor is not parseable")
    except (KeyError, TypeError) as exc:
        raise ValueError("cursor is not parseable") from exc
    return {"tok": token, "pol": pol, "mi": mi, "off": off, "mem": parsed}


def _reopen_stream_session(paths, state, quiesce):
    """Rebuild a session from a persisted cursor (e.g. another process).

    Every member is reopened by path and must still resolve to the
    inode the cursor pinned; a member that vanished, became unreadable
    or was replaced under the cursor raises FileNotFoundError.
    """
    fds = []
    try:
        inos = []
        for path, (ino, _end) in zip(paths, state["mem"]):
            try:
                fd = _open_path_wait(path, quiesce)
            except FileNotFoundError:
                raise FileNotFoundError(
                    f"linked stream member missing: {path!r}"
                )
            except OSError as exc:
                raise FileNotFoundError(
                    f"linked stream member unreadable: {path!r}: {exc}"
                ) from exc
            if os.fstat(fd.fileno()).st_ino != ino:
                fd.close()
                raise FileNotFoundError(
                    f"linked stream member replaced under cursor: {path!r}"
                )
            fds.append(fd)
            inos.append(ino)
    except BaseException:
        for fd in fds:
            fd.close()
        raise
    return {
        "fds": fds,
        "ends": [end for _ino, end in state["mem"]],
        "inos": inos,
        "pol": state["pol"],
    }


def _stream_emit(session, paths, mi, off, batch_records, quiesce):
    """One batch: up to *batch_records* records, never straddling a member."""
    records = []
    fds = session["fds"]
    ends = session["ends"]
    pol = session["pol"]
    total = len(paths)
    while mi < total and len(records) < batch_records:
        _probe_stream_member(paths[mi], quiesce)
        fd = fds[mi]
        end = ends[mi]
        fd.seek(off)
        while len(records) < batch_records and off < end:
            line = fd.readline(end - off)
            if not line:
                off = end
                break
            if not line.endswith(b"\n"):
                # A record caught mid-append at the snapshot instant is
                # excluded, exactly like the one-shot read.
                off = end
                break
            off += len(line)
            rec = loads(line)
            if pol:
                rec = migrate(rec, CURRENT_VERSION)
            records.append(rec)
        if off >= end:
            try:
                fd.close()
            finally:
                fds[mi] = None
            mi += 1
            off = 0
            if records:
                # Batches are member-aligned: an empty member is
                # skipped over, but a member that contributed records
                # ends the batch at its boundary.
                break
    return records, mi, off


def read_linked_logs_stream(groups, cursor=None, batch_records=4096, *,
                            batch_size=None, quiesce=DEFAULT_QUIESCE):
    """Read all groups' current content in cursor-resumable batches.

    Returns ``(records, next_cursor)``: a batch of at most
    *batch_records* decoded records -- never straddling a member, so
    batches advance strictly in group, member and line order -- and an
    opaque string cursor for the next call.  When the snapshot is
    exhausted the batch may be smaller (or empty) and *next_cursor* is
    ``None``.  Concatenating every batch reproduces exactly what
    :func:`read_linked_logs` returns for the same groups (flattened in
    group, member and line order): the snapshot is pinned at the first
    call, so no batch repeats or loses records and old/new field shapes
    never mix -- including while appenders keep writing and a
    path-reopening appender lands old-format records after a
    migration's wrap-up.  Members are read line by line; no group is
    ever loaded into memory wholesale.

    The cursor is a plain string and may be persisted between calls.
    A cursor that is not a string raises :class:`TypeError`; a string
    whose content is corrupt raises :class:`ValueError`; a cursor whose
    current member is missing or unreadable raises
    :class:`FileNotFoundError` (batches already returned are
    unaffected).  The group list is validated exactly like
    :func:`migrate_linked_logs`.  *batch_size* is accepted as an alias
    of *batch_records*.
    """
    groups = _normalize_groups(groups)
    paths = [path for group in groups for path in group]
    if batch_size is not None:
        batch_records = batch_size
    if isinstance(batch_records, bool) \
            or not isinstance(batch_records, int):
        raise TypeError("batch_records must be an integer")
    if batch_records < 1:
        raise ValueError("batch_records must be positive")
    if cursor is not None and not isinstance(cursor, str):
        raise TypeError("cursor must be a string or None")

    if cursor is None:
        _probe_members(paths)
        _reap_stream_sessions()
        fds, ends, posts = _open_stream_session(paths, quiesce)
        session = {
            "fds": fds,
            "ends": ends,
            "inos": [os.fstat(fd.fileno()).st_ino for fd in fds],
            "pol": _stream_policy(fds, ends, posts),
            "at": time.monotonic(),
        }
        token = f"{os.getpid()}-{next(_STREAM_TOKENS)}"
        with _STREAM_LOCK:
            _STREAM_SESSIONS[token] = session
        mi = off = 0
    else:
        state = _decode_cursor(cursor)
        if len(state["mem"]) != len(paths):
            raise ValueError("cursor does not match the group list")
        token = state["tok"]
        # Reap idle/over-cap sessions first; a reaped session is
        # transparently rebuilt from the self-describing cursor.
        _reap_stream_sessions()
        with _STREAM_LOCK:
            session = _STREAM_SESSIONS.get(token)
        if session is None:
            session = _reopen_stream_session(paths, state, quiesce)
            session["at"] = time.monotonic()
            with _STREAM_LOCK:
                _STREAM_SESSIONS[token] = session
        else:
            session["at"] = time.monotonic()
        mi, off = state["mi"], state["off"]

    try:
        records, mi, off = _stream_emit(
            session, paths, mi, off, batch_records, quiesce
        )
    except BaseException:
        _drop_stream_session(token)
        raise
    if mi >= len(paths):
        _drop_stream_session(token)
        return records, None
    return records, _encode_cursor(token, session, mi, off)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def run_linked_cli(argv):
    parser = argparse.ArgumentParser(
        prog="python3 -m proto_migrate migrate-linked-logs",
        description="Migrate linked groups of append-only JSONL logs "
        "with cross-group referential integrity, as one atomic unit: "
        "all groups end at the current version with every reference "
        "intact, or all stay untouched.",
    )
    parser.add_argument("--group", action="append", nargs="+",
                        required=True, metavar="FILE",
                        help="one log group (repeat per group, in "
                        "group order)")
    parser.add_argument("--link", action="append", default=[],
                        metavar="SRC:DST:SRC_FIELD:DST_FIELD",
                        help="declare a reference: records of group "
                        "SRC whose SRC_FIELD equals the DST_FIELD of a "
                        "record in group DST (repeatable)")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--strict", action="store_const", const="strict",
                      dest="on_bad", help="abort on the first bad line "
                      "or bad reference (default; exit %d, files "
                      "untouched)" % EXIT_BAD_RECORD)
    mode.add_argument("--skip", action="store_const", const="skip",
                      dest="on_bad", help="skip bad lines and drop "
                      "bad-reference records, auditing each to stderr "
                      "as '<file>:<lineno>:<first 32 bytes>'")
    parser.set_defaults(on_bad="strict")
    parser.add_argument("--segment-size", type=int,
                        default=DEFAULT_SEGMENT_SIZE, metavar="BYTES",
                        help="rotate temp segments at this size "
                        "(default: %(default)s)")
    parser.add_argument("--quiesce-ms", type=float,
                        default=DEFAULT_QUIESCE * 1000, metavar="MS",
                        help="EOF must hold this long before input is "
                        "considered complete (default: %(default)s)")
    args = parser.parse_args(argv)

    links = []
    for spec in args.link:
        parts = spec.split(":")
        if len(parts) != 4:
            print(f"error: malformed --link: {spec!r}", file=sys.stderr)
            return EXIT_USAGE
        try:
            src_g = int(parts[0])
            dst_g = int(parts[1])
        except ValueError:
            print(f"error: malformed --link: {spec!r}", file=sys.stderr)
            return EXIT_USAGE
        links.append((src_g, dst_g, parts[2], parts[3]))

    for group in args.group:
        for path in group:
            if not os.path.isfile(path):
                print(f"error: not a file: {path}", file=sys.stderr)
                return EXIT_ERROR
    try:
        result = migrate_linked_logs(
            args.group,
            links=links,
            on_bad=args.on_bad,
            segment_size=args.segment_size,
            quiesce=args.quiesce_ms / 1000,
        )
    except (LinkedBadReferenceError, BadRecordError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_BAD_RECORD
    except MigrationLockedError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_ERROR
    except (TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_USAGE
    except OSError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_ERROR
    print(
        "groups=%d members=%d migrated=%d skipped=%d refs_bad=%d "
        "salvaged=%d replaced=%s"
        % (
            len(result.groups),
            sum(len(g) for g in result.groups),
            result.records_migrated,
            result.records_skipped,
            result.references_bad,
            result.records_salvaged,
            "yes" if result.replaced else "no",
        )
    )
    if result.post_commit_error:
        # The atomic commit already succeeded: later faults show up as
        # a warning, never as a non-zero exit.
        print(f"warning: {result.post_commit_error}", file=sys.stderr)
    return EXIT_OK
