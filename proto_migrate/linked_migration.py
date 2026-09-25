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
untouched.  In ``"skip"`` mode bad lines are skipped exactly as in the
single-group migration and records holding bad references are dropped
from the migrated output; each dropped record is audited to the audit
stream as ``<filename>:<lineno>:<first 32 raw bytes>``.  The summary
counts bad records (``records_skipped``) and bad references
(``references_bad``) separately, and both counters only cover records
this invocation newly migrated and references it newly resolved.

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
warnings (exit 0), never failures.

:func:`read_linked_logs` returns one version-consistent snapshot of
every group in a single call -- old and new field shapes never mix
between groups, between members or inside a member, including while a
path-reopening appender writes old records during wrap-up.
"""

from __future__ import annotations

import argparse
import fcntl
import json
import os
import shutil
import sqlite3
import struct
import sys
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
    _converge,
    _crash_point,
    _fsync_dir,
    _read_stage_sidecar,
    _recover_backup_appends,
    _write_stage_sidecar,
)
from .group_migration import (
    GroupBadRecordError,
    _AuditPrefix,
    _acquire_member_locks,
    _prepare_member,
    _probe_members,
    _publish_member_marker,
    _read_checkpoint_log,
    _read_member_gate,
)

__all__ = [
    "LinkedBadReferenceError",
    "LinkedMigrationResult",
    "migrate_linked_logs",
    "read_linked_logs",
    "run_linked_cli",
]

_LINKED_DIR_NAME = ".migrate-linked-tmp"
_LINKED_LOCK_NAME = ".migrate-linked.lock"
_LINKED_COMMITTED = "linked-committed"
_LINKED_COMMITTED_TMP = "linked-committed.tmp"
_MANIFEST = "manifest.json"
_LINKED_PREPARED = "linked-prepared"
_LINKED_FINAL = "linked-final"
_LINES_NAME = "lines"
_REFS_DB = "refs.sqlite3"
_FINAL = "final"

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
    first_dir = os.path.dirname(os.path.abspath(groups[0][0]))
    return os.path.join(first_dir, _LINKED_DIR_NAME)


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


class _LineIndex:
    """Durable map of every source line a member's prepare consumed.

    One variable-length record per consumed line: a ``<qBBI`` header
    (source offset, kind, reserved, projection length) followed by the
    projection -- a JSON object of the link-involved string fields of
    the *migrated* record (or, for a skipped line, of a lenient parse
    of the raw line).  The index is fsynced before every checkpoint
    record, so a trusted checkpoint never covers unindexed lines.
    """

    def __init__(self, member_dir, fields):
        self._path = os.path.join(member_dir, _LINES_NAME)
        self._fields = tuple(sorted(fields))
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
        proj = self._project(kind, payload)
        blob = json.dumps(
            proj, ensure_ascii=True, separators=(",", ":"), sort_keys=True
        ).encode("ascii")
        self._fh.write(_LINE_HEADER.pack(offset, kind, 0, len(blob)))
        self._fh.write(blob)

    def _project(self, kind, payload):
        if not self._fields:
            return {}
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
        return {
            f: obj[f] for f in self._fields
            if isinstance(obj.get(f), str)
        }

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


def _prepare_linked_member(index, group_index, path, member_dir, on_bad,
                           segment_size, quiesce, audit_stream, fields,
                           bad_sink=None):
    """Prepare one member with its durable line index."""
    member = _prepare_member(
        path, member_dir, on_bad, segment_size, quiesce,
        _AuditPrefix(audit_stream, path),
        line_index=_LineIndex(member_dir, fields),
        bad_sink=bad_sink,
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
            line_index=_LineIndex(member_dir, fields),
        )
    _write_linked_prepared(member_dir, {
        "dirty": member.dirty, "offset": member.offset,
        "lineno": member.lineno, "fields": sorted(fields),
    })
    fresh_from = member.lineno - (member.run_migrated + member.run_skipped)
    return _LinkedMember(
        index, group_index, path, member_dir, member.src, member.dirty,
        member.offset, member.lineno, member.run_migrated,
        member.run_skipped, fresh_from,
    )


def _resume_linked_prepared(index, group_index, path, member_dir, on_bad,
                            fields):
    """Fast path: a member fully prepared before the kill is not redone.

    The durable line index only projects the link fields of the run
    that built it; a resume under a link set needing a field that index
    does not cover re-prepares the member, so reference resolution never
    reads a missing projection.
    """
    info = _read_linked_prepared(member_dir)
    if info is None:
        return None
    have_fields = set(info.get("fields", ()))
    if not set(fields) <= have_fields:
        shutil.rmtree(member_dir, ignore_errors=True)
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

        for lineno0, _offset, kind, proj in _stream_line_entries(
            member.member_dir
        ):
            if kind == LINE_GOOD:
                cur.execute(
                    "INSERT INTO nodes(mi, lineno) VALUES (?,?)",
                    (member.index, lineno0),
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
                            1 if lineno0 >= member.fresh_from else 0,
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

    Dirty members are filtered from their assembled ``final``; clean
    members (canonical bytes, never re-encoded) are filtered straight
    from the source byte ranges the line index recorded.  Both are
    byte-level operations -- nothing is rescanned or re-decoded.
    """
    out_path = os.path.join(member.member_dir, _LINKED_FINAL)
    if member.dirty:
        final = os.path.join(member.member_dir, _FINAL)
        entries = (
            entry for entry in _stream_line_entries(member.member_dir)
            if entry[2] == LINE_GOOD
        )
        with open(final, "rb") as fin, open(out_path, "wb") as out:
            for lineno0, _offset, _kind, _proj in entries:
                line = fin.readline()
                if lineno0 not in member.dropped:
                    out.write(line)
            out.flush()
            os.fsync(out.fileno())
    else:
        with open(member.path, "rb") as src, open(out_path, "wb") as out:
            for lineno0, offset, kind, _proj in _stream_line_entries(
                member.member_dir
            ):
                if kind != LINE_GOOD or lineno0 in member.dropped:
                    continue
                src.seek(offset)
                out.write(src.readline())
            out.flush()
            os.fsync(out.fileno())
    _fsync_dir(member.member_dir)


def _audit_dropped(member, audit_stream):
    """One ``<file>:<lineno>:<first 32 bytes>`` entry per dropped record."""
    prefix = _AuditPrefix(audit_stream, member.path)
    with open(member.path, "rb") as src:
        for lineno0, offset, kind, _proj in _stream_line_entries(
            member.member_dir
        ):
            if kind != LINE_GOOD or lineno0 not in member.dropped:
                continue
            src.seek(offset)
            prefix.write(_audit_line(lineno0 + 1, src.readline()))
    audit_stream.flush()


# ---------------------------------------------------------------------------
# Two-phase rename
# ---------------------------------------------------------------------------


def _stage_linked_member(member, on_bad):
    """Phase 1: promote this member's output over its path; backup held."""
    path = member.path
    parent = os.path.dirname(os.path.abspath(path))
    d = _linked_debris(path)
    src_name = _LINKED_FINAL if member.dropped else _FINAL
    src_path = os.path.join(member.member_dir, src_name)
    # Durably record how a committed-run recovery must fold appends
    # stranded on the backup, before any rename makes the backup exist.
    _write_stage_sidecar(member.member_dir, {
        "staged_size": os.path.getsize(src_path),
        "offset": member.offset,
        "lineno": member.lineno,
        "mode": on_bad,
        "inode": os.stat(path).st_ino,
        "filtered": bool(member.dropped),
    })
    os.replace(src_path, d["staged"])
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
    member is finished exactly like an idempotent single-group rerun:
    the current inode is rescanned (re-encoding a canonical file
    changes no bytes), any old-format tail a path-reopening appender
    landed after wrap-up is migrated through a fresh atomic
    replacement, and racing appenders are converged.  Counters count
    only records this invocation newly moves.
    """
    warnings = []
    member_results = []
    salvaged_total = skipped_total = migrated_total = 0
    retained = set()

    for path in paths:
        parent = os.path.dirname(os.path.abspath(path))
        d = _linked_debris(path)
        member_dir = _linked_member_dir(path)

        if os.path.exists(d["backup"]):
            # Fold appends stranded on the killed run's backup before
            # touching the live path; only then may the backup go.
            sidecar = _read_stage_sidecar(member_dir)
            keep = True
            if sidecar is not None:
                try:
                    backup_inode = os.stat(d["backup"]).st_ino
                except OSError as exc:
                    backup_inode = None
                    warnings.append(
                        f"{path}: cannot stat surviving backup: {exc}"
                    )
                if backup_inode is not None:
                    if sidecar["inode"] != backup_inode \
                            or sidecar["mode"] != on_bad:
                        warnings.append(
                            f"{path}: surviving backup does not match the "
                            "staging record; backup retained, rerun after "
                            "inspection"
                        )
                    else:
                        appended, bskip, retain, warning = (
                            _recover_backup_appends(
                                path, d["backup"], sidecar["offset"],
                                on_bad, quiesce,
                                _AuditPrefix(audit_stream, path),
                                base_size=sidecar["staged_size"],
                                filtered=sidecar.get("filtered", False),
                            )
                        )
                        migrated_total += appended
                        salvaged_total += appended
                        skipped_total += bskip
                        if warning:
                            warnings.append(f"{path}: {warning}")
                        keep = retain
            else:
                warnings.append(
                    f"{path}: surviving backup without a staging record; "
                    "backup retained, rerun after inspection"
                )
            if keep:
                retained.add(path)
                member_results.append(MigrationResult(
                    path=path, records_migrated=0, records_skipped=0,
                    records_salvaged=0, replaced=False,
                ))
                continue
            try:
                _remove(d["backup"])
                _fsync_dir(parent)
            except OSError as exc:
                warnings.append(f"{path}: backup removal failed: {exc}")
                retained.add(path)
                member_results.append(MigrationResult(
                    path=path, records_migrated=0, records_skipped=0,
                    records_salvaged=0, replaced=False,
                ))
                continue

        shutil.rmtree(member_dir, ignore_errors=True)
        member_replaced = False
        salvaged = conv_skipped = 0
        try:
            member = _prepare_member(
                path, member_dir, on_bad, segment_size, quiesce,
                _AuditPrefix(audit_stream, path),
            )
            if member.dirty:
                staged_path = os.path.join(member_dir, _FINAL)
                _write_stage_sidecar(member_dir, {
                    "staged_size": os.path.getsize(staged_path),
                    "offset": member.offset,
                    "lineno": member.lineno,
                    "mode": on_bad,
                    "inode": os.stat(path).st_ino,
                    "filtered": False,
                })
                os.replace(staged_path, path + _STAGED_SUFFIX)
                _publish_member_marker(path)
                os.replace(path, path + _BACKUP_SUFFIX)
                os.replace(path + _STAGED_SUFFIX, path)
                _fsync_dir(parent)
                member_replaced = True
                salvaged, conv_skipped, _lineno, warning = _converge(
                    path, parent, member_dir, member.src, member.offset,
                    member.lineno, quiesce,
                    _AuditPrefix(audit_stream, path), on_bad,
                )
                if warning:
                    warnings.append(f"{path}: {warning}")
                _remove(path + _BACKUP_SUFFIX)
                _fsync_dir(parent)
            try:
                member.src.close()
            except OSError:
                pass
            migrated_total += salvaged
            skipped_total += conv_skipped
            salvaged_total += salvaged
            member_results.append(MigrationResult(
                path=path, records_migrated=salvaged,
                records_skipped=conv_skipped, records_salvaged=salvaged,
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

    if retained:
        for path in paths:
            if path not in retained:
                shutil.rmtree(_linked_member_dir(path), ignore_errors=True)
    else:
        try:
            _sweep_linked(paths, group_dir)
        except OSError as exc:
            warnings.append(f"cleanup failed: {exc}")
    return migrated_total, skipped_total, salvaged_total, \
        tuple(member_results), warnings


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


def migrate_linked_logs(groups, *, links=(), on_bad="strict",
                        segment_size=DEFAULT_SEGMENT_SIZE,
                        quiesce=DEFAULT_QUIESCE, audit=None,
                        _group_dir_override=None, _lock_path_override=None,
                        _member_lock_fhs=None):
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
    references it newly resolves.

    Raises :class:`TypeError` for a non-sequence group list,
    :class:`ValueError` for an empty or duplicated list (and
    :class:`LinkedBadReferenceError` / :class:`GroupBadRecordError` on
    the first bad reference or bad line in strict mode), and
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
    first_dir = os.path.dirname(os.path.abspath(groups[0][0]))
    group_dir = (_group_dir_override
                 if _group_dir_override is not None
                 else _linked_group_dir(groups))
    lock_path = (_lock_path_override
                 if _lock_path_override is not None
                 else os.path.join(first_dir, _LINKED_LOCK_NAME))

    # Link-involved fields, and per-group source/target link views.
    fields = set()
    dst_links = {}   # group index -> [(link_id, dst_field)]
    src_links = {}   # group index -> [(link_id, src_field)]
    for link_id, (src_g, dst_g, src_field, dst_field) in enumerate(links):
        fields.add(src_field)
        fields.add(dst_field)
        dst_links.setdefault(dst_g, []).append((link_id, dst_field))
        src_links.setdefault(src_g, []).append((link_id, src_field))

    members = []
    staged_members = []
    replaced = False
    references_bad = 0
    post_commit_error = None

    with open(lock_path, "a+b") as group_lock:
        fcntl.flock(group_lock, fcntl.LOCK_EX)
        if _member_lock_fhs is not None:
            # Coordinated run: the per-member leases were already
            # acquired (non-blocking) by the coordinator and stay
            # owned by it for the whole run.
            member_locks = list(_member_lock_fhs)
            owns_member_locks = False
        else:
            member_locks = _acquire_member_locks(paths)
            owns_member_locks = True
        try:
            _crash_point("linked-lock")

            manifest = _read_linked_manifest(group_dir)
            committed = os.path.exists(
                os.path.join(group_dir, _LINKED_COMMITTED)
            )
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
            # In strict mode every member is prepared leniently: bad
            # lines are collected rather than raised, so after
            # references are resolved the globally first bad line and
            # the globally first bad reference can be compared in
            # group/member/line order -- an earlier bad reference must
            # be reported ahead of a later bad line.
            member_bad = {}
            try:
                index = 0
                for group_index, group in enumerate(groups):
                    for path in group:
                        member_dir = _linked_member_dir(path)
                        member = _resume_linked_prepared(
                            index, group_index, path, member_dir, on_bad,
                            fields,
                        )
                        if member is None:
                            sink = [] if on_bad == "strict" else None
                            member = _prepare_linked_member(
                                index, group_index, path, member_dir,
                                on_bad, segment_size, quiesce,
                                audit_stream, fields, bad_sink=sink,
                            )
                            if sink:
                                member_bad[index] = sink
                        elif on_bad == "strict":
                            # Fast-resumed member: its bad lines survive
                            # only as line-index kinds; recover the first
                            # one for the global first-error comparison.
                            for lineno0, _off, kind, _proj in (
                                    _stream_line_entries(member_dir)):
                                if kind != LINE_GOOD:
                                    raw = _read_source_line(member, lineno0)
                                    cause = "bad record"
                                    try:
                                        loads(raw)
                                    except ValueError as exc:
                                        cause = str(exc)
                                    member_bad[index] = [
                                        (lineno0 + 1, raw, cause)
                                    ]
                                    break
                        members.append(member)
                        index += 1
                _crash_point("linked-prepare")

                # --- RESOLVE references from the durable line indexes.
                # The spill database is rebuilt on every uncommitted
                # run; building it reads only the per-member line
                # indexes, never the sources.
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

                if on_bad == "strict":
                    first_line = None
                    for member in members:
                        sink = member_bad.get(member.index)
                        if sink:
                            first_line = (member, sink[0])
                            break
                    if first_line is not None and (
                            first is None
                            or (first_line[0].index,
                                first_line[1][0] - 1)
                            <= (first[0], first[1])):
                        bad_member, (lineno, raw, cause) = first_line
                        raise GroupBadRecordError(
                            bad_member.path, lineno, raw, cause
                        )
                if on_bad == "strict" and first is not None:
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
                            _stage_linked_member(member, on_bad)
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
            if owns_member_locks:
                for fh in member_locks:
                    fcntl.flock(fh, fcntl.LOCK_UN)
                    fh.close()

    totals_migrated = sum(m.run_migrated for m in staged_members)
    totals_skipped = sum(m.run_skipped for m in staged_members)
    totals_salvaged = sum(m.salvaged for m in staged_members)
    member_results = tuple(
        MigrationResult(
            path=m.path,
            records_migrated=m.run_migrated,
            records_skipped=m.run_skipped,
            records_salvaged=m.salvaged,
            replaced=replaced and (m.dirty or bool(m.dropped)),
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
    """The raw bytes of one prepared source line (for error reports)."""
    for n, offset, _kind, _proj in _stream_line_entries(member.member_dir):
        if n == lineno0:
            with open(member.path, "rb") as f:
                f.seek(offset)
                return f.readline()
    return b""


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
