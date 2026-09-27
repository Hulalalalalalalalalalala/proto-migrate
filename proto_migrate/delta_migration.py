"""Incremental, ledger-based cross-group linked migration.

:func:`migrate_linked_delta` is the incremental companion of
:func:`proto_migrate.migrate_linked_logs` (subcommand
``delta-migrate-linked-logs``).  Where a full linked migration rewrites
every member from byte zero, the delta entry only processes the rows
*appended since the previous migration*; the already reconciled prefix
is neither rescanned nor rewritten.  The inputs (``--group`` /
``--link`` / ``--strict|--skip``) are identical to the full entry.

The reconciliation ledger
--------------------------
Every member carries a small local ledger,
``<path>.migrate-delta-ledger``: a plain JSON file pinned to the
member's live inode recording

  * the exact byte offset and output-line count of the reconciled
    prefix (everything a prior migration already settled),
  * the surviving reference-target key per declared link present in
    that prefix, so a new row's reference to an old key resolves
    without a prefix rescan,
  * the group list, link set and bad-record policy the ledger was
    produced under.

The ledger lives only in local files, has no remote dependency and can
always be rebuilt: if it is missing, unreadable/corrupt, names a
different configuration, or its source inode has changed, *every*
member is processed from offset zero through the very same pipeline --
a deterministic full migration -- and the result records the fallback
reason.  Because the conversion, reference-resolution and drop rules
are the full entry's, that fallback leaves byte-for-byte the same files
a full migration would.

Counters
--------
Each member reports four reconciliation counts covering the rows this
invocation newly processes (a prefix an earlier, killed attempt already
prepared is not recounted):

  * ``lines_added``         -- appended source rows settled;
  * ``records_rewritten``   -- appended surviving good rows whose bytes
                               change (an old version re-encoded);
  * ``records_skipped``     -- appended bad records (skip mode);
  * ``records_dropped``     -- appended good records discarded for a
                               bad reference.

The group result additionally carries ``references_bad`` (bad
references -- edges, not records -- among the newly parsed rows), like
:func:`migrate_linked_logs`.  The four counts are exactly the summary a
full migration of the same appended segment produces, and the finished
files are byte-for-byte identical to one uninterrupted full migration.

Concurrency, leases and crashes
-------------------------------
Lease, appender and crash semantics are the full linked entry's:
non-blocking per-member ``flock`` leases (overlap is mutually
exclusive, disjoint group sets run in parallel,
:class:`MigrationLockedError` while a lease is held by a live
instance), whole-record appends are drained in their original order
with zero loss and zero duplication, and a kill at any point resumes
only the unfinished part.  The group commit marker ``delta-committed``
is the single success/failure border.

Strict mode keeps the global first-error order -- groups in list
order, members in list order, lines in file order, a bad line compared
against a bad reference -- and raises
:class:`~proto_migrate.group_migration.GroupBadRecordError` /
:class:`LinkedBadReferenceError` (both :class:`ValueError`).
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
from typing import NamedTuple

from . import loads
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
    _assemble,
    _audit_line,
    _classify_bad_line,
    _commit_marker_path,
    _converge,
    _convert,
    _crash_point,
    _emit,
    _fsync_dir,
    _prepare_workdir,
    _publish_prefix_checkpoint,
    _read_lines,
)
from .group_migration import (
    GroupBadRecordError,
    _AuditPrefix,
    _probe_members,
    _publish_member_marker,
)
from .linked_migration import (
    LinkedBadReferenceError,
    MigrationLockedError,
    _LineIndex,
    _REASON_TEXT,
    _RefsDb,
    _member_leases,
    _normalize_groups,
    _normalize_links,
    _read_checkpoint_log,
    _remove,
    _salvage_backup_tail,
    _stream_line_entries,
)

__all__ = [
    "DeltaMigrationResult",
    "DeltaMemberResult",
    "migrate_linked_delta",
    "run_delta_cli",
]

_DELTA_DIR_NAME = ".migrate-delta-tmp"
_DELTA_COMMITTED = "delta-committed"
_DELTA_COMMITTED_TMP = "delta-committed.tmp"
_DELTA_MANIFEST = "manifest.json"
_DELTA_PREPARED = "delta-prepared"
_DELTA_SCAN = "delta-scan.json"
_DELTA_SUMMARY = "segment-summary.json"
_DELTA_ACK = "segment-ack"
_DELTA_SUFFIX_FINAL = "suffix-final"
_DELTA_FINAL = "final"
_DELTA_LINKED_FINAL = "linked-final"
_REFS_DB = "refs.sqlite3"

_MEMBER_TMP_SUFFIX = ".migrate-delta-tmp"
_BACKUP_SUFFIX = ".migrate-delta-backup"
_STAGED_SUFFIX = ".migrate-delta-staged"
_LEDGER_SUFFIX = ".migrate-delta-ledger"

_FAULT_ENV = "PROTO_MIGRATE_FAULT_AT"
_LEDGER_VERSION = 1
# Reconciled-prefix target keys seed node_key with this placeholder node
# (never inserted into ``nodes``): a new edge to such a key resolves,
# but the sentinel can never itself be dropped or cascade-bad.
_SENTINEL_NODE = -1


class DeltaMemberResult(NamedTuple):
    """The four per-member reconciliation counts."""

    path: str
    group: int
    member: int
    lines_added: int
    records_rewritten: int
    records_skipped: int
    records_dropped: int
    replaced: bool


class DeltaMigrationResult(NamedTuple):
    groups: tuple
    records_added: int
    records_rewritten: int
    records_skipped: int
    records_dropped: int
    references_bad: int
    replaced: bool
    members: tuple
    fallback: bool
    fallback_reason: str | None
    post_commit_error: str | None = None


# ---------------------------------------------------------------------------
# Work-directory layout
# ---------------------------------------------------------------------------


def _delta_group_dir(groups):
    import hashlib

    first_dir = os.path.dirname(os.path.abspath(groups[0][0]))
    key = json.dumps(
        [[os.path.abspath(p) for p in g] for g in groups],
        separators=(",", ":"),
    )
    digest = hashlib.sha1(key.encode("utf-8")).hexdigest()[:16]
    return os.path.join(first_dir, f"{_DELTA_DIR_NAME}-{digest}")


def _delta_member_dir(path):
    return path + _MEMBER_TMP_SUFFIX


def _ledger_path(path):
    return path + _LEDGER_SUFFIX


def _delta_debris(path):
    marker = _commit_marker_path(path)
    return {
        "staged": path + _STAGED_SUFFIX,
        "backup": path + _BACKUP_SUFFIX,
        "marker": marker,
        "marker_tmp": marker + ".tmp",
    }


def _sweep_delta(paths, group_dir):
    for path in paths:
        shutil.rmtree(_delta_member_dir(path), ignore_errors=True)
    shutil.rmtree(group_dir, ignore_errors=True)


def _write_delta_manifest(group_dir, groups, links, on_bad):
    data = {
        "version": 1,
        "on_bad": on_bad,
        "groups": [[os.path.abspath(p) for p in g] for g in groups],
        "links": [list(link) for link in links],
    }
    tmp = os.path.join(group_dir, _DELTA_MANIFEST + ".tmp")
    with open(tmp, "wb") as f:
        f.write(json.dumps(data).encode("ascii"))
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, os.path.join(group_dir, _DELTA_MANIFEST))
    _fsync_dir(group_dir)


def _read_delta_manifest(group_dir):
    try:
        with open(os.path.join(group_dir, _DELTA_MANIFEST), "rb") as f:
            return json.loads(f.read())
    except (OSError, ValueError):
        return None


def _write_json_atomic(path, data):
    with open(path + ".tmp", "wb") as f:
        f.write(json.dumps(data, sort_keys=True).encode("ascii"))
        f.flush()
        os.fsync(f.fileno())
    os.replace(path + ".tmp", path)
    _fsync_dir(os.path.dirname(os.path.abspath(path)))


def _write_segment_summary(group_dir, members, references_bad,
                           fallback=False, fallback_reason=None,
                           converged=False):
    """Durably record the segment's resolved counters (replayed on
    recovery past the commit marker).

    With *converged* true the post-border convergence has run, so rows
    it salvaged (and bad rows it skipped) are folded into the totals --
    matching exactly what an uninterrupted run reports.
    """
    per_member = []
    for m in members:
        salv = m.salvaged if converged else 0
        conv_skip = m.conv_skipped if converged else 0
        per_member.append({
            "index": m.index, "group": m.group_index,
            "member": m.member_index, "path": m.path,
            "good": m.suffix_good + salv,
            "bad": m.suffix_bad + conv_skip,
            "rewritten": len(m.changed - m.dropped) + salv,
            "dropped": len(m.dropped),
            "will_stage": m.will_stage,
        })
    _write_json_atomic(os.path.join(group_dir, _DELTA_SUMMARY), {
        "version": 1,
        "references_bad": references_bad,
        "fallback": bool(fallback),
        "fallback_reason": fallback_reason,
        "members": per_member,
    })


def _read_segment_summary(group_dir):
    try:
        with open(os.path.join(group_dir, _DELTA_SUMMARY), "rb") as f:
            data = json.loads(f.read())
        if not isinstance(data, dict) or data.get("version") != 1:
            return None
        return data
    except (OSError, ValueError):
        return None


def _segment_acknowledged(group_dir):
    """Whether the segment's ledgers were already durably published.

    The acknowledgment is written after the ledgers and before the work
    state is swept; a recovery that finds it reports nothing for that
    segment (only rows it newly converges), so a rerun following a kill
    during/after cleanup counts zero.
    """
    return os.path.exists(os.path.join(group_dir, _DELTA_ACK))


def _write_segment_ack(group_dir):
    tmp = os.path.join(group_dir, _DELTA_ACK + ".tmp")
    with open(tmp, "wb") as f:
        f.write(b"1\n")
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, os.path.join(group_dir, _DELTA_ACK))
    _fsync_dir(group_dir)


def _reconcile_delta_member(path, committed):
    """Resolve one member's interrupted rename state, deterministically."""
    d = _delta_debris(path)
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
# Ledger
# ---------------------------------------------------------------------------


def _read_one_ledger(path, abs_groups, links_list, on_bad):
    """Return a validated ledger dict, or ``(None, reason)``."""
    try:
        with open(_ledger_path(path), "rb") as f:
            data = json.loads(f.read())
    except FileNotFoundError:
        return None, "ledger missing"
    except (OSError, ValueError):
        return None, "ledger corrupt"
    if not isinstance(data, dict) or data.get("version") != _LEDGER_VERSION:
        return None, "ledger corrupt"
    if (
        data.get("groups") != abs_groups
        or data.get("links") != links_list
        or data.get("on_bad") != on_bad
    ):
        return None, "ledger configuration changed"
    try:
        st = os.stat(path)
        offset = int(data["offset"])
        lineno = int(data["lineno"])
        inode = int(data["inode"])
        raw_keys = data["keys"]
    except (OSError, KeyError, TypeError, ValueError):
        return None, "ledger corrupt"
    if st.st_ino != inode:
        return None, "source inode changed"
    if offset < 0 or lineno < 0 or offset > st.st_size:
        return None, "ledger corrupt"
    if not isinstance(raw_keys, dict):
        return None, "ledger corrupt"
    keys = {}
    for link_id_s, values in raw_keys.items():
        try:
            link_id = int(link_id_s)
        except (TypeError, ValueError):
            return None, "ledger corrupt"
        if not (isinstance(values, list)
                and all(isinstance(v, str) for v in values)):
            return None, "ledger corrupt"
        keys[link_id] = set(values)
    return {
        "inode": inode,
        "offset": offset,
        "lineno": lineno,
        "keys": keys,
    }, None


def _load_ledgers(groups, links, on_bad):
    """Decide each member's scan start.

    Returns ``(starts, fallback, reason)`` with one
    ``(offset, lineno, old_keys)`` per flat member.  Any unusable
    ledger deterministically zeroes *every* start (a full migration)
    and *reason* names the first cause in group/member order.
    """
    abs_groups = [[os.path.abspath(p) for p in g] for g in groups]
    links_list = [list(l) for l in links]
    parsed = []
    bad_reason = None
    for group in groups:
        for path in group:
            ledger, why = _read_one_ledger(
                path, abs_groups, links_list, on_bad)
            if ledger is None and bad_reason is None:
                bad_reason = f"{path}: {why}"
            parsed.append(ledger)
    if bad_reason is not None:
        # Any unusable ledger deterministically processes every member
        # from byte zero -- a full migration.
        return [(0, 0, {}) for _ledger in parsed], True, bad_reason
    starts = [
        (ledger["offset"], ledger["lineno"], ledger["keys"])
        for ledger in parsed
    ]
    return starts, False, None


def _write_ledger(path, groups, links, on_bad, inode, offset, lineno, keys):
    data = {
        "version": _LEDGER_VERSION,
        "groups": [[os.path.abspath(p) for p in g] for g in groups],
        "links": [list(l) for l in links],
        "on_bad": on_bad,
        "path": os.path.abspath(path),
        "inode": inode,
        "offset": offset,
        "lineno": lineno,
        "keys": {
            str(link_id): sorted(values)
            for link_id, values in sorted(keys.items())
            if values
        },
    }
    _write_json_atomic(_ledger_path(path), data)


# ---------------------------------------------------------------------------
# Delta member
# ---------------------------------------------------------------------------


class _DeltaMember:
    def __init__(self, index, group_index, member_index, path, member_dir,
                 src, start_offset, start_lineno, old_keys):
        self.index = index
        self.group_index = group_index
        self.member_index = member_index
        self.path = path
        self.member_dir = member_dir
        self.src = src
        self.start_offset = start_offset
        self.start_lineno = start_lineno
        self.old_keys = old_keys

        # Filled by prepare.  Every counter describes the WHOLE delta
        # segment (the suffix past the ledger boundary), not just the
        # rows the current process scans: a recovery after a kill must
        # report the same summary as one uninterrupted run, and the
        # durable per-line index is the source of truth for it.
        self.dirty = False
        self.offset = start_offset   # drain-end offset on source inode
        self.lineno = start_lineno   # drain-end source line number
        self.suffix_good = 0         # good rows in the whole suffix
        self.suffix_bad = 0          # bad rows in the whole suffix
        self.changed = set()         # suffix entries whose bytes change
        self.dropped = set()         # suffix entries dropped (bad refs)
        self.will_stage = False
        self.staged_size = 0
        self.salvaged = 0
        self.conv_skipped = 0

    @property
    def lines_added(self):
        # Good + bad rows settled for this segment, including rows
        # drained by the post-border convergence of racing appenders.
        return self.suffix_good + self.suffix_bad + self.salvaged \
            + self.conv_skipped

    @property
    def records_skipped(self):
        return self.suffix_bad + self.conv_skipped

    @property
    def records_dropped(self):
        return len(self.dropped)

    @property
    def records_rewritten(self):
        # Surviving appended rows whose bytes change (a dropped record
        # never reaches the output); rows already canonical are not
        # rewritten.  Post-convergence salvaged rows are always encoded,
        # so they are always rewritten.
        return len(self.changed - self.dropped) + self.salvaged


def _write_scan_marker(member_dir, inode, on_bad, offset, lineno):
    _write_json_atomic(os.path.join(member_dir, _DELTA_SCAN), {
        "inode": inode, "on_bad": on_bad, "offset": offset,
        "lineno": lineno,
    })


def _read_scan_marker(member_dir):
    try:
        with open(os.path.join(member_dir, _DELTA_SCAN), "rb") as f:
            return json.loads(f.read())
    except (OSError, ValueError):
        return None


def _prepare_delta_member(index, group_index, member_index, path,
                          member_dir, on_bad, segment_size, quiesce,
                          audit_stream, start_offset, start_lineno,
                          old_keys):
    """Scan exactly the suffix past the ledger boundary.

    Same segmented output, fsynced checkpoints and durable per-line
    index as a full prepare -- but the scan starts at the ledger
    offset and the reconciled prefix is never read.  In strict mode a
    bad line is deferred (recorded, never raised) so the caller can run
    its global first-error comparison.

    Returns the populated :class:`_DeltaMember`; every counter is the
    segment-wide one recomputed from the durable per-line index, so a
    recovery reports the same summary as an uninterrupted run.
    """
    current_inode = os.stat(path).st_ino
    state = _prepare_workdir(
        path, member_dir, segment_size, current_inode, on_bad
    )
    writer = state["writer"]
    cp_fh = state["cp_fh"]
    resumed = state["resumed"]
    follow_window = max(1.0, quiesce * 20)

    marker = _read_scan_marker(member_dir)
    if resumed:
        # The scan marker must agree with the ledger boundary; if it
        # does not, the checkpoints belong to an older decision and the
        # member is rebuilt from the correct boundary.
        if not (isinstance(marker, dict)
                and marker.get("inode") == current_inode
                and marker.get("on_bad") == on_bad
                and marker.get("offset") == start_offset
                and marker.get("lineno") == start_lineno):
            cp_fh.close()
            writer.abort()
            shutil.rmtree(member_dir, ignore_errors=True)
            return _prepare_delta_member(
                index, group_index, member_index, path, member_dir,
                on_bad, segment_size, quiesce, audit_stream,
                start_offset, start_lineno, old_keys)
        offset = state["offset"]
        migrated = state["count"]
        skipped = state["skipped"]
        dirty = state["dirty"]
    else:
        _write_scan_marker(member_dir, current_inode, on_bad,
                           start_offset, start_lineno)
        offset = start_offset
        migrated = skipped = 0
        dirty = False

    lineno = start_lineno + migrated + skipped
    boundary_entries = migrated + skipped  # suffix entries at a resume
    line_index = _LineIndex(member_dir)
    line_index.begin(boundary_entries, resumed)

    changed = set()
    src = open(path, "rb")

    def handle(raw, line_start):
        """Classify one line; return (out, bad)."""
        entry = migrated + skipped
        try:
            out, bad = _emit(audit_stream, on_bad, raw,
                             start_lineno + entry + 1)
        except BadRecordError as exc:
            if on_bad != "strict":
                raise GroupBadRecordError(
                    path, exc.lineno, exc.raw, exc.cause) from exc
            out, bad = None, True
        if bad:
            line_index.record(line_start, _classify_bad_line(raw), raw)
        else:
            line_index.record(line_start, LINE_GOOD, out)
            if raw != out:
                changed.add(entry)
        return out, bad
    try:
        for raw in _read_lines(src, quiesce, offset=offset,
                               follow=follow_window):
            line_start = offset
            offset += len(raw)
            out, bad = handle(raw, line_start)
            if bad:
                skipped += 1
                dirty = True
                continue
            if raw != out:
                dirty = True
            writer.write(out)
            migrated += 1
            if writer.size >= writer.limit:
                name = writer.current_segment
                writer.close()
                line_index.sync()
                _publish_prefix_checkpoint(
                    cp_fh, member_dir, offset, migrated, skipped,
                    dirty, name,
                    os.path.getsize(os.path.join(member_dir, name)),
                )

        if writer.has_open_segment:
            name = writer.current_segment
            writer.close()
            line_index.sync()
            _publish_prefix_checkpoint(
                cp_fh, member_dir, offset, migrated, skipped, dirty,
                name, os.path.getsize(os.path.join(member_dir, name)),
            )
    except BaseException:
        writer.abort()
        cp_fh.close()
        src.close()
        line_index.close()
        raise
    writer.abort()
    cp_fh.close()

    member = _DeltaMember(
        index, group_index, member_index, path, member_dir, src,
        start_offset, start_lineno, old_keys,
    )
    member.dirty = dirty

    # Assemble the migrated suffix (scan segments + drain tail).
    assembled = os.path.join(member_dir, _DELTA_SUFFIX_FINAL)
    if os.path.exists(assembled):
        os.remove(assembled)
    segments = sorted(
        n for n in os.listdir(member_dir) if n.startswith("seg-"))
    _assemble(member_dir, segments, assembled)
    try:
        with open(assembled, "ab") as out_fh:
            for raw in _read_lines(src, quiesce, offset=offset,
                                   follow=follow_window):
                line_start = offset
                offset += len(raw)
                out, bad = handle(raw, line_start)
                if bad:
                    skipped += 1
                    dirty = True
                    continue
                if raw != out:
                    dirty = True
                out_fh.write(out)
                migrated += 1
            out_fh.flush()
            os.fsync(out_fh.fileno())
    except BaseException:
        src.close()
        line_index.close()
        raise
    line_index.sync()
    line_index.close()

    member.dirty = dirty
    member.offset = offset
    member.lineno = start_lineno + migrated + skipped
    _refresh_member_stats(member)
    _write_json_atomic(os.path.join(member_dir, _DELTA_PREPARED), {
        "dirty": dirty, "offset": offset,
        "lineno": start_lineno + migrated + skipped,
        "start_offset": start_offset, "start_lineno": start_lineno,
        "good": member.suffix_good, "bad": member.suffix_bad,
    })
    return member


def _refresh_member_stats(member):
    """Recompute segment-wide counters from the durable line index.

    The index classifies every suffix row the prepare consumed; a good
    row is "rewritten" when its stored bytes differ from the canonical
    re-encoding.  Reading only the index plus the named source byte
    ranges makes the result identical for a fresh scan and a resume.
    """
    good = bad = 0
    changed = set()
    with open(member.path, "rb") as raw_src:
        for lineno0, off, kind, _proj in _stream_line_entries(
                member.member_dir):
            if kind == LINE_GOOD:
                good += 1
                raw_src.seek(off)
                raw = raw_src.readline()
                try:
                    if raw != _convert(raw):
                        changed.add(lineno0)
                except ValueError:
                    pass
            else:
                bad += 1
    member.suffix_good = good
    member.suffix_bad = bad
    member.changed = changed


def _resume_delta_prepared(index, group_index, member_index, path,
                           member_dir, on_bad, start):
    """Fast path: a member fully prepared before a kill is not redone."""
    start_offset, start_lineno, old_keys = start
    try:
        with open(os.path.join(member_dir, _DELTA_PREPARED), "rb") as f:
            info = json.loads(f.read())
    except (OSError, ValueError):
        return None
    if not isinstance(info, dict):
        return None
    inode, mode, _records, _good = _read_checkpoint_log(member_dir)
    try:
        current_inode = os.stat(path).st_ino
    except OSError:
        return None
    if inode != current_inode or mode != on_bad:
        return None
    if info.get("start_offset") != start_offset \
            or info.get("start_lineno") != start_lineno:
        shutil.rmtree(member_dir, ignore_errors=True)
        return None
    if not os.path.isfile(os.path.join(member_dir, _DELTA_SUFFIX_FINAL)):
        # Rolled-back staging consumed the assembled suffix; re-prepare
        # resumes from checkpoints (no rescan) and reassembles it.
        return None
    try:
        src = open(path, "rb")
    except OSError:
        return None
    member = _DeltaMember(
        index, group_index, member_index, path, member_dir, src,
        start_offset, start_lineno, old_keys,
    )
    member.dirty = bool(info["dirty"])
    member.offset = int(info["offset"])
    member.lineno = int(info["lineno"])
    _refresh_member_stats(member)
    return member


# ---------------------------------------------------------------------------
# Reference index over the suffix plus ledger-prefix target keys
# ---------------------------------------------------------------------------


class _Shim:
    def __init__(self, member):
        self.index = member.index
        self.member_dir = member.member_dir
        # Every suffix row belongs to this delta segment (the boundary
        # is the previous, already committed migration), so every edge
        # on it is "fresh" for the segment's reference counter -- even
        # rows a killed attempt scanned, which the recovery must still
        # report to match an uninterrupted run's summary.
        self.fresh_from = 0


def _build_delta_refs(group_dir, members, dst_links, src_links):
    """Resolve references from suffix indexes + ledger-prefix keys."""
    refs_path = os.path.join(group_dir, _REFS_DB)
    _remove(refs_path)
    refs = _RefsDb(refs_path)
    try:
        for member in members:
            rows = [
                (link_id, value)
                for link_id, values in member.old_keys.items()
                for value in values
            ]
            if rows:
                refs._db.executemany(
                    "INSERT INTO node_key(link_id, value, node) "
                    "VALUES (?,?,?)",
                    [(lid, value, _SENTINEL_NODE) for lid, value in rows],
                )
        refs._db.commit()
        for member in members:
            refs.build_member(
                _Shim(member),
                dst_links.get(member.group_index, []),
                src_links.get(member.group_index, []),
            )
        refs.validate()
        return refs
    except BaseException:
        refs.close()
        raise


# ---------------------------------------------------------------------------
# Output construction: reconciled prefix + surviving migrated suffix
# ---------------------------------------------------------------------------


def _copy_range(src_path, out, length):
    remaining = length
    with open(src_path, "rb") as src:
        while remaining:
            chunk = src.read(min(1 << 20, remaining))
            if not chunk:
                break
            out.write(chunk)
            remaining -= len(chunk)


def _build_delta_outputs(member):
    """Write the rename source(s): verbatim prefix + surviving suffix.

    The assembled suffix holds exactly one migrated output line per
    GOOD line-index entry, in order (bad rows are skipped from the
    output), so the filtered copy reads one suffix line for every good
    entry and keeps the non-dropped ones.
    """
    final = os.path.join(member.member_dir, _DELTA_FINAL)
    linked_final = os.path.join(member.member_dir, _DELTA_LINKED_FINAL)
    for name in (final, linked_final):
        if os.path.exists(name):
            os.remove(name)

    suffix = os.path.join(member.member_dir, _DELTA_SUFFIX_FINAL)
    entries = list(_stream_line_entries(member.member_dir))

    if member.dirty:
        target = linked_final if member.dropped else final
        with open(target, "wb") as out:
            _copy_range(member.path, out, member.start_offset)
            with open(suffix, "rb") as fin:
                for lineno0, _off, kind, _proj in entries:
                    if kind == LINE_GOOD:
                        line = fin.readline()
                        if lineno0 not in member.dropped:
                            out.write(line)
            out.flush()
            os.fsync(out.fileno())
    elif member.dropped:
        # Canonical suffix with rows removed: splice source ranges.
        with open(member.path, "rb") as src, open(linked_final, "wb") as out:
            _copy_range(member.path, out, member.start_offset)
            for lineno0, offset, kind, _proj in entries:
                if kind != LINE_GOOD or lineno0 in member.dropped:
                    continue
                src.seek(offset)
                out.write(src.readline())
            out.flush()
            os.fsync(out.fileno())
    _fsync_dir(member.member_dir)


def _audit_dropped_delta(member, audit_stream):
    prefix = _AuditPrefix(audit_stream, member.path)
    with open(member.path, "rb") as src:
        for lineno0, offset, kind, _proj in _stream_line_entries(
                member.member_dir):
            if kind != LINE_GOOD or lineno0 not in member.dropped:
                continue
            src.seek(offset)
            prefix.write(_audit_line(
                member.start_lineno + lineno0 + 1, src.readline()))
    audit_stream.flush()


# ---------------------------------------------------------------------------
# Two-phase rename
# ---------------------------------------------------------------------------


def _stage_delta_member(member):
    path = member.path
    parent = os.path.dirname(os.path.abspath(path))
    d = _delta_debris(path)
    src_name = _DELTA_LINKED_FINAL if member.dropped else _DELTA_FINAL
    os.replace(os.path.join(member.member_dir, src_name), d["staged"])
    _publish_member_marker(path)
    os.replace(path, d["backup"])
    os.replace(d["staged"], path)
    _fsync_dir(parent)


def _undo_staged(members):
    failed = False
    for member in members:
        try:
            _reconcile_delta_member(member.path, committed=False)
        except OSError:
            failed = True
    return failed


# ---------------------------------------------------------------------------
# Ledger boundary / target-key discovery
# ---------------------------------------------------------------------------


def _dst_key_map(group_index, dst_links):
    return {link_id: field for link_id, field
            in dst_links.get(group_index, [])}


def _suffix_target_keys_from_index(member, dst_links):
    """Like :func:`_suffix_target_keys` but from the in-memory drop set.

    Used after the work directory (and its line index) has been swept.
    """
    fields = _dst_key_map(member.group_index, dst_links)
    out = {link_id: set() for link_id in fields}
    if not getattr(member, "_suffix_proj", None):
        return out
    for lineno0, proj in member._suffix_proj:
        if lineno0 in member.dropped:
            continue
        for link_id, field in fields.items():
            value = proj.get(field)
            if value is not None:
                out[link_id].add(value)
    return out


def _capture_suffix_projections(member):
    """Remember surviving rows' string-field projections pre-sweep."""
    member._suffix_proj = [
        (lineno0, proj)
        for lineno0, _off, kind, proj
        in _stream_line_entries(member.member_dir)
        if kind == LINE_GOOD
    ]


def _canonical_tail(path, from_off, quiesce, link_fields):
    """Scan an already-canonical tail.

    *link_fields* is a list of ``(link_id, field)``.  Returns
    ``(boundary, lines, per_link_values)``: the line-aligned offset up
    to which the live file holds settled current-version canonical
    bytes, the number of complete canonical lines covered, and the
    named string values seen, attributed per link.  Stops at a torn or
    non-canonical line (left for the next incremental run).
    """
    boundary = from_off
    lines = 0
    values = {link_id: set() for link_id, _field in link_fields}
    follow = max(1.0, quiesce * 20)
    with open(path, "rb") as f:
        for raw in _read_lines(f, quiesce, offset=from_off, follow=follow):
            try:
                if raw != _convert(raw):
                    break
                obj = loads(raw)
            except ValueError:
                break
            boundary += len(raw)
            lines += 1
            for link_id, field in link_fields:
                value = obj.get(field)
                if isinstance(value, str):
                    values[link_id].add(value)
    return boundary, lines, values


def _merge_keys(old_keys, suffix_keys):
    merged = {lid: set(vals) for lid, vals in old_keys.items()}
    for lid, vals in suffix_keys.items():
        merged.setdefault(lid, set()).update(vals)
    return merged


# ---------------------------------------------------------------------------
# Post-border finishing
# ---------------------------------------------------------------------------


def _converge_and_finalize(staged_members, paths, group_dir, on_bad,
                           quiesce, audit_stream, all_members=(),
                           references_bad=0, fallback=False,
                           fallback_reason=None):
    warnings = []
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
            warnings.append(f"{member.path}: post-commit convergence "
                            f"incomplete, rerun to finish: {exc}")
            continue
        member.salvaged = salvaged
        member.conv_skipped = conv_skipped
        member.lineno = lineno
        if warning:
            warnings.append(f"{member.path}: {warning}")
        try:
            _remove(member.path + _BACKUP_SUFFIX)
            _fsync_dir(os.path.dirname(os.path.abspath(member.path)))
        except OSError as exc:
            warnings.append(f"{member.path}: backup removal failed: {exc}")

    _crash_point("delta-backups")
    # Refresh the durable segment summary with the rows convergence
    # moved, so a kill after convergence still replays the full
    # uninterrupted counters (not only the pre-drain ones).
    _write_segment_summary(
        group_dir, all_members, references_bad,
        fallback=fallback, fallback_reason=fallback_reason,
        converged=True)
    # The work directory (commit marker + durable segment summary) is
    # intentionally NOT swept here: ledgers must be published first, so a
    # kill between convergence and ledger publication still reaches the
    # committed-recovery path and replays the segment summary.  The
    # caller sweeps after the ledgers are durable.
    return warnings


def _sweep_after_ledgers(paths, group_dir, warnings):
    try:
        _crash_point("delta-cleanup")
        if os.environ.get(_FAULT_ENV) == "delta-cleanup":
            raise OSError("injected delta cleanup fault")
        _sweep_delta(paths, group_dir)
    except OSError as exc:
        warnings.append(f"cleanup failed: {exc}")


def _finish_committed_delta(paths, group_dir, groups, links, on_bad,
                            segment_size, quiesce, audit_stream, dst_links):
    """Finish a delta run whose commit marker already exists (a rerun).

    The border was crossed, so every fault is a warning.  A surviving
    backup is converged before it is removed; the live inode then gets
    the same idempotent catch-up a full linked rerun performs.  Ledgers
    are rebuilt from the settled live files (local-only, rebuildable).
    Returns ``(per_member_added, per_member_skipped, warnings)`` --
    counters cover only rows this invocation newly moves.
    """
    from .group_migration import _prepare_member

    warnings = []
    added = [0] * len(paths)
    skipped = [0] * len(paths)
    flat = [p for g in groups for p in g]

    for index, path in enumerate(paths):
        parent = os.path.dirname(os.path.abspath(path))
        d = _delta_debris(path)
        member_dir = _delta_member_dir(path)
        try:
            if os.path.exists(d["backup"]):
                info = None
                try:
                    with open(os.path.join(member_dir, _DELTA_PREPARED),
                              "rb") as f:
                        info = json.loads(f.read())
                except (OSError, ValueError):
                    info = None
                if info is None:
                    warnings.append(
                        f"{path}: prepared state lost; cannot converge the "
                        f"backup, dropping it")
                    _remove(d["backup"])
                    _fsync_dir(parent)
                else:
                    salv, skip, _gens, complete = _salvage_backup_tail(
                        path, d["backup"], int(info["offset"]),
                        int(info["lineno"]), on_bad,
                        _AuditPrefix(audit_stream, path), member_dir,
                        quiesce,
                    )
                    added[index] += salv
                    skipped[index] += skip
                    if complete:
                        _remove(d["backup"])
                        _fsync_dir(parent)
                    else:
                        warnings.append(
                            f"{path}: backup still receiving appends at the "
                            f"backstop; rerun to finish")
                        continue

            shutil.rmtree(member_dir, ignore_errors=True)
            member = _prepare_member(
                path, member_dir, on_bad, segment_size, quiesce,
                _AuditPrefix(audit_stream, path),
            )
            try:
                if member.dirty:
                    os.replace(
                        os.path.join(member_dir, "final"),
                        path + _STAGED_SUFFIX)
                    _publish_member_marker(path)
                    os.replace(path, path + _BACKUP_SUFFIX)
                    os.replace(path + _STAGED_SUFFIX, path)
                    _fsync_dir(parent)
                    salvaged, conv_skipped, _lineno, warning = _converge(
                        path, parent, member_dir, member.src,
                        member.offset, member.lineno, quiesce,
                        _AuditPrefix(audit_stream, path), on_bad,
                    )
                    added[index] += salvaged + member.run_migrated
                    skipped[index] += conv_skipped + member.run_skipped
                    if warning:
                        warnings.append(f"{path}: {warning}")
                    _remove(path + _BACKUP_SUFFIX)
                    _fsync_dir(parent)
            finally:
                try:
                    member.src.close()
                except OSError:
                    pass
        except (OSError, ValueError) as exc:
            warnings.append(
                f"{path}: post-commit finishing incomplete, rerun to "
                f"finish: {exc}")
            shutil.rmtree(member_dir, ignore_errors=True)

    ledgers = _rebuild_ledgers_from_live(paths, groups, quiesce, dst_links,
                                         warnings)
    # The work directory is swept by the caller AFTER the rebuilt
    # ledgers are published, so a kill in between still finds the commit
    # marker and replays the segment summary.
    return added, skipped, ledgers, warnings


def _rebuild_ledgers_from_live(paths, groups, quiesce, dst_links, warnings):
    """Reconstruct every ledger by scanning the settled live files."""
    ledgers = {}
    for index, path in enumerate(paths):
        gi = _group_of(groups, path)
        link_fields = dst_links.get(gi, [])
        try:
            boundary, lines, values = _canonical_tail(
                path, 0, quiesce, link_fields)
            st = os.stat(path)
            ledgers[index] = {
                "inode": st.st_ino, "offset": boundary, "lineno": lines,
                "keys": {link_id: set(vals)
                         for link_id, vals in values.items()},
            }
        except OSError as exc:
            warnings.append(f"{path}: ledger rebuild failed: {exc}")
    return ledgers


def _group_of(groups, path):
    for gi, group in enumerate(groups):
        if path in group:
            return gi
    raise ValueError("path not in groups")


def _publish_ledgers(paths, groups, links, on_bad, ledgers):
    for index, path in enumerate(paths):
        entry = ledgers.get(index)
        if entry is None:
            continue
        _write_ledger(
            path, groups, links, on_bad, entry["inode"], entry["offset"],
            entry["lineno"], entry["keys"],
        )


# ---------------------------------------------------------------------------
# Strict-mode global first error
# ---------------------------------------------------------------------------


def _raise_delta_first_error(members, links, first):
    bad_line = None
    for member in members:
        for lineno0, offset, kind, _proj in _stream_line_entries(
                member.member_dir):
            if kind != LINE_GOOD:
                bad_line = (member, lineno0, offset)
                break
        if bad_line is not None:
            break
    if bad_line is not None and (
        first is None
        or (bad_line[0].index, bad_line[1]) < (first[0], first[1])
    ):
        member, lineno0, offset = bad_line
        with open(member.path, "rb") as f:
            f.seek(offset)
            raw = f.readline()
        try:
            _convert(raw)
            cause = ValueError("undecodable record")
        except ValueError as exc:
            cause = exc
        raise GroupBadRecordError(
            member.path, member.start_lineno + lineno0 + 1, raw, cause)
    if first is not None:
        mi, lineno0, reason, link_id, value = first
        member = members[mi]
        raw = b""
        for n, offset, _kind, _proj in _stream_line_entries(member.member_dir):
            if n == lineno0:
                with open(member.path, "rb") as f:
                    f.seek(offset)
                    raw = f.readline()
                break
        _src_g, dst_g, _src_field, dst_field = links[link_id]
        raise LinkedBadReferenceError(
            member.path, member.start_lineno + lineno0 + 1, raw,
            f"{_REASON_TEXT[reason]}: {dst_field}={value!r} "
            f"in group {dst_g}",
        )


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


def migrate_linked_delta(groups, *, links=(), on_bad="strict",
                         segment_size=DEFAULT_SEGMENT_SIZE,
                         quiesce=DEFAULT_QUIESCE, audit=None):
    """Migrate only the rows appended since the previous migration.

    The arguments and input shape are exactly
    :func:`migrate_linked_logs`'s.  Rows covered by a member's
    reconciliation ledger are neither rescanned nor rewritten; only the
    appended suffix is prepared, resolved and committed.  A missing,
    corrupt, configuration-stale or inode-stale ledger deterministically
    processes every member from offset zero (a full migration), with the
    reason on the result.

    Returns a :class:`DeltaMigrationResult` with four per-member counts.
    Leases, appender, crash-resume and strict-ordering semantics match
    the full entry.  Raises :class:`MigrationLockedError` while a needed
    lease is held, :class:`TypeError` / :class:`ValueError` for a
    malformed group/link list (and the strict first-error ValueErrors),
    and :class:`FileNotFoundError` for a missing or unreadable member.
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
    group_dir = _delta_group_dir(groups)

    dst_links = {}
    src_links = {}
    for link_id, (src_g, dst_g, src_field, dst_field) in enumerate(links):
        dst_links.setdefault(dst_g, []).append((link_id, dst_field))
        src_links.setdefault(src_g, []).append((link_id, src_field))

    members = []
    replaced = False
    references_bad = 0
    ledgers = {}

    with _member_leases(paths):
        try:
            _crash_point("delta-lock")
            os.makedirs(group_dir, exist_ok=True)
            committed = os.path.exists(
                os.path.join(group_dir, _DELTA_COMMITTED))

            manifest = _read_delta_manifest(group_dir)
            abs_groups = [[os.path.abspath(p) for p in g] for g in groups]
            links_list = [list(l) for l in links]
            if manifest is not None and (
                manifest.get("groups") != abs_groups
                or manifest.get("links") != links_list
                or manifest.get("on_bad") != on_bad
            ):
                if committed:
                    raise ValueError(
                        "delta commit marker exists for a different group "
                        "list, link set or policy; refusing to mix runs")
                old_paths = [
                    p for g in manifest.get("groups", []) for p in g
                    if os.path.exists(p)
                    or os.path.exists(p + _BACKUP_SUFFIX)
                    or os.path.exists(p + _STAGED_SUFFIX)
                ]
                for old_path in old_paths:
                    _reconcile_delta_member(old_path, committed=False)
                _sweep_delta(old_paths, group_dir)
                manifest = None
            if manifest is None:
                _write_delta_manifest(group_dir, groups, links, on_bad)
            _remove(os.path.join(group_dir, _DELTA_COMMITTED_TMP))

            # Resolve interrupted renames BEFORE reading ledgers: until
            # reconciliation the live path may still name the pre-rename
            # (ledger-current) inode while staging is half done.
            for path in paths:
                _reconcile_delta_member(path, committed)

            if committed:
                summary = _read_segment_summary(group_dir)
                # Once the ledgers were durably acknowledged this
                # segment is settled: a recovery rerun counts only rows
                # it newly converges, not the already-reported segment.
                if _segment_acknowledged(group_dir):
                    summary = None
                added, skipped, rebuilt, warnings = (
                    _finish_committed_delta(
                        paths, group_dir, groups, links, on_bad,
                        segment_size, quiesce, audit_stream, dst_links))
                _publish_ledgers(paths, groups, links, on_bad, rebuilt)
                _write_segment_ack(group_dir)
                # Ledgers are durable before the marker/work state is
                # removed: a kill here reruns committed once more and
                # replays the same summary; after the sweep it resumes
                # incrementally from the new ledgers.
                try:
                    _sweep_delta(paths, group_dir)
                except OSError as exc:
                    warnings.append(f"cleanup failed: {exc}")
                return _committed_result(
                    groups, paths, summary, added, skipped, warnings)

            # --- Uncommitted path: read ledgers against the original
            # inodes the reconciliation restored/kept in place.
            starts, fallback, fallback_reason = _load_ledgers(
                groups, links, on_bad)

            # --- PREPARE each member's suffix (durable, resumable).
            try:
                index = 0
                for group_index, group in enumerate(groups):
                    for member_index, path in enumerate(group):
                        member_dir = _delta_member_dir(path)
                        start = starts[index]
                        member = _resume_delta_prepared(
                            index, group_index, member_index, path,
                            member_dir, on_bad, start)
                        if member is None:
                            member = _prepare_delta_member(
                                index, group_index, member_index, path,
                                member_dir, on_bad, segment_size, quiesce,
                                audit_stream, *start)
                        members.append(member)
                        index += 1
                _crash_point("delta-prepare")

                # --- RESOLVE references (suffix + ledger prefix keys).
                refs = _build_delta_refs(
                    group_dir, members, dst_links, src_links)
                try:
                    dropped = refs.dropped_lines()
                    first = refs.first_bad()
                    references_bad = refs.fresh_bad_edges()
                finally:
                    refs.close()
                _crash_point("delta-refs")

                if on_bad == "strict":
                    _raise_delta_first_error(members, links, first)

                for member in members:
                    member.dropped = dropped.get(member.index, set())
                    if member.dropped:
                        if on_bad == "skip":
                            _audit_dropped_delta(member, audit_stream)
                    # Remember suffix projections before the work
                    # directory is swept, so ledger target keys can be
                    # computed post-convergence.
                    _capture_suffix_projections(member)
                _crash_point("delta-filter")

                staged_members = [
                    m for m in members if m.dirty or m.dropped]
                clean_members = [
                    m for m in members if not m.dirty and not m.dropped]
                for member in staged_members:
                    _build_delta_outputs(member)
                    member.will_stage = True
                    member.staged_size = os.path.getsize(
                        os.path.join(
                            member.member_dir,
                            _DELTA_LINKED_FINAL if member.dropped
                            else _DELTA_FINAL))

                # Durably record this segment's resolved summary before
                # any rename: a recovery that finds the commit marker
                # replays exactly these segment counters even though it
                # itself rescans nothing.
                _write_segment_summary(
                    group_dir, members, references_bad,
                    fallback=fallback, fallback_reason=fallback_reason)

                if not staged_members:
                    # Nothing to rename; advance the ledgers over the
                    # canonical suffix so the boundary moves forward.
                    for member in members:
                        _ledger_for_clean(member, groups, links, on_bad,
                                          dst_links, ledgers)
                    _publish_ledgers(paths, groups, links, on_bad, ledgers)
                    _sweep_delta(paths, group_dir)
                    return _build_result(
                        groups, members, fallback, fallback_reason,
                        references_bad, replaced=False, warnings=[])

                # --- PHASE 1: stage every member.
                staged = []
                try:
                    for member in staged_members:
                        _stage_delta_member(member)
                        staged.append(member)
                        _crash_point("delta-stage")
                    _crash_point("delta-staged")
                except BaseException:
                    if not _undo_staged(staged):
                        _sweep_delta(paths, group_dir)
                    raise

                # --- COMMIT border.
                marker = os.path.join(group_dir, _DELTA_COMMITTED)
                tmp = os.path.join(group_dir, _DELTA_COMMITTED_TMP)
                with open(tmp, "wb") as f:
                    f.write(b"1\n")
                    f.flush()
                    os.fsync(f.fileno())
                os.replace(tmp, marker)
                _fsync_dir(group_dir)
                _crash_point("delta-marker")
                replaced = True

                warnings = _converge_and_finalize(
                    staged_members, paths, group_dir, on_bad, quiesce,
                    audit_stream, all_members=members,
                    references_bad=references_bad,
                    fallback=fallback, fallback_reason=fallback_reason)

                # --- Ledgers: pin the post-finish live inodes.  These
                # are published BEFORE the work directory (with its
                # commit marker and segment summary) is swept, so a kill
                # anywhere in this tail is recovered deterministically:
                # before the ledgers land, a rerun finds the marker and
                # replays the durable summary; afterwards the ledgers
                # alone describe the new boundary.
                for member in clean_members:
                    _ledger_for_clean(member, groups, links, on_bad,
                                      dst_links, ledgers)
                for member in staged_members:
                    _ledger_for_staged(member, groups, links, on_bad,
                                       quiesce, dst_links, ledgers)
                _crash_point("delta-ledger")
                _publish_ledgers(paths, groups, links, on_bad, ledgers)
                _write_segment_ack(group_dir)

                _sweep_after_ledgers(paths, group_dir, warnings)
                post_commit_error = "; ".join(warnings) or None
            except BaseException:
                if not os.path.exists(
                        os.path.join(group_dir, _DELTA_COMMITTED)) \
                        and not any(os.path.exists(m.path + _BACKUP_SUFFIX)
                                    for m in members):
                    _sweep_delta(paths, group_dir)
                raise
        finally:
            for member in members:
                try:
                    member.src.close()
                except (OSError, AttributeError):
                    pass

    return _build_result(
        groups, members, fallback, fallback_reason, references_bad,
        replaced=replaced,
        warnings=[post_commit_error] if post_commit_error else [])


def _ledger_for_clean(member, groups, links, on_bad, dst_links, ledgers):
    # No rename: boundary is the drain end on the same live inode.
    suffix_keys = _suffix_target_keys_from_index(member, dst_links)
    st = os.stat(member.path)
    ledgers[member.index] = {
        "inode": st.st_ino,
        "offset": member.offset,
        "lineno": member.lineno,
        "keys": _merge_keys(member.old_keys, suffix_keys),
    }


def _ledger_for_staged(member, groups, links, on_bad, quiesce, dst_links,
                       ledgers):
    # Past the staged bytes, convergence may have folded in a canonical
    # tail (whole-record straddle writes); extend the boundary and the
    # target keys over exactly that settled tail.
    link_fields = dst_links.get(member.group_index, [])
    boundary, lines, tail_keys = _canonical_tail(
        member.path, member.staged_size, quiesce, link_fields)
    suffix_keys = _suffix_target_keys_from_index(member, dst_links)
    surviving_suffix_lines = member.suffix_good - len(member.dropped)
    st = os.stat(member.path)
    ledgers[member.index] = {
        "inode": st.st_ino,
        "offset": boundary,
        "lineno": member.start_lineno + surviving_suffix_lines + lines,
        "keys": _merge_keys(member.old_keys,
                            _merge_keys(suffix_keys, tail_keys)),
    }


# ---------------------------------------------------------------------------
# Results
# ---------------------------------------------------------------------------


def _build_result(groups, members, fallback, fallback_reason,
                  references_bad, *, replaced, warnings):
    by_path = {m.path: m for m in members}
    member_results = []
    for gi, group in enumerate(groups):
        for mi, path in enumerate(group):
            m = by_path.get(path)
            if m is None:
                member_results.append(DeltaMemberResult(
                    path, gi, mi, 0, 0, 0, 0, False))
            else:
                member_results.append(DeltaMemberResult(
                    path=path, group=gi, member=mi,
                    lines_added=m.lines_added,
                    records_rewritten=m.records_rewritten,
                    records_skipped=m.records_skipped,
                    records_dropped=m.records_dropped,
                    replaced=replaced and m.will_stage,
                ))
    return DeltaMigrationResult(
        groups=tuple(tuple(g) for g in groups),
        records_added=sum(r.lines_added for r in member_results),
        records_rewritten=sum(r.records_rewritten for r in member_results),
        records_skipped=sum(r.records_skipped for r in member_results),
        records_dropped=sum(r.records_dropped for r in member_results),
        references_bad=references_bad,
        replaced=replaced,
        members=tuple(member_results),
        fallback=bool(fallback),
        fallback_reason=fallback_reason,
        post_commit_error="; ".join(w for w in warnings if w) or None,
    )


def _committed_result(groups, paths, summary, added, skipped, warnings):
    """Recovery past the commit marker: replay the durable segment
    summary plus rows this recovery newly converges."""
    seg = {}
    references_bad = 0
    fallback = False
    fallback_reason = None
    if summary is not None:
        references_bad = int(summary.get("references_bad", 0))
        fallback = bool(summary.get("fallback"))
        fallback_reason = summary.get("fallback_reason")
        for entry in summary.get("members", []):
            seg[entry["path"]] = entry

    member_results = []
    any_replaced = False
    for gi, group in enumerate(groups):
        for mi, path in enumerate(group):
            index = paths.index(path)
            entry = seg.get(path)
            if entry is None:
                good = bad = rewritten = dropped = staged = 0
            else:
                good = int(entry["good"])
                bad = int(entry["bad"])
                rewritten = int(entry["rewritten"])
                dropped = int(entry["dropped"])
                staged = bool(entry.get("will_stage"))
            salv = added[index]
            conv_skip = skipped[index]
            did_replace = staged or salv > 0 or conv_skip > 0
            any_replaced = any_replaced or did_replace
            member_results.append(DeltaMemberResult(
                path=path, group=gi, member=mi,
                lines_added=good + bad + salv + conv_skip,
                records_rewritten=rewritten + salv,
                records_skipped=bad + conv_skip,
                records_dropped=dropped,
                replaced=did_replace,
            ))
    return DeltaMigrationResult(
        groups=tuple(tuple(g) for g in groups),
        records_added=sum(r.lines_added for r in member_results),
        records_rewritten=sum(r.records_rewritten for r in member_results),
        records_skipped=sum(r.records_skipped for r in member_results),
        records_dropped=sum(r.records_dropped for r in member_results),
        references_bad=references_bad,
        replaced=any_replaced,
        members=tuple(member_results),
        fallback=fallback,
        fallback_reason=fallback_reason,
        post_commit_error="; ".join(w for w in warnings if w) or None,
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def run_delta_cli(argv):
    parser = argparse.ArgumentParser(
        prog="python3 -m proto_migrate delta-migrate-linked-logs",
        description="Incremental linked-group migration: migrate only "
        "the rows appended since the previous migration, reconciled via "
        "a local per-member ledger (deterministically falls back to a "
        "full migration when the ledger is missing, corrupt, stale or "
        "inode-changed).",
    )
    parser.add_argument("--group", action="append", nargs="+",
                        required=True, metavar="FILE",
                        help="one log group (repeat per group, in "
                        "group order)")
    parser.add_argument("--link", action="append", default=[],
                        metavar="SRC:DST:SRC_FIELD:DST_FIELD",
                        help="declare a reference (repeatable)")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--strict", action="store_const", const="strict",
                      dest="on_bad", help="abort on the first bad line "
                      "or bad reference (default; exit %d, files "
                      "untouched)" % EXIT_BAD_RECORD)
    mode.add_argument("--skip", action="store_const", const="skip",
                      dest="on_bad", help="skip bad lines and drop "
                      "bad-reference records, auditing each to stderr")
    parser.set_defaults(on_bad="strict")
    parser.add_argument("--segment-size", type=int,
                        default=DEFAULT_SEGMENT_SIZE, metavar="BYTES")
    parser.add_argument("--quiesce-ms", type=float,
                        default=DEFAULT_QUIESCE * 1000, metavar="MS")
    args = parser.parse_args(argv)

    links = []
    for spec in args.link:
        parts = spec.split(":")
        if len(parts) != 4:
            print(f"error: malformed --link: {spec!r}", file=sys.stderr)
            return EXIT_USAGE
        try:
            src_g = int(parts[0]); dst_g = int(parts[1])
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
        result = migrate_linked_delta(
            args.group, links=links, on_bad=args.on_bad,
            segment_size=args.segment_size,
            quiesce=args.quiesce_ms / 1000.0,
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
        "groups=%d members=%d added=%d rewritten=%d skipped=%d "
        "dropped=%d refs_bad=%d replaced=%s fallback=%s"
        % (
            len(result.groups),
            sum(len(g) for g in result.groups),
            result.records_added,
            result.records_rewritten,
            result.records_skipped,
            result.records_dropped,
            result.references_bad,
            "yes" if result.replaced else "no",
            "yes" if result.fallback else "no",
        )
    )
    for m in result.members:
        print(
            "member group=%d index=%d path=%s added=%d rewritten=%d "
            "skipped=%d dropped=%d %s"
            % (m.group, m.member, m.path, m.lines_added,
               m.records_rewritten, m.records_skipped, m.records_dropped,
               "replaced" if m.replaced else "untouched")
        )
    if result.fallback_reason:
        print(f"fallback-reason: {result.fallback_reason}",
              file=sys.stderr)
    if result.post_commit_error:
        print(f"warning: {result.post_commit_error}", file=sys.stderr)
    return EXIT_OK
