"""Cross-group, reference-aware, all-or-nothing log migration.

This is the multi-group companion of :mod:`proto_migrate.group_migration`.
Where :func:`~proto_migrate.migrate_log_group` migrates one *group* of
files with no knowledge of their contents, this module migrates several
groups at once and keeps the references *between* their records intact.

The caller gives the groups (each an explicit member list; group indices
are zero based in list order) and a list of reference declarations
``(source_group_index, target_group_index)``.  Records carry no special
reference field: for every declared edge *src -> dst*, **every** record
of the source group points at the first record of the target group with
the same ``order_id`` in member-list order and then line order; when
src and dst are the same group that can be the source line itself, a
length-one self loop.

After a successful run every surviving reference still names a
complete, surviving target record; if that cannot be guaranteed the
whole set of groups stays exactly as it was -- no partially migrated
group is ever left in durable state after a handled failure.

Bad references come in four kinds:

  * **missing** -- the target group holds no other record with that
    order id;
  * **target-skipped** -- the earliest matching target line is a bad
    line the run skips, or a good record itself dropped for a bad
    reference;
  * **illegal-target-version** -- the earliest matching target line
    parses as JSON but carries a missing/non-integer/unsupported
    version key;
  * **cycle** -- the reference edge lies on a chain whose head reaches
    back to its own source line (a self loop included).

Strict mode stops at the globally first bad line *or* bad reference
(groups in list order, members within a group in list order, then
lines in file order; edges of one line in declaration order) and raises
:class:`LinkedBadReferenceError` or
:class:`~proto_migrate.group_migration.GroupBadRecordError` (both
:class:`ValueError`) before any original is touched.  Skip mode skips
the offending record and audits
``<filename>:<lineno>:<first 32 raw bytes>`` to stderr.  Bad lines and
bad references are counted separately; every counter counts only
records/references this invocation newly processes -- a resuming,
finishing or idempotent rerun reports zero for work already done.

Streaming
---------
No whole group is ever loaded to resolve references.  Each member is
tailed and migrated exactly like a group migration: size-bounded
fsynced output segments plus a durable local checkpoint, and an
assembled ``final`` extended by one bounded append-tail drain.  Beside
the segments the scan appends one small structured index row per
record (own key, good/bad/illegal-v state, changed flag) to a spill
file in the member work directory; the spill is checkpointed together
with the output prefix and the drain appends to both the final and the
spill past the last checkpoint, so a killed drain is rolled back and
redone exactly like the assembled final.

Once every member is prepared, the per-member spills are bulk-loaded
into a temporary on-disk SQLite database (pure derived state, rebuilt
from scratch every run and trivially reconstructable after a crash)
where earliest-target resolution, target existence, the skip-mode drop
closure and cycle elimination run without holding the reference graph
in memory.

Commit and crash recovery mirror the group protocol with linked
debris names: per-member ``final -> staged``, per-file commit marker,
``path -> backup``, ``staged -> path``; only after every rename
landed is the single ``linked-committed`` border marker published.
A kill before the border rolls every rename back and resumes members
from their local checkpoints (prepared members are neither rescanned
nor rewritten); a kill after it completes the renames forward,
converges appends still landing on the backup inode *before* deleting
it, and only reports warnings.  A handled pre-border failure sweeps
all work and leaves every original byte-for-byte untouched.
"""

from __future__ import annotations

import argparse
import fcntl
import json
import os
import shutil
import sqlite3
import sys
from collections.abc import Sequence
from typing import NamedTuple

from . import CURRENT_VERSION, VERSIONS, loads, migrate
from .group_migration import (
    GroupBadRecordError,
    MigrationResult,
    _AuditPrefix,
    _fsync_dir,
    _remove,
    _wait_path,
)
from .log_migration import (
    DEFAULT_QUIESCE,
    DEFAULT_SEGMENT_SIZE,
    EXIT_BAD_RECORD,
    EXIT_ERROR,
    EXIT_OK,
    EXIT_USAGE,
    BadRecordError,
    _assemble,
    _audit_line,
    _commit_marker_path,
    _convert,
    _converge,
    _crash_point,
    _emit,
    _prepare_workdir,
    _publish_prefix_checkpoint,
    _read_checkpoint_log,
    _read_lines,
)

__all__ = [
    "LinkedBadReferenceError",
    "LinkedMigrationResult",
    "migrate_linked_groups",
    "read_linked_logs",
    "run_linked_cli",
]

_LINKED_DIR_NAME = ".migrate-linked-tmp"
_LINKED_LOCK_NAME = ".migrate-linked.lock"
_LINKED_COMMITTED = "linked-committed"
_LINKED_COMMITTED_TMP = "linked-committed.tmp"
_MANIFEST = "manifest.json"
_PREPARED = "prepared"
_FINAL = "final"
_INDEX = "index.ndjson"
_INDEX_CP = "index-checkpoint"
_DROP = "drop.lines"
_REFS_DB = "refs.sqlite"

_MEMBER_TMP_SUFFIX = ".migrate-linked-tmp"
_BACKUP_SUFFIX = ".migrate-linked-backup"
_STAGED_SUFFIX = ".migrate-linked-staged"

_FAULT_ENV = "PROTO_MIGRATE_FAULT_AT"


# ---------------------------------------------------------------------------
# Errors and results
# ---------------------------------------------------------------------------


class LinkedBadReferenceError(BadRecordError):
    """Strict-mode bad reference, annotated with source and kind."""

    def __init__(self, path, lineno, kind, target):
        self.path = path
        self.kind = kind
        self.target = target
        cause = f"bad reference ({kind}: {target!r})"
        super().__init__(lineno, b"", cause)

    def __str__(self):
        return f"{self.path}: line {self.lineno}: {self.cause}"


class LinkedMigrationResult(NamedTuple):
    groups: tuple
    refs: tuple
    paths: tuple
    records_migrated: int
    records_skipped: int
    references_bad: int
    records_salvaged: int
    replaced: bool
    members: tuple
    post_commit_error: str | None = None


# ---------------------------------------------------------------------------
# Record coding (the plain codec; references live in the declarations)
# ---------------------------------------------------------------------------


def _reject_constant(value):
    raise ValueError(f"non-JSON constant: {value}")


def _parse_json(raw):
    return json.loads(raw.decode("utf-8"), parse_constant=_reject_constant)


def _decode_line(raw):
    """Decode one raw line; never raise for a malformed line.

    Returns ``(out, key, bad, vbad, changed)``.  *vbad* marks a body
    that parses as a JSON object whose version key itself is
    missing/non-integer/unsupported (the illegal-target-version
    reference kind); its extractable ``order_id`` is still returned so
    it can (fail to) serve as a reference target.
    """
    try:
        obj = _parse_json(raw)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None, None, True, False, False
    try:
        rec = loads(raw)
    except ValueError:
        key = obj.get("order_id") if isinstance(obj, dict) else None
        if not isinstance(key, str):
            key = None
        v = obj.get("v") if isinstance(obj, dict) else None
        vbad = (
            isinstance(obj, dict)
            and (not isinstance(v, int) or isinstance(v, bool)
                 or v not in VERSIONS))
        return None, key, True, vbad, False
    out = _convert(raw)
    return out, rec["order_id"], False, False, raw != out


def _strict_cause(raw):
    try:
        loads(raw)
    except ValueError as exc:
        return exc
    return ValueError("bad record")


def _linked_emit(path):
    """``(audit, on_bad, raw, lineno) -> (out, bad)`` converter.

    Used by the shared convergence routine, which only needs good
    bytes / skip audits / the strict GroupBadRecordError shape.
    """

    def emit(audit_stream, on_bad, raw, lineno):
        out, _k, bad, _vb, _ch = _decode_line(raw)
        if bad:
            if on_bad == "strict":
                raise GroupBadRecordError(
                    path, lineno, raw, _strict_cause(raw))
            audit_stream.write(_audit_line(lineno, raw))
            audit_stream.flush()
            return None, True
        return out, False

    return emit


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def _normalize_one_path(item):
    path = os.fspath(item)
    if isinstance(path, bytes):
        path = os.fsdecode(path)
    return path


def _normalize_groups(groups):
    """Validate the nested group list; return groups as given (str)."""
    if isinstance(groups, (str, bytes, os.PathLike)) \
            or not isinstance(groups, Sequence):
        raise TypeError("groups must be a sequence of path sequences")
    normalized = []
    seen = set()
    for group in groups:
        if isinstance(group, (str, bytes, os.PathLike)) \
                or not isinstance(group, Sequence):
            raise ValueError("each group must be a sequence of members")
        members = []
        for item in group:
            path = _normalize_one_path(item)
            absolute = os.path.abspath(path)
            if absolute in seen:
                raise ValueError(f"duplicate linked member: {path!r}")
            seen.add(absolute)
            members.append(path)
        if not members:
            raise ValueError("linked group member list is empty")
        normalized.append(members)
    if not normalized:
        raise ValueError("linked group list is empty")
    return normalized


def _is_plain_int(value):
    return isinstance(value, int) and not isinstance(value, bool)


def _normalize_refs(refs, group_count):
    """Validate reference declarations ``(src_group, dst_group)``.

    A non-sequence (including a single pair tuple) is a TypeError for
    the list itself; a malformed pair, an out-of-range index or a
    repeated declaration is a ValueError.
    """
    if isinstance(refs, (str, bytes)) or not isinstance(refs, Sequence):
        raise TypeError("refs must be a sequence of (src, dst) pairs")
    out = []
    seen = set()
    for ref in refs:
        if isinstance(ref, (str, bytes)) or not isinstance(ref, Sequence) \
                or len(ref) != 2:
            raise ValueError(
                "each reference must be a (source, target) index pair")
        src, dst = ref
        if not (_is_plain_int(src) and _is_plain_int(dst)):
            raise ValueError("reference group indices must be integers")
        if not (0 <= src < group_count and 0 <= dst < group_count):
            raise ValueError(
                f"reference index out of range: ({src}, {dst})")
        if (src, dst) in seen:
            raise ValueError(f"duplicate reference: ({src}, {dst})")
        seen.add((src, dst))
        out.append((src, dst))
    return out


def _flatten(groups):
    return [path for group in groups for path in group]


def _probe_members(paths):
    for path in paths:
        try:
            with open(path, "rb"):
                pass
        except OSError as exc:
            raise FileNotFoundError(
                f"linked member missing or unreadable: {path!r}: {exc}"
            ) from exc


# ---------------------------------------------------------------------------
# Work-directory layout
# ---------------------------------------------------------------------------


def _group_dir_for(paths):
    first_dir = os.path.dirname(os.path.abspath(paths[0]))
    return os.path.join(first_dir, _LINKED_DIR_NAME)


def _member_workdir(path):
    return path + _MEMBER_TMP_SUFFIX


def _debris(path):
    marker = _commit_marker_path(path)
    return {
        "staged": path + _STAGED_SUFFIX,
        "backup": path + _BACKUP_SUFFIX,
        "marker": marker,
        "marker_tmp": marker + ".tmp",
    }


def _sweep_dirs(paths, group_dir):
    for path in paths:
        shutil.rmtree(_member_workdir(path), ignore_errors=True)
    shutil.rmtree(group_dir, ignore_errors=True)


def _read_manifest(group_dir):
    try:
        with open(os.path.join(group_dir, _MANIFEST), "rb") as f:
            return json.loads(f.read())
    except (OSError, ValueError):
        return None


def _write_manifest(group_dir, groups, paths, refs, on_bad):
    data = {
        "version": 1,
        "on_bad": on_bad,
        "groups": [[os.path.abspath(p) for p in g] for g in groups],
        "members": [os.path.abspath(p) for p in paths],
        "refs": [[s, d] for s, d in refs],
    }
    tmp = os.path.join(group_dir, _MANIFEST + ".tmp")
    with open(tmp, "wb") as f:
        f.write(json.dumps(data).encode("ascii"))
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, os.path.join(group_dir, _MANIFEST))
    _fsync_dir(group_dir)


def _write_prepared(member_dir, info):
    path = os.path.join(member_dir, _PREPARED)
    with open(path + ".tmp", "wb") as f:
        f.write(json.dumps(info).encode("ascii"))
        f.flush()
        os.fsync(f.fileno())
    os.replace(path + ".tmp", path)
    _fsync_dir(member_dir)


def _read_prepared(member_dir):
    try:
        with open(os.path.join(member_dir, _PREPARED), "rb") as f:
            data = json.loads(f.read())
        for key in ("dirty", "offset", "lineno", "final_size",
                    "complete"):
            if key not in data:
                return None
        return data
    except (OSError, ValueError):
        return None


# ---------------------------------------------------------------------------
# Index spill + coupled checkpoint
# ---------------------------------------------------------------------------
#
# One JSON row per record the prepare phase emits, in final order:
#   {"l": lineno, "ok": 1|0, "k": own key|None, "vb": 1|0, "ch": 1|0}
#
# Scan rows are covered by the coupled prefix checkpoints; drain rows
# are appended past the last boundary and are discarded on recovery
# (the assembled final is rebuilt and the drain replayed, exactly like
# the group protocol's uncheckpointed drain).


def _index_row(lineno, dec):
    out, key, bad, vbad, changed = dec
    return {
        "l": lineno, "ok": 0 if bad else 1, "k": key,
        "vb": 1 if vbad else 0, "ch": 1 if changed else 0,
    }


def _read_index_checkpoint(member_dir):
    """Newest valid ``(input-offset, index-size)`` pair."""
    path = os.path.join(member_dir, _INDEX_CP)
    try:
        with open(path, "rb") as f:
            data = f.read()
    except FileNotFoundError:
        return None
    last = None
    prev_off = -1
    for line in data.splitlines():
        try:
            off_s, size_s = line.decode("ascii").split(" ")
            off, size = int(off_s), int(size_s)
        except (ValueError, UnicodeDecodeError):
            break
        if off <= prev_off or size < 0:
            break
        last = (off, size)
        prev_off = off
    return last


def _publish_scan_checkpoint(cp_fh, member_dir, index_fh, offset,
                             migrated, skipped, dirty, seg_name,
                             seg_size):
    """Fsync the index, then the output-prefix and index boundary."""
    index_fh.flush()
    os.fsync(index_fh.fileno())
    index_size = index_fh.tell()
    _publish_prefix_checkpoint(
        cp_fh, member_dir, offset, migrated, skipped, dirty,
        seg_name, seg_size)
    with open(os.path.join(member_dir, _INDEX_CP), "ab") as icp:
        icp.write(f"{offset} {index_size}\n".encode("ascii"))
        icp.flush()
        os.fsync(icp.fileno())
    _fsync_dir(member_dir)


def _recover_index(member_dir, resume_offset):
    """Open the spill, truncated to the boundary at *resume_offset*.

    Returns ``(index_fh, durable_row_count)``.  When no boundary
    matches (fresh start, or a kill landed between the coupled
    checkpoint files) the spill is wiped; the caller's row-count guard
    then restarts the member from scratch rather than trusting orphan
    rows.
    """
    boundary = _read_index_checkpoint(member_dir)
    index_path = os.path.join(member_dir, _INDEX)
    valid = boundary is not None and boundary[0] == resume_offset
    if valid:
        try:
            if os.path.getsize(index_path) != boundary[1]:
                with open(index_path, "r+b") as f:
                    f.truncate(boundary[1])
        except FileNotFoundError:
            pass
    else:
        try:
            with open(index_path, "wb"):
                pass
        except FileNotFoundError:
            pass
    index_fh = open(index_path, "a+b")
    index_fh.seek(0)
    rows = sum(1 for _ in index_fh) if valid else 0
    index_fh.seek(0, os.SEEK_END)
    return index_fh, rows


# ---------------------------------------------------------------------------
# Per-member prepare: scan, assemble, drain (mirrors group _prepare_member)
# ---------------------------------------------------------------------------


class _LinkedMember:
    def __init__(self, path, group_index, member_dir, src, resumed,
                 dirty, offset, lineno, run_good, run_skipped,
                 run_rewritten, index_rows, first_new_lineno=0,
                 fully_prepared=False):
        self.path = path
        self.group_index = group_index
        self.member_dir = member_dir
        self.src = src
        # resumed: prepare reused a scan checkpoint; fully_prepared:
        # the member already had a complete post-verification final
        # (nothing this invocation has to scan, filter or count).
        self.resumed = resumed
        self.fully_prepared = fully_prepared
        self.scan_dirty = dirty
        self.dirty = dirty
        self.offset = offset
        self.lineno = lineno
        # Source line number at which THIS invocation's scan starts;
        # counters/audits only cover lines past it, so a member
        # resumed from a checkpoint contributes zero for covered rows.
        self.first_new_lineno = first_new_lineno
        # Counters cover only lines THIS invocation scans; resumed
        # members and finishing/convoy work contribute zero here.
        self.run_good = run_good
        self.run_skipped = run_skipped
        self.run_rewritten = run_rewritten
        self.index_rows = index_rows
        # Set after the reference check: changed records that survive.
        self.surviving_rewritten = 0
        self.salvaged = 0
        self.conv_skipped = 0
        self.conv_rewritten = 0
        self.final_size = 0


def _resume_prepared_linked(path, member_dir, on_bad):
    """Fast path: a prepared member is neither rescanned nor rewritten."""
    info = _read_prepared(member_dir)
    if info is None:
        return None
    try:
        inode = os.stat(path).st_ino
    except OSError:
        return None
    cp_inode, mode, _records, _good = _read_checkpoint_log(member_dir)
    if cp_inode != inode or mode != on_bad:
        return None
    # Only a marker the earlier run published *after* verification and
    # filtering is a complete fast-resume.  Anything less re-enters the
    # prepare path (checkpoints still prevent a rescan).
    if not info.get("complete"):
        return None
    if info["dirty"] and not os.path.isfile(
        os.path.join(member_dir, _FINAL)):
        # The final was consumed by a staging that rolled back; the
        # member re-assembles from its checkpoints (still no rescan).
        return None
    try:
        src = open(path, "rb")
    except OSError:
        return None
    member = _LinkedMember(
        path, -1, member_dir, src, resumed=True,
        dirty=bool(info["dirty"]), offset=int(info["offset"]),
        lineno=int(info["lineno"]), run_good=0, run_skipped=0,
        run_rewritten=0, index_rows=int(info.get("index_rows", 0)),
        fully_prepared=True)
    member.final_size = int(info["final_size"])
    return member


def _prepare_linked_member(path, group_index, member_dir, on_bad,
                           segment_size, quiesce, audit_stream,
                           allow_fresh_retry=True):
    """Scan one member to a durable ``final`` plus its reference spill.

    No ``prepared`` marker is published here: the caller publishes it
    only after the global reference check and skip-mode filtering, so a
    kill in between re-enters here and resumes from the scan checkpoint
    (covered bytes are never rescanned) instead of trusting an
    unfiltered final.
    """
    current_inode = os.stat(path).st_ino
    state = _prepare_workdir(
        path, member_dir, segment_size, current_inode, on_bad)
    resumed = state.get("resumed", False)
    writer = state["writer"]
    cp_fh = state["cp_fh"]
    offset = state["offset"]
    migrated = state["count"]
    skipped = state["skipped"]
    dirty = state["dirty"]
    lineno = migrated + skipped

    run_good = run_skipped = run_rewritten = 0
    first_new_lineno = lineno
    index_fh, durable_rows = _recover_index(member_dir, offset)

    # The spill and the scan checkpoint are written back to back; if a
    # kill landed between the two files, the spill cannot be trusted to
    # match the recovered output prefix -- redo this one member from
    # scratch (rare, still byte-identical because migration is
    # deterministic).
    if durable_rows != migrated + skipped and allow_fresh_retry:
        writer.abort()
        cp_fh.close()
        index_fh.close()
        shutil.rmtree(member_dir, ignore_errors=True)
        return _prepare_linked_member(
            path, group_index, member_dir, on_bad, segment_size,
            quiesce, audit_stream, allow_fresh_retry=False)

    follow_window = max(1.0, quiesce * 20)
    src = open(path, "rb")

    def checkpoint(seg_name):
        _publish_scan_checkpoint(
            cp_fh, member_dir, index_fh, offset, migrated, skipped,
            dirty, seg_name,
            os.path.getsize(os.path.join(member_dir, seg_name)))

    def handle(raw, lineno):
        """Decode + index one line; strict defers to the global order."""
        nonlocal dirty, skipped, migrated
        nonlocal run_good, run_skipped, run_rewritten
        dec = _decode_line(raw)
        out, _key, bad, _vbad, changed = dec
        index_fh.write(json.dumps(
            _index_row(lineno, dec), ensure_ascii=True).encode("ascii"))
        index_fh.write(b"\n")
        if bad:
            if on_bad != "strict":
                audit_stream.write(_audit_line(lineno, raw))
                audit_stream.flush()
            skipped += 1
            run_skipped += 1
            dirty = True
            return None
        if changed:
            dirty = True
            run_rewritten += 1
        migrated += 1
        run_good += 1
        return out

    try:
        # --- SCAN (checkpointed, resumable).
        for raw in _read_lines(src, quiesce, offset=offset,
                               follow=follow_window):
            lineno += 1
            offset += len(raw)
            out = handle(raw, lineno)
            if out is not None:
                writer.write(out)
            if writer.size >= writer.limit:
                name = writer.current_segment
                writer.close()
                checkpoint(name)

        if writer.has_open_segment:
            name = writer.current_segment
            writer.close()
            checkpoint(name)
    except BaseException:
        writer.abort()
        cp_fh.close()
        index_fh.close()
        src.close()
        raise
    writer.abort()
    cp_fh.close()

    # --- ASSEMBLE + DRAIN for every member.  Even a canonical member
    # gets a final: the global reference check may still force a
    # rewrite (skip mode dropping a canonical, already-current
    # record whose reference is bad).  Drain rows append to final and
    # spill past the last checkpoint, so a killed drain is rolled back
    # with the assembled final and deterministically replayed.
    segments = sorted(
        n for n in os.listdir(member_dir) if n.startswith("seg-"))
    final = _assemble(member_dir, segments,
                      os.path.join(member_dir, _FINAL))
    try:
        with open(final, "ab") as out_fh:
            for raw in _read_lines(src, quiesce, offset=offset,
                                   follow=follow_window):
                lineno += 1
                offset += len(raw)
                dec = _decode_line(raw)
                out, _k, bad, _v, changed = dec
                index_fh.write(json.dumps(
                    _index_row(lineno, dec), ensure_ascii=True
                ).encode("ascii"))
                index_fh.write(b"\n")
                if bad:
                    # Indexed like scan rows; strict defers the raise
                    # to the global first-offense decision.
                    if on_bad != "strict":
                        audit_stream.write(_audit_line(lineno, raw))
                        audit_stream.flush()
                    run_skipped += 1
                    dirty = True
                    continue
                out_fh.write(out)
                run_good += 1
                if changed:
                    dirty = True
                    run_rewritten += 1
            out_fh.flush()
            os.fsync(out_fh.fileno())
        index_fh.flush()
        os.fsync(index_fh.fileno())
    except BaseException:
        index_fh.close()
        src.close()
        raise
    index_fh.close()

    total_rows = durable_rows + run_good + run_skipped
    return _LinkedMember(
        path, group_index, member_dir, src, resumed, dirty, offset,
        lineno, run_good, run_skipped, run_rewritten, total_rows,
        first_new_lineno=first_new_lineno + 1)


# ---------------------------------------------------------------------------
# Global reference verification (on-disk SQLite)
# ---------------------------------------------------------------------------
#
# ``idx`` holds one row per emitted record in flat member order; seq is
# therefore exactly the (group order, member order, line order) total
# order.  ``edge`` materializes one row per (good source record,
# declared reference): its target is the earliest line of the target
# group with the same order id (the source line itself is never its own
# target).  All graph algorithms run inside the database -- neither the
# key universe nor the edge set is ever held in memory.


def _connect_db(group_dir):
    db_path = os.path.join(group_dir, _REFS_DB)
    try:
        os.remove(db_path)
    except FileNotFoundError:
        pass
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=MEMORY")
    conn.execute("PRAGMA synchronous=OFF")
    return conn, db_path


def _build_ref_db(conn, members):
    """Bulk-load every member's spill (scan + drain rows) into the db."""
    conn.execute(
        "CREATE TABLE idx (seq INTEGER PRIMARY KEY AUTOINCREMENT,"
        " mi INTEGER, gi INTEGER, lineno INTEGER, good INTEGER,"
        " k TEXT, vb INTEGER)")
    batch = []
    for mi, member in enumerate(members):
        with open(os.path.join(member.member_dir, _INDEX), "rb") as f:
            for line in f:
                row = json.loads(line.decode("ascii"))
                batch.append((
                    mi, member.group_index, int(row["l"]),
                    int(row["ok"]), row.get("k"),
                    int(row.get("vb", 0))))
                if len(batch) >= 8192:
                    conn.executemany(
                        "INSERT INTO idx(mi,gi,lineno,good,k,vb)"
                        " VALUES (?,?,?,?,?,?)", batch)
                    batch.clear()
    if batch:
        conn.executemany(
            "INSERT INTO idx(mi,gi,lineno,good,k,vb)"
            " VALUES (?,?,?,?,?,?)", batch)
    conn.execute("CREATE INDEX idx_gk ON idx(gi, k)")
    conn.commit()


def _materialize_edges(conn, refs):
    """One edge per good source record per declared reference.

    The target is the earliest member-order/line-order record of the
    target group carrying the same order id -- when source and target
    are the same group that can be the source line itself, which is a
    self-loop reference (a cycle of length one).
    """
    conn.execute(
        "CREATE TABLE edge (src_seq INTEGER NOT NULL,"
        " dst_seq INTEGER, decl INTEGER NOT NULL)")
    for decl, (src_group, dst_group) in enumerate(refs):
        conn.execute(
            "INSERT INTO edge(src_seq, dst_seq, decl) "
            "SELECT s.seq, "
            "(SELECT t.seq FROM idx t WHERE t.gi=? AND t.k=s.k "
            "ORDER BY t.seq LIMIT 1), ? "
            "FROM idx s WHERE s.good=1 AND s.gi=?",
            (dst_group, decl, src_group))
    conn.execute("CREATE INDEX edge_src ON edge(src_seq)")
    conn.execute("CREATE INDEX edge_dst ON edge(dst_seq)")
    conn.commit()


def _build_reachability(conn):
    """Transitive reachability over edges between good records.

    ``reach(a, b)`` says a chain of declared edges gets from record *a*
    to record *b* without crossing a bad line.  Built purely in the
    database (fixpoint joins), so the edge graph is never held in
    memory; an edge ``s -> t`` is then cyclic exactly when *t* can
    reach *s* again (the chain's head reconnects with its source).
    """
    conn.execute(
        "CREATE TEMP TABLE reach (a INTEGER NOT NULL, b INTEGER NOT NULL,"
        " PRIMARY KEY(a,b)) WITHOUT ROWID")
    conn.execute(
        "CREATE TEMP TABLE frontier (a INTEGER NOT NULL, "
        "b INTEGER NOT NULL, PRIMARY KEY(a,b)) WITHOUT ROWID")
    conn.execute(
        "INSERT OR IGNORE INTO reach(a, b) "
        "SELECT e.src_seq, e.dst_seq FROM edge e "
        "JOIN idx d ON d.seq=e.dst_seq WHERE d.good=1")
    conn.execute("INSERT INTO frontier(a, b) SELECT a, b FROM reach")
    conn.commit()
    conn.execute(
        "CREATE TEMP TABLE nextf (a INTEGER NOT NULL, "
        "b INTEGER NOT NULL, PRIMARY KEY(a,b)) WITHOUT ROWID")
    # Delta fixpoint: only the pairs discovered last round are expanded
    # one more hop, so the total work is linear in the pairs derived.
    while True:
        conn.execute("DELETE FROM nextf")
        conn.execute(
            "INSERT OR IGNORE INTO nextf(a, b) "
            "SELECT f.a, e.dst_seq FROM frontier f "
            "JOIN edge e ON e.src_seq=f.b "
            "JOIN idx d ON d.seq=e.dst_seq WHERE d.good=1")
        conn.execute("DELETE FROM nextf WHERE EXISTS ("
                     "SELECT 1 FROM reach WHERE reach.a=nextf.a "
                     "AND reach.b=nextf.b)")
        if not conn.execute("SELECT COUNT(*) FROM nextf").fetchone()[0]:
            break
        conn.execute("INSERT INTO reach(a, b) SELECT a, b FROM nextf")
        conn.execute("DELETE FROM frontier")
        conn.execute("INSERT INTO frontier(a, b) SELECT a, b FROM nextf")
        conn.commit()
    conn.execute("CREATE INDEX reach_ba ON reach(b, a)")
    conn.commit()


def _first_bad_line(conn):
    """``(mi, lineno)`` of the first bad line (illegal v included)."""
    return conn.execute(
        "SELECT mi, lineno FROM idx WHERE good=0 ORDER BY seq LIMIT 1"
    ).fetchone()


def _iter_bad_references(conn):
    """Yield ``(mi, lineno, kind, target)`` for every bad edge, ordered.

    Edges are ordered by their source's global position and then by
    declaration order, so the first yield is the globally first bad
    reference.  A cycle is an *edge-level* property (its target reaches
    its source again): another declaration of the same record may be
    perfectly fine and must not be reported as cyclic.
    """
    cursor = conn.execute(
        "SELECT i.mi, i.lineno, i.k, d.good, d.vb, "
        "EXISTS (SELECT 1 FROM reach WHERE reach.a=e.dst_seq "
        "AND reach.b=i.seq) AS loops "
        "FROM edge e JOIN idx i ON i.seq=e.src_seq "
        "LEFT JOIN idx d ON d.seq=e.dst_seq "
        "ORDER BY i.seq, e.decl")
    for mi, lineno, key, dgood, dvb, loops in cursor:
        kind = None
        if dgood is None:
            kind = "missing"
        elif not dgood:
            kind = "illegal-target-version" if dvb else "target-skipped"
        elif loops:
            kind = "cycle"
        if kind is not None:
            yield mi, lineno, kind, key


def _read_source_line(path, lineno):
    with open(path, "rb") as f:
        for i, raw in enumerate(f, start=1):
            if i == lineno:
                return raw
    return b""


def _raise_first_offense(conn, members):
    """Strict mode: stop at the globally first bad line or bad reference."""
    bad_ref = next(_iter_bad_references(conn), None)
    bad_line = _first_bad_line(conn)
    if bad_ref is not None and (bad_line is None
                                or (bad_ref[0], bad_ref[1])
                                <= (bad_line[0], bad_line[1])):
        mi, lineno, kind, target = bad_ref
        raise LinkedBadReferenceError(
            members[mi].path, lineno, kind, target)
    if bad_line is not None:
        mi, lineno = bad_line
        raw = _read_source_line(members[mi].path, lineno)
        raise GroupBadRecordError(
            members[mi].path, lineno, raw, _strict_cause(raw))


def _compute_dropped(conn):
    """Skip-mode closure of source lines that cannot survive.

    Dropped: bad/illegal-v lines themselves, nodes with an edge to a
    missing/bad target, nodes whose edge loops back to them (a cycle),
    and transitively every node an edge of which reaches a dropped
    line.
    """
    conn.execute("CREATE TABLE dropseq (seq INTEGER PRIMARY KEY)")
    conn.execute("INSERT INTO dropseq(seq) SELECT seq FROM idx "
                 "WHERE good=0")
    conn.execute(
        "INSERT OR IGNORE INTO dropseq(seq) "
        "SELECT DISTINCT i.seq FROM idx i "
        "JOIN edge e ON e.src_seq=i.seq "
        "LEFT JOIN idx d ON d.seq=e.dst_seq "
        "WHERE i.good=1 AND (d.seq IS NULL OR d.good=0)")
    conn.execute(
        "INSERT OR IGNORE INTO dropseq(seq) "
        "SELECT DISTINCT e.src_seq FROM edge e "
        "JOIN reach r ON r.a=e.dst_seq AND r.b=e.src_seq")
    while True:
        changes = conn.total_changes
        conn.execute(
            "INSERT OR IGNORE INTO dropseq(seq) "
            "SELECT DISTINCT e.src_seq FROM edge e "
            "JOIN dropseq z ON z.seq=e.dst_seq")
        conn.commit()
        if conn.total_changes == changes:
            break
    conn.commit()


def _write_drop_lists(conn, members):
    """Per-member source line numbers of good records being dropped."""
    counts = [0] * len(members)
    for mi, member in enumerate(members):
        count = 0
        with open(os.path.join(member.member_dir, _DROP), "wb") as f:
            cursor = conn.execute(
                "SELECT lineno FROM idx WHERE mi=? AND good=1 AND seq IN "
                "(SELECT seq FROM dropseq) ORDER BY lineno", (mi,))
            while True:
                rows = cursor.fetchmany(8192)
                if not rows:
                    break
                for (lineno,) in rows:
                    f.write(f"{lineno}\n".encode("ascii"))
                    count += 1
        counts[mi] = count
        _fsync_dir(member.member_dir)
    return counts


def _is_bad_edge_condition():
    """SQL predicate: an edge whose target is missing/dropped/cyclic."""
    return (
        "(d.seq IS NULL OR d.seq IN (SELECT seq FROM dropseq) OR "
        " EXISTS (SELECT 1 FROM reach WHERE reach.a=e.dst_seq "
        " AND reach.b=i.seq))")


def _count_bad_edges(conn, members, fresh):
    """Bad reference edges on lines this invocation freshly prepared.

    Covers all four reference kinds: after skip closure an edge is bad
    exactly when its target is missing or its resolved line is being
    dropped (a bad/illegal-v target or a transitively pulled-down
    record), or when the edge loops back to its source (a cycle).
    Per-member first-new-line boundaries exclude edges a killed run
    already resolved.
    """
    total = 0
    for mi, is_fresh in enumerate(fresh):
        if not is_fresh:
            continue
        lower = members[mi].first_new_lineno
        row = conn.execute(
            "SELECT COUNT(*) FROM edge e JOIN idx i ON i.seq=e.src_seq "
            "LEFT JOIN idx d ON d.seq=e.dst_seq "
            "WHERE i.mi=? AND i.lineno>=? AND i.good=1 AND "
            + _is_bad_edge_condition(),
            (mi, lower)).fetchone()
        total += int(row[0])
    return total


def _audit_drops(conn, members, fresh, audit_stream):
    """Audit every good record the reference check drops, this run only.

    One ``<file>:<lineno>:<first 32 bytes>`` line per removed record
    (both bad-edge records and transitively pulled-down records),
    restricted to source lines this invocation newly scanned; sources
    still name the pre-commit inodes.  The ordered database cursor and
    the ordered source file are merged as two streams, so neither the
    dropped-line set nor the file is held in memory.
    """
    for mi, member in enumerate(members):
        if not fresh[mi]:
            continue
        lower = member.first_new_lineno
        cursor = conn.execute(
            "SELECT lineno FROM idx WHERE mi=? AND good=1 AND lineno>=? "
            "AND seq IN (SELECT seq FROM dropseq) ORDER BY lineno",
            (mi, lower))
        row = cursor.fetchone()
        if row is None:
            continue
        next_drop = row[0]
        member_audit = _AuditPrefix(audit_stream, member.path)
        with open(member.path, "rb") as src:
            lineno = 0
            while True:
                raw = src.readline()
                if not raw:
                    break
                lineno += 1
                if next_drop is None or lineno < next_drop:
                    continue
                if lineno == next_drop:
                    member_audit.write(_audit_line(lineno, raw))
                    row = cursor.fetchone()
                    next_drop = row[0] if row is not None else None
                    if next_drop is None:
                        break
        member_audit.flush()


# ---------------------------------------------------------------------------
# Filtered assembly (skip mode)
# ---------------------------------------------------------------------------


class _FinalLineReader:
    """Readline view of one final file."""

    def __init__(self, path):
        self._fh = open(path, "rb")

    def readline(self):
        return self._fh.readline()

    def close(self):
        self._fh.close()


def _iter_drop_linenos(member_dir):
    """Yield the sorted dropped source line numbers from the drop file."""
    path = os.path.join(member_dir, _DROP)
    if not os.path.exists(path):
        return
    with open(path, "rb") as f:
        for line in f:
            if line.strip():
                yield int(line)


def _count_surviving_rewritten(member):
    """Count this invocation's surviving changed records from the spill.

    A changed record that the reference check later drops is not
    "newly migrated", and lines a checkpoint already covered belong to
    the killed run -- both are excluded by a streaming merge against
    the ordered drop file (no drop set is held in memory) and the
    member's first-new-line boundary.
    """
    drops = iter(_iter_drop_linenos(member.member_dir))
    try:
        next_drop = next(drops)
    except StopIteration:
        next_drop = None
    total = 0
    with open(os.path.join(member.member_dir, _INDEX), "rb") as f:
        for line in f:
            row = json.loads(line.decode("ascii"))
            lineno = int(row["l"])
            while next_drop is not None and next_drop < lineno:
                try:
                    next_drop = next(drops)
                except StopIteration:
                    next_drop = None
            is_dropped = next_drop is not None and next_drop == lineno
            if is_dropped:
                try:
                    next_drop = next(drops)
                except StopIteration:
                    next_drop = None
            elif (row["ok"] and row.get("ch")
                    and lineno >= member.first_new_lineno):
                total += 1
    return total


def _filter_final(member):
    """Rewrite ``final`` dropping good records on the member's drop list.

    Spill rows and final lines are produced in lockstep (every ``ok=1``
    row is exactly the next final line, in source-line order); the
    rewrite is a streaming merge against the ordered drop file that
    holds neither side whole.
    """
    member_dir = member.member_dir
    final = os.path.join(member_dir, _FINAL)
    tmp = final + ".new"
    drops = iter(_iter_drop_linenos(member_dir))
    try:
        next_drop = next(drops)
    except StopIteration:
        next_drop = None
    reader = _FinalLineReader(final)
    try:
        with open(tmp, "wb") as out, \
                open(os.path.join(member_dir, _INDEX), "rb") as idx:
            for row_line in idx:
                row = json.loads(row_line.decode("ascii"))
                lineno = int(row["l"])
                while next_drop is not None and next_drop < lineno:
                    try:
                        next_drop = next(drops)
                    except StopIteration:
                        next_drop = None
                if not row["ok"]:
                    continue
                line = reader.readline()
                if not line:
                    raise OSError(
                        "linked final/index mismatch while filtering "
                        f"{member.path!r}")
                if next_drop is not None and next_drop == lineno:
                    try:
                        next_drop = next(drops)
                    except StopIteration:
                        next_drop = None
                    continue
                out.write(line)
            out.flush()
            os.fsync(out.fileno())
    finally:
        reader.close()
    os.replace(tmp, final)
    _fsync_dir(member_dir)
    return os.path.getsize(final)


# ---------------------------------------------------------------------------
# Two-phase rename (mirrors group_migration with linked debris names)
# ---------------------------------------------------------------------------


def _publish_member_marker(path):
    marker = _commit_marker_path(path)
    parent = os.path.dirname(os.path.abspath(path))
    with open(marker + ".tmp", "wb") as f:
        f.write(b"linked\n")
        f.flush()
        os.fsync(f.fileno())
    os.replace(marker + ".tmp", marker)
    _fsync_dir(parent)


def _stage_member(member):
    path = member.path
    parent = os.path.dirname(os.path.abspath(path))
    d = _debris(path)
    os.replace(os.path.join(member.member_dir, _FINAL), d["staged"])
    _publish_member_marker(path)
    os.replace(path, d["backup"])
    os.replace(d["staged"], path)
    _fsync_dir(parent)


def _reconcile_member(path, committed):
    """Resolve one member's interrupted rename state, deterministically."""
    d = _debris(path)
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


def _undo_staged_members(members):
    failed = False
    for member in members:
        try:
            _reconcile_member(member.path, committed=False)
        except OSError:
            failed = True
    return failed


def _acquire_member_locks(paths):
    fhs = []
    try:
        for path in paths:
            fh = open(path + ".migrate.lock", "a+b")
            fcntl.flock(fh, fcntl.LOCK_EX)
            fhs.append(fh)
    except BaseException:
        for fh in fhs:
            fcntl.flock(fh, fcntl.LOCK_UN)
            fh.close()
        raise
    return fhs


# ---------------------------------------------------------------------------
# Post-border finishing
# ---------------------------------------------------------------------------


class _NullAudit:
    def write(self, data):
        pass

    def flush(self):
        pass


def _align_backup_resume(path, backup_path, info):
    """Locate where converging the backup inode must resume.

    The killed run's ``final`` (``final_size`` bytes) is the prefix of
    every convergence rebuild; the suffix already folded into ``path``
    therefore starts with migrated backup-inode records.  Replaying
    backup lines from the recorded drain offset and matching them
    against that suffix pinpoints the consumed boundary so the rerun
    neither folds a record twice nor loses a reachable one.
    """
    resume_offset = int(info["offset"])
    resume_lineno = int(info["lineno"])
    staged_size = info.get("final_size")
    if not isinstance(staged_size, int) or staged_size < 0:
        return resume_offset, resume_lineno
    try:
        live_size = os.path.getsize(path)
        if live_size < staged_size:
            return resume_offset, resume_lineno
        with open(path, "rb") as live:
            live.seek(staged_size)
            folded = live.read(live_size - staged_size)
    except OSError:
        return resume_offset, resume_lineno
    if not folded:
        return resume_offset, resume_lineno

    leftover = folded
    try:
        backup = open(backup_path, "rb")
    except OSError:
        return resume_offset, resume_lineno
    try:
        backup.seek(resume_offset)
        while leftover:
            raw = backup.readline()
            if not raw or not raw.endswith(b"\n"):
                break
            resume_lineno += 1
            try:
                out, bad = _emit(_NullAudit(), "skip", raw,
                                 resume_lineno)
            except BadRecordError:
                resume_lineno -= 1
                break
            if bad:
                resume_offset += len(raw)
                continue
            if not leftover.startswith(out):
                resume_lineno -= 1
                break
            leftover = leftover[len(out):]
            resume_offset += len(raw)
    finally:
        backup.close()
    return resume_offset, resume_lineno


def _converge_one_committed_backup(path, on_bad, quiesce, audit_stream):
    """Finish one member that still has a post-rename backup.

    ``path`` names the migrated inode and the backup is the old inode.
    Late whole-record writes on the backup are converged (aligned past
    whatever the killed run already folded) and the backup deleted only
    once drained.  Returns a warning string or None; faults never fail
    the already-crossed border.
    """
    parent = os.path.dirname(os.path.abspath(path))
    d = _debris(path)
    member_dir = _member_workdir(path)
    backup_path = d["backup"]
    old_info = _read_prepared(member_dir)
    if old_info is None or "final_size" not in old_info:
        _remove(backup_path)
        _fsync_dir(parent)
        return (f"{path}: backup survived without a resume boundary; "
                "removed without draining late writes")
    os.makedirs(member_dir, exist_ok=True)
    resume_offset, resume_lineno = _align_backup_resume(
        path, backup_path, old_info)
    member_audit = _AuditPrefix(audit_stream, path)
    backup_fh = open(backup_path, "rb")
    try:
        _salvaged, _skipped, _lineno, warning, _rewritten = _converge(
            path, parent, member_dir, backup_fh, resume_offset,
            resume_lineno, quiesce, member_audit, on_bad,
            count_rewrites=True)
    except (OSError, ValueError) as exc:
        return f"{path}: post-commit convergence incomplete, rerun " \
               f"to finish: {exc}"
    finally:
        backup_fh.close()
    _remove(backup_path)
    _fsync_dir(parent)
    if warning:
        return f"{path}: {warning}"
    return None


def _converge_committed_backups(paths, on_bad, quiesce, audit_stream):
    """Drain every surviving committed-run backup before the pipeline."""
    warnings = []
    for path in paths:
        backup_path = _debris(path)["backup"]
        if not os.path.exists(backup_path):
            continue
        try:
            warning = _converge_one_committed_backup(
                path, on_bad, quiesce, audit_stream)
        except OSError as exc:
            warning = (f"{path}: post-commit finishing incomplete, "
                       f"rerun to finish: {exc}")
        if warning:
            warnings.append(warning)
    return warnings


def _run_linked_pipeline(groups, refs, paths, group_dir, on_bad,
                         segment_size, quiesce, audit_stream,
                         post_commit, pre_warnings, members_out):
    """Prepare, verify, filter and (unless post-border) commit.

    Used by both a fresh run and a committed finishing run: with
    *post_commit* the border marker already exists, so the same
    prepare/verify catches post-border tails (cross-member references
    included) and the border publication is skipped because it cannot
    be un-crossed.  A strict offense found only on a post-border tail
    cannot un-commit the set: it is reported as a warning and the run
    leaves the committed content for an explicit --skip rerun.

    Every member it opens is appended to *members_out* as it is
    prepared, so the caller can still close its source descriptor when
    the pipeline raises.
    """
    members = members_out
    fresh_flags = []
    bad_refs = 0
    warnings = list(pre_warnings)

    def placeholder(path, gi, exc):
        # Post-border prepare failure (e.g. an I/O problem on one tail)
        # must not turn the committed migration into a failure: leave
        # that member for a later rerun.
        warnings.append(
            f"{path}: post-commit finishing incomplete, rerun to "
            f"finish: {exc}")
        member = _LinkedMember(
            path, gi, _member_workdir(path), open(path, "rb"),
            resumed=False, dirty=False, offset=0, lineno=0,
            run_good=0, run_skipped=0, run_rewritten=0,
            index_rows=0)
        member.skip_verify = True
        return member

    # --- PREPARE every member (resumed via local checkpoints).
    for gi, group in enumerate(groups):
        for path in group:
            member_dir = _member_workdir(path)
            member_audit = _AuditPrefix(audit_stream, path)
            prepared = None if post_commit else _resume_prepared_linked(
                path, member_dir, on_bad)
            if prepared is None:
                try:
                    member = _prepare_linked_member(
                        path, gi, member_dir, on_bad, segment_size,
                        quiesce, member_audit)
                except OSError as exc:
                    if not post_commit:
                        raise
                    member = placeholder(path, gi, exc)
                    shutil.rmtree(member_dir, ignore_errors=True)
                fresh_flags.append(True)
            else:
                member = prepared
                member.group_index = gi
                fresh_flags.append(False)
            members.append(member)
    _crash_point("linked-prepare")

    verifiable = [m for m in members
                  if not getattr(m, "skip_verify", False)]

    # --- GLOBAL REFERENCE VERIFICATION (derived, on-disk database).
    conn, db_path = _connect_db(group_dir)
    try:
        _build_ref_db(conn, verifiable)
        _materialize_edges(conn, refs)
        _crash_point("linked-verify")
        _build_reachability(conn)
        if on_bad == "strict":
            try:
                _raise_first_offense(conn, verifiable)
            except ValueError as exc:
                if not post_commit:
                    raise
                # The border cannot be un-crossed: keep the committed
                # member bytes; sweep only derived work (never the
                # members) so an explicit --skip invocation restarts a
                # normal, reference-checked run over the v3 bodies plus
                # the bad tail.
                warnings.append(
                    "post-commit tail has a bad record/reference; run "
                    f"with --skip to migrate past it: {exc}")
                _sweep_dirs(paths, group_dir)
                return {
                    "members": members, "fresh": fresh_flags,
                    "replaced": False, "bad_refs": 0,
                    "post_commit_error": "; ".join(warnings) or None,
                }
        else:
            _compute_dropped(conn)
            _write_drop_lists(conn, verifiable)
            verif_fresh = [f for m, f in zip(members, fresh_flags)
                           if not getattr(m, "skip_verify", False)]
            bad_refs = _count_bad_edges(conn, verifiable, verif_fresh)
            _audit_drops(conn, verifiable, verif_fresh, audit_stream)
    finally:
        conn.close()
        try:
            os.remove(db_path)
        except OSError:
            pass

    # --- FILTER (skip) and publish complete prepared markers.
    for member in members:
        if getattr(member, "skip_verify", False) or member.fully_prepared:
            continue
        final_path = os.path.join(member.member_dir, _FINAL)
        drop_path = os.path.join(member.member_dir, _DROP)
        has_drop = (
            on_bad == "skip"
            and os.path.exists(drop_path)
            and os.path.getsize(drop_path) > 0)
        if has_drop:
            size = _filter_final(member)
            member.dirty = True
        else:
            size = os.path.getsize(final_path)
        member.final_size = size if member.dirty else 0
        _write_prepared(member.member_dir, {
            "dirty": bool(member.dirty),
            "offset": member.offset,
            "lineno": member.lineno,
            "index_rows": member.index_rows,
            "final_size": member.final_size,
            "complete": True,
        })
        member.surviving_rewritten = _count_surviving_rewritten(member)
    _crash_point("linked-assembled")

    dirty_members = [m for m in members if m.dirty]
    if not dirty_members:
        _sweep_dirs(paths, group_dir)
        return {
            "members": members, "fresh": fresh_flags, "replaced": False,
            "bad_refs": bad_refs,
            "post_commit_error": "; ".join(warnings) or None,
        }

    # --- PHASE 1: stage every dirty member.  This runs on the
    # post-border finishing path too (a path-reopening appender's
    # old-format tail must be promoted by a fresh rename); only the
    # border-marker publication is skipped when the border already
    # exists.
    staged = []
    try:
        for member in dirty_members:
            _stage_member(member)
            staged.append(member)
            _crash_point("linked-stage")
        _crash_point("linked-staged")
    except BaseException:
        if not post_commit and not _undo_staged_members(staged):
            _sweep_dirs(paths, group_dir)
        raise

    if not post_commit:
        marker = os.path.join(group_dir, _LINKED_COMMITTED)
        marker_tmp = os.path.join(group_dir, _LINKED_COMMITTED_TMP)
        with open(marker_tmp, "wb") as f:
            f.write(b"1\n")
            f.flush()
            os.fsync(f.fileno())
        os.replace(marker_tmp, marker)
        _fsync_dir(group_dir)
        _crash_point("linked-marker")
    replaced = True

    # --- PHASE 2: converge, drop backups, sweep.
    for member in dirty_members:
        try:
            salvaged, conv_skipped, lineno, warning, conv_rewritten = \
                _converge(
                    member.path,
                    os.path.dirname(os.path.abspath(member.path)),
                    member.member_dir, member.src, member.offset,
                    member.lineno, quiesce,
                    _AuditPrefix(audit_stream, member.path),
                    on_bad, count_rewrites=True,
                    emit_fn=_linked_emit(member.path))
        except (OSError, ValueError) as exc:
            warnings.append(
                f"{member.path}: post-commit convergence incomplete, "
                f"rerun to finish: {exc}")
            continue
        member.salvaged = salvaged
        member.conv_skipped = conv_skipped
        member.conv_rewritten = conv_rewritten
        member.lineno = lineno
        if warning:
            warnings.append(f"{member.path}: {warning}")
        try:
            _remove(member.path + _BACKUP_SUFFIX)
            _fsync_dir(os.path.dirname(os.path.abspath(member.path)))
        except OSError as exc:
            warnings.append(
                f"{member.path}: backup removal failed: {exc}")
    _crash_point("linked-backups")
    try:
        _crash_point("linked-cleanup")
        if (not post_commit
                and os.environ.get(_FAULT_ENV) == "linked-cleanup"):
            raise OSError("injected linked cleanup fault")
        _sweep_dirs(paths, group_dir)
    except OSError as exc:
        warnings.append(f"cleanup failed: {exc}")
    return {
        "members": members, "fresh": fresh_flags, "replaced": replaced,
        "bad_refs": bad_refs,
        "post_commit_error": "; ".join(warnings) or None,
    }


def _build_result(groups, refs, paths, members, fresh, replaced, bad_refs,
                  post_commit_error):
    total_rewritten = total_skipped = 0
    for member, is_fresh in zip(members, fresh):
        if is_fresh:
            total_rewritten += member.surviving_rewritten
            total_skipped += member.run_skipped
        total_rewritten += member.conv_rewritten
        total_skipped += member.conv_skipped
    total_salvaged = sum(m.salvaged for m in members if m.dirty)
    member_results = tuple(
        MigrationResult(
            path=m.path,
            records_migrated=(m.surviving_rewritten if f else 0)
            + m.conv_rewritten,
            records_skipped=(m.run_skipped if f else 0)
            + m.conv_skipped,
            records_salvaged=m.salvaged,
            replaced=replaced and m.dirty)
        for m, f in zip(members, fresh))
    return LinkedMigrationResult(
        groups=tuple(tuple(g) for g in groups),
        refs=tuple(refs),
        paths=tuple(paths),
        records_migrated=total_rewritten,
        records_skipped=total_skipped,
        references_bad=bad_refs,
        records_salvaged=total_salvaged,
        replaced=replaced,
        members=member_results,
        post_commit_error=post_commit_error)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


def migrate_linked_groups(groups, refs, *, on_bad="strict",
                          segment_size=DEFAULT_SEGMENT_SIZE,
                          quiesce=DEFAULT_QUIESCE, audit=None):
    """Migrate several explicitly listed groups as one linked unit.

    Either every member of every group ends migrated with every
    declared reference intact, or every member stays byte-for-byte as
    it was.  Members may be appended to by different processes while
    the run is alive.  Returns :class:`LinkedMigrationResult`; the
    counters count only records/references this invocation newly
    processes.

    Raises :class:`TypeError` for a non-sequence group/reference list,
    :class:`ValueError` for empty/duplicated input or an out-of-range
    reference index (and :class:`GroupBadRecordError` /
    :class:`LinkedBadReferenceError` on the first strict-mode
    offender), and :class:`FileNotFoundError` when a member is missing
    or unreadable.
    """
    if on_bad not in ("strict", "skip"):
        raise ValueError(
            f"on_bad must be 'strict' or 'skip', got {on_bad!r}")
    if segment_size <= 0:
        raise ValueError("segment_size must be positive")
    groups = _normalize_groups(groups)
    refs = _normalize_refs(refs, len(groups))
    paths = _flatten(groups)
    _probe_members(paths)

    audit_stream = audit if audit is not None else sys.stderr.buffer
    first_dir = os.path.dirname(os.path.abspath(paths[0]))
    group_dir = _group_dir_for(paths)
    lock_path = os.path.join(first_dir, _LINKED_LOCK_NAME)

    members = []
    fresh = []
    replaced = False
    post_commit_error = None
    bad_refs_fresh = 0

    with open(lock_path, "a+b") as group_lock:
        fcntl.flock(group_lock, fcntl.LOCK_EX)
        member_locks = _acquire_member_locks(paths)
        try:
            _crash_point("linked-lock")
            manifest = _read_manifest(group_dir)
            committed = os.path.exists(
                os.path.join(group_dir, _LINKED_COMMITTED))
            abs_paths = [os.path.abspath(p) for p in paths]
            abs_groups = [[os.path.abspath(p) for p in g] for g in groups]
            abs_refs = [[s, d] for s, d in refs]
            if manifest is not None:
                same_group = (
                    manifest.get("members") == abs_paths
                    and manifest.get("groups") == abs_groups
                    and manifest.get("refs") == abs_refs
                    and manifest.get("on_bad") == on_bad)
                if not same_group:
                    if committed:
                        raise ValueError(
                            "linked commit marker exists for a different "
                            "member list, reference set or policy; "
                            "refusing to mix groups")
                    old_paths = [p for p in manifest.get("members", [])
                                 if os.path.exists(p)
                                 or os.path.exists(p + _BACKUP_SUFFIX)
                                 or os.path.exists(p + _STAGED_SUFFIX)]
                    for old_path in old_paths:
                        _reconcile_member(old_path, committed=False)
                    _sweep_dirs(old_paths, group_dir)
                    manifest = None
            os.makedirs(group_dir, exist_ok=True)
            if manifest is None:
                _write_manifest(group_dir, groups, paths, refs, on_bad)
            _remove(os.path.join(group_dir, _LINKED_COMMITTED_TMP))

            for path in paths:
                _reconcile_member(path, committed)

            pre_warnings = []
            if committed:
                # Finish forward: late appends still landing on a
                # surviving backup inode are converged in place and the
                # backup deleted; afterwards the whole set is put
                # through the normal prepare/verify pipeline so
                # post-border tails are reference-checked like every
                # other record.
                pre_warnings = _converge_committed_backups(
                    paths, on_bad, quiesce, audit_stream)

            try:
                result_info = _run_linked_pipeline(
                    groups, refs, paths, group_dir, on_bad,
                    segment_size, quiesce, audit_stream,
                    post_commit=committed, pre_warnings=pre_warnings,
                    members_out=members)
            except BaseException:
                # A handled pre-border failure (e.g. the first strict
                # bad line/reference) must leave every original
                # byte-for-byte untouched and no partial intermediate;
                # staging already reversed its own renames.  A real kill
                # bypasses this, keeping durable checkpoints for resume.
                if not committed and not os.path.exists(
                        os.path.join(group_dir, _LINKED_COMMITTED)) \
                        and not any(
                            os.path.exists(p + _BACKUP_SUFFIX)
                            for p in paths):
                    _sweep_dirs(paths, group_dir)
                raise
            members = result_info["members"]
            fresh = result_info["fresh"]
            replaced = result_info["replaced"]
            post_commit_error = result_info["post_commit_error"]
            bad_refs_fresh = result_info["bad_refs"]
            return _build_result(groups, refs, paths, members, fresh,
                                 replaced, bad_refs_fresh,
                                 post_commit_error)
        finally:
            for member in members:
                try:
                    member.src.close()
                except OSError:
                    pass
            for fh in member_locks:
                fcntl.flock(fh, fcntl.LOCK_UN)
                fh.close()


# ---------------------------------------------------------------------------
# Consistent linked snapshot
# ---------------------------------------------------------------------------


def _read_member_gate_linked(path, quiesce):
    """One pinned-inode read; return ``(records, post_commit_side)``."""

    def open_blob():
        try:
            with open(path, "rb") as fh:
                return fh.read()
        except FileNotFoundError:
            return None

    def decode_blob(blob):
        parts = blob.split(b"\n")
        return [loads(line + b"\n") for line in parts[:-1]]

    marker = _commit_marker_path(path)
    if not os.path.exists(marker):
        blob = open_blob()
        if blob is None:
            blob = _wait_path(path, quiesce)
            return decode_blob(blob), True
        if not os.path.exists(marker):
            return decode_blob(blob), False
    blob = open_blob()
    if blob is None:
        blob = _wait_path(path, quiesce)
    return decode_blob(blob), True


def read_linked_logs(groups, *, quiesce=DEFAULT_QUIESCE):
    """Return one version-consistent snapshot of every linked member.

    A flat list of decoded records is returned: groups in list order,
    members within a group in list order, then lines in file order.
    Across the whole snapshot the old (v1/v2) and current (v3) field
    shapes never mix -- between members or inside one member.  The
    group list validates exactly like :func:`migrate_linked_groups`;
    a complete but undecodable line raises ValueError, like
    :func:`proto_migrate.loads`.
    """
    groups = _normalize_groups(groups)
    paths = _flatten(groups)
    _probe_members(paths)

    snapshots = [_read_member_gate_linked(path, quiesce) for path in paths]
    any_post = any(post for _records, post in snapshots)
    all_old = all(
        not post and all(rec["v"] != CURRENT_VERSION for rec in records)
        for records, post in snapshots)
    if not any_post and all_old:
        return [rec for records, _post in snapshots for rec in records]
    return [
        migrate(rec, CURRENT_VERSION)
        for records, _post in snapshots
        for rec in records
    ]


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _parse_ref_token(text, group_count):
    if text.count(":") != 1:
        raise ValueError(f"bad --ref {text!r}: expected SRC:DST")
    src_s, dst_s = text.split(":")
    if not (src_s.isdigit() and dst_s.isdigit()):
        raise ValueError(f"bad --ref {text!r}: indices must be integers")
    src, dst = int(src_s), int(dst_s)
    if not (0 <= src < group_count and 0 <= dst < group_count):
        raise ValueError(
            f"bad --ref {text!r}: index out of range (have "
            f"{group_count} groups)")
    return src, dst


def run_linked_cli(argv):
    parser = argparse.ArgumentParser(
        prog="python3 -m proto_migrate migrate-linked-logs",
        description="Migrate multiple groups of append-only JSONL logs "
        "with declared cross-group order references as one atomic "
        "unit: every reference still resolves or every member stays "
        "untouched; online, resumable from durable checkpoints.")
    parser.add_argument(
        "--group", dest="groups", action="append", nargs="+",
        metavar="FILE", default=[],
        help="one log group (repeat for more groups): its member files")
    parser.add_argument(
        "--ref", dest="refs", action="append", metavar="SRC:DST",
        default=[],
        help="reference declaration (repeat): every record of group "
        "SRC points at the earliest same-order-id record of group DST")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--strict", action="store_const", const="strict",
                      dest="on_bad", help="abort on the first bad line "
                      "or bad reference (default; exit %d)"
                      % EXIT_BAD_RECORD)
    mode.add_argument("--skip", action="store_const", const="skip",
                      dest="on_bad", help="skip bad lines/references, "
                      "auditing each to stderr")
    parser.set_defaults(on_bad="strict")
    parser.add_argument("--segment-size", type=int,
                        default=DEFAULT_SEGMENT_SIZE, metavar="BYTES")
    parser.add_argument("--quiesce-ms", type=float,
                        default=DEFAULT_QUIESCE * 1000, metavar="MS")
    args = parser.parse_args(argv)

    if not args.groups:
        print("error: at least one --group is required", file=sys.stderr)
        return EXIT_USAGE
    groups = args.groups
    try:
        refs = [_parse_ref_token(text, len(groups)) for text in args.refs]
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_USAGE

    for path in (p for g in groups for p in g):
        if not os.path.isfile(path):
            print(f"error: not a file: {path}", file=sys.stderr)
            return EXIT_ERROR
    try:
        result = migrate_linked_groups(
            groups, refs,
            on_bad=args.on_bad,
            segment_size=args.segment_size,
            quiesce=args.quiesce_ms / 1000)
    except (LinkedBadReferenceError, GroupBadRecordError,
            BadRecordError) as exc:
        print(f"error: bad record/reference at {exc}", file=sys.stderr)
        return EXIT_BAD_RECORD
    except (TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_USAGE
    except OSError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_ERROR
    print(
        "groups=%d members=%d migrated=%d skipped=%d bad_refs=%d "
        "salvaged=%d replaced=%s"
        % (len(result.groups), len(result.paths),
           result.records_migrated, result.records_skipped,
           result.references_bad, result.records_salvaged,
           "yes" if result.replaced else "no"))
    if result.post_commit_error:
        print(f"warning: {result.post_commit_error}", file=sys.stderr)
    return EXIT_OK
