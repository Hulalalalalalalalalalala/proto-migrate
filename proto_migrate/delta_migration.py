"""Incremental (delta) cross-group linked migration with a local ledger.

This is the incremental companion of :func:`migrate_linked_logs`:
:func:`migrate_linked_delta` migrates **only the rows appended since the
last migration** instead of reprocessing the whole group set.  The
baseline -- the already reconciled byte prefix of every member, together
with everything reference resolution must remember about it -- lives in
one local *reconciliation ledger* (``ledger.json``) left by the previous
(full or delta) migration.  The ledger is a plain local file: it depends
on no remote state and is rebuilt from the member files at any time.

Only the new suffix is scanned, resolved and written:

  * the reconciled prefix is never rescanned (its records are not
    decoded again) and never rewritten -- the candidate output starts
    from the prefix bytes verbatim;
  * rows appended while the run is active are migrated in their original
    order with no loss and no duplication (the same tailing, drain,
    commit and convergence protocol as a full linked run);
  * references from new rows resolve against new rows *and* the recorded
    prefix keys, including keys of prefix rows that were skipped as bad
    records or carried an illegal version key;
  * per member the run reports four reconciliation counters --
    **added** rows (new rows that survive into the output), **rewritten**
    of those whose encoding actually changed, **skipped** bad rows, and
    **dropped** good rows discarded for bad references.

Running the delta over the same input a full migration processes yields
the same counters for the new input and a byte-for-byte identical file.

Deterministic fallback
----------------------
When the ledger cannot anchor an incremental pass it is abandoned and
the run migrates the whole group set through this same engine with an
empty baseline, which is exactly one full migration.  The result marks
the fallback (``fallback=True``) and names the reason:

  * ``ledger-missing`` -- no ledger exists yet (the first delta run), a
    listed member is absent from it, or it names another group set;
  * ``ledger-corrupt`` -- the ledger is unparseable, fails its schema
    checks, names another link set / policy, or the recorded prefix no
    longer matches the member bytes;
  * ``source-inode-changed`` -- a member was replaced in place since the
    ledger was published (its inode changed).

Crash safety, leases and strict ordering reuse the full linked run's
rules: non-blocking per-member leases raise
:class:`MigrationLockedError` while a live instance holds a member and a
disappeared holder is taken over from durable checkpoints; a kill at any
point reruns only the unfinished part and the summary counts only rows
that continuation newly moves; in strict mode the first bad line or bad
reference *of the new suffix* (groups, members, then lines) raises
:class:`ValueError`.
"""

from __future__ import annotations

import argparse
import hashlib
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
    _classify_bad_line,
    _converge,
    _crash_point,
    _emit,
    _fsync_dir,
    _prepare_workdir,
    _publish_prefix_checkpoint,
    _read_checkpoint_log,
    _read_lines,
)
from .group_migration import GroupBadRecordError, _AuditPrefix, _publish_member_marker
from .linked_migration import (
    LinkedBadReferenceError,
    MigrationLockedError,
    _REASON_TEXT,
    _FINAL,
    _LINKED_COMMITTED,
    _LINKED_COMMITTED_TMP,
    _LINKED_FINAL,
    _REFS_DB,
    _LineIndex,
    _RefsDb,
    _linked_debris,
    _linked_group_dir,
    _member_leases,
    _normalize_groups,
    _normalize_links,
    _probe_members,
    _read_linked_prepared,
    _reconcile_linked_member,
    _remove,
    _salvage_backup_tail,
    _stream_line_entries,
    _undo_staged_members,
    _write_linked_prepared,
)

__all__ = [
    "DeltaMigrationResult",
    "DeltaMemberResult",
    "migrate_linked_delta",
    "run_delta_cli",
]

_DELTA_DIR_PREFIX = ".migrate-delta-tmp"
_LEDGER_NAME = "ledger.json"
_DELTA_MANIFEST = "delta-manifest.json"
_CANDIDATE_TMP = "candidate.tmp"

_MEMBER_TMP_SUFFIX = ".migrate-delta-tmp"
_BACKUP_SUFFIX = ".migrate-linked-backup"
_STAGED_SUFFIX = ".migrate-linked-staged"

_BATCH = 8192

# Fallback reasons (observable in DeltaMigrationResult.fallback_reason).
_REASON_NO_LEDGER = "ledger-missing"
_REASON_CORRUPT = "ledger-corrupt"
_REASON_INODE = "source-inode-changed"


class DeltaMemberResult(NamedTuple):
    """The four reconciliation counters for one member.

    Every consumed new source row is accounted exactly once:
    ``lines_added + lines_skipped + references_dropped`` equals the
    number of new source rows; ``lines_rewritten`` counts the subset of
    added rows whose bytes the migration changed.
    """

    path: str
    group: int
    member: int
    lines_added: int
    lines_rewritten: int
    lines_skipped: int
    references_dropped: int
    replaced: bool


class DeltaMigrationResult(NamedTuple):
    groups: tuple
    members: tuple
    records_added: int
    records_rewritten: int
    records_skipped: int
    references_dropped: int
    records_salvaged: int
    replaced: bool
    fallback: bool
    fallback_reason: str | None
    post_commit_error: str | None = None


# ---------------------------------------------------------------------------
# Work-directory layout and small JSON helpers
# ---------------------------------------------------------------------------


def _delta_group_dir(groups):
    """Hash-scoped delta work directory next to the first member.

    Reuses the linked work directory's member-set digest so both
    flavours deterministically find the same members, under a distinct
    directory name.
    """
    linked_dir = _linked_group_dir(groups)
    parent = os.path.dirname(linked_dir)
    digest = os.path.basename(linked_dir).rsplit("-", 1)[1]
    return os.path.join(parent, f"{_DELTA_DIR_PREFIX}-{digest}")


def _delta_member_dir(path):
    return path + _MEMBER_TMP_SUFFIX


def _ledger_path(group_dir):
    return os.path.join(group_dir, _LEDGER_NAME)


def _write_json_atomic(path, data):
    with open(path + ".tmp", "wb") as f:
        f.write(json.dumps(data, sort_keys=True).encode("ascii"))
        f.flush()
        os.fsync(f.fileno())
    os.replace(path + ".tmp", path)
    _fsync_dir(os.path.dirname(path))


def _read_json(path):
    with open(path, "rb") as f:
        return json.loads(f.read())


def _sweep_delta_member_dirs(paths):
    for path in paths:
        shutil.rmtree(_delta_member_dir(path), ignore_errors=True)


def _empty_baseline():
    return {"inode": None, "offset": 0, "lines": 0, "prefix_sha256": None,
            "good": [], "skipped": [], "badv": []}


def _dst_fields_by_group(links):
    """Group index -> [(link_id, destination field name)]."""
    dst = {}
    for link_id, (_src_g, dst_g, _src_f, dst_field) in enumerate(links):
        dst.setdefault(dst_g, []).append((link_id, dst_field))
    return dst


def _rebuild_baseline_from_file(path, dst_fields_links):
    """Whole-file baseline for one committed member (ledger recovery).

    Scans complete lines (byte boundary, then one decode per committed
    line) and records every current-version line as a dense target; the
    result matches what a full migration over the current file would
    anchor on.
    """
    h = hashlib.sha256()
    good = []
    prefix_len = 0
    with open(path, "rb") as f:
        lineno = 0
        while True:
            raw = f.readline()
            if not raw.endswith(b"\n"):
                break
            h.update(raw)
            prefix_len += len(raw)
            obj = loads(raw)
            keys = {field: obj[field] for _lid, field in dst_fields_links
                    if field in obj}
            good.append({"lineno": lineno, "keys": keys})
            lineno += 1
    return {
        "inode": os.stat(path).st_ino,
        "offset": prefix_len,
        "lines": len(good),
        "prefix_sha256": h.hexdigest(),
        "good": good,
        "skipped": [],
        "badv": [],
        "_rebuilt": True,
    }


def _hash_prefix(path, length):
    """sha256 of the first *length* bytes of *path* (raw, no decoding)."""
    h = hashlib.sha256()
    remaining = length
    with open(path, "rb") as f:
        while remaining:
            chunk = f.read(min(1 << 20, remaining))
            if not chunk:
                break
            h.update(chunk)
            remaining -= len(chunk)
    return h.hexdigest()


# ---------------------------------------------------------------------------
# Ledger load / validate
# ---------------------------------------------------------------------------


def _load_ledger(group_dir, groups, links, on_bad, paths, lenient=False):
    """Validate the ledger; return ``(bases, usable, reason)``.

    *bases* maps each given path to its baseline entry (an empty baseline
    when unusable); *reason* is the fallback reason, or ``None`` when the
    ledger anchors a true incremental pass.  *lenient* is used only on a
    post-border resume: the commit marker already crossed, which renames
    members (changing their inodes) by design, so the inode/prefix
    freshness checks are skipped and the stored target records are
    trusted for finishing.
    """
    abs_groups = [[os.path.abspath(p) for p in g] for g in groups]
    if lenient:
        # Post-border resume: trust whatever structurally valid ledger
        # exists (inodes changed through the staged renames); rebuild a
        # whole-file baseline for any member whose stored record is
        # missing or malformed.  The ledger is derived state, so this
        # rebuild is always safe and is not reported as a fallback.
        gi_by_path = {
            os.path.abspath(p): gi
            for gi, group in enumerate(groups) for p in group
        }
        dst_by_group = _dst_fields_by_group(links)
        try:
            ledger = _read_json(_ledger_path(group_dir))
        except (OSError, ValueError):
            ledger = None
        stored = ledger.get("members") if isinstance(ledger, dict) else None
        bases = {}
        for path in paths:
            ap = os.path.abspath(path)
            entry = stored.get(ap) if isinstance(stored, dict) else None
            if isinstance(entry, dict) and isinstance(entry.get("good"), list) \
                    and isinstance(entry.get("skipped"), list) \
                    and isinstance(entry.get("badv"), list):
                bases[path] = {
                    "inode": entry.get("inode"),
                    "offset": int(entry.get("offset", 0)),
                    "lines": int(entry.get("lines", 0)),
                    "prefix_sha256": entry.get("prefix_sha256"),
                    "good": entry["good"],
                    "skipped": entry["skipped"],
                    "badv": entry["badv"],
                }
            else:
                bases[path] = _rebuild_baseline_from_file(
                    path, dst_by_group.get(gi_by_path[ap], []))
        return bases, True, None

    empties = {p: _empty_baseline() for p in paths}
    try:
        ledger = _read_json(_ledger_path(group_dir))
    except FileNotFoundError:
        return empties, False, _REASON_NO_LEDGER
    except (OSError, ValueError):
        return empties, False, _REASON_CORRUPT

    if not isinstance(ledger, dict) or ledger.get("version") != 1 \
            or not isinstance(ledger.get("members"), dict):
        return empties, False, _REASON_CORRUPT
    if ledger.get("groups") != abs_groups:
        return empties, False, _REASON_NO_LEDGER
    # A different link set or bad-record policy makes the recorded
    # baseline unusable for this request.
    if ledger.get("links") != [list(l) for l in links] \
            or ledger.get("on_bad") != on_bad:
        return empties, False, _REASON_CORRUPT

    stored = ledger["members"]
    bases = {}
    reason = None

    def fail(why):
        nonlocal reason
        if reason is None:
            reason = why

    for path in paths:
        ap = os.path.abspath(path)
        entry = stored.get(ap)
        if not isinstance(entry, dict):
            fail(_REASON_NO_LEDGER)
            bases[path] = _empty_baseline()
            continue
        try:
            inode = entry["inode"]
            offset = int(entry["offset"])
            lines = int(entry["lines"])
            digest = entry["prefix_sha256"]
            good, skipped, badv = entry["good"], entry["skipped"], entry["badv"]
            if not isinstance(inode, int) or not isinstance(digest, str) \
                    or offset < 0 or lines < 0 \
                    or not all(isinstance(x, list) for x in
                               (good, skipped, badv)):
                raise ValueError("bad ledger entry")
        except (KeyError, TypeError, ValueError):
            fail(_REASON_CORRUPT)
            bases[path] = _empty_baseline()
            continue
        try:
            current_inode = os.stat(path).st_ino
            size = os.path.getsize(path)
        except OSError:
            fail(_REASON_CORRUPT)
            bases[path] = _empty_baseline()
            continue
        if inode != current_inode:
            fail(_REASON_INODE)
            bases[path] = _empty_baseline()
            continue
        if size < offset or _hash_prefix(path, offset) != digest:
            fail(_REASON_CORRUPT)
            bases[path] = _empty_baseline()
            continue
        bases[path] = {
            "inode": inode, "offset": offset, "lines": lines,
            "prefix_sha256": digest, "good": good,
            "skipped": skipped, "badv": badv,
        }
    return bases, reason is None, reason


# ---------------------------------------------------------------------------
# Incremental per-member prepare
# ---------------------------------------------------------------------------


class _DeltaMember:
    def __init__(self, index, group_index, local_index, path, member_dir,
                 src, base_offset, base_line, offset, lineno, good,
                 rewritten, skipped, stage_name):
        self.index = index
        self.group_index = group_index
        self.local_index = local_index
        self.path = path
        self.member_dir = member_dir
        self.src = src
        self.base_offset = base_offset
        self.base_line = base_line
        # Absolute drain-end offset / global line count the convergence
        # phase continues from.
        self.offset = offset
        self.lineno = lineno
        self.good = good
        self.rewritten = rewritten
        self.skipped = skipped
        self.stage_name = stage_name
        self.dropped_local = set()
        self.dropped_count = 0
        self.salvaged = 0
        # Good/bad rows the convergence phase newly moves.
        self.salvage_good = []
        self.salvage_bad = []

    @property
    def added(self):
        return self.good - self.dropped_count + self.salvaged

    @property
    def will_stage(self):
        return self.stage_name is not None


_PREPARED_KEYS = ("base_offset", "base_line", "good", "rewritten",
                  "skipped", "stage")


def _write_delta_prepared(member, stage_name, dropped_lines=()):
    _write_linked_prepared(member.member_dir, {
        "dirty": stage_name is not None,
        "offset": member.offset,
        "lineno": member.lineno,
        "base_offset": member.base_offset,
        "base_line": member.base_line,
        "good": member.good,
        "rewritten": member.rewritten,
        "skipped": member.skipped,
        "dropped": member.dropped_count,
        "dropped_lines": sorted(dropped_lines),
        "stage": stage_name,
    })


def _resume_delta_prepared(index, group_index, local_index, path,
                           member_dir, baseline):
    """Fast path: a delta member fully prepared before a kill."""
    info = _read_linked_prepared(member_dir)
    if info is None or any(k not in info for k in _PREPARED_KEYS):
        return None
    if int(info["base_offset"]) != baseline["offset"] \
            or int(info["base_line"]) != baseline["lines"]:
        # Anchored at a different baseline: redo the member.
        shutil.rmtree(member_dir, ignore_errors=True)
        return None
    inode, _mode, _records, _good = _read_checkpoint_log(member_dir)
    try:
        if inode != os.stat(path).st_ino:
            shutil.rmtree(member_dir, ignore_errors=True)
            return None
    except OSError:
        return None
    stage = info["stage"]
    if stage is not None and not os.path.isfile(
        os.path.join(member_dir, stage)
    ):
        return None
    try:
        src = open(path, "rb")
    except OSError:
        return None
    member = _DeltaMember(
        index, group_index, local_index, path, member_dir, src,
        int(info["base_offset"]), int(info["base_line"]),
        int(info["offset"]), int(info["lineno"]),
        int(info["good"]), int(info["rewritten"]), int(info["skipped"]),
        stage,
    )
    member.dropped_count = int(info.get("dropped", 0))
    member.dropped_local = set(info.get("dropped_lines", ()))
    member.filtered = stage is not None
    return member


def _recovered_rewritten(path, member_dir):
    """Rewritten count for the durable index prefix (checkpoint resume).

    The migrated line is a deterministic pure function of the raw line,
    so the changed flag is re-derived at the recorded offsets without
    rescanning anything.
    """
    count = 0
    with open(path, "rb") as src:
        for _j, offset, kind, _proj in _stream_line_entries(member_dir):
            if kind != LINE_GOOD:
                continue
            src.seek(offset)
            raw = src.readline()
            from .log_migration import _convert
            if raw != _convert(raw):
                count += 1
    return count


def _prepare_delta_member(index, group_index, local_index, path,
                          member_dir, baseline, on_bad, segment_size,
                          quiesce, audit_stream):
    """Scan only the new suffix of one member into delta work state.

    Checkpoints hold absolute source offsets (the scan starts at the
    ledger's byte boundary) while good/skipped counts are local to the
    new suffix.  Strict mode defers bad rows into the line index (never
    audited, never raised) so the run attributes the globally first new
    problem afterwards, exactly like a full linked prepare.
    """
    base_offset = baseline["offset"]
    base_line = baseline["lines"]
    current_inode = os.stat(path).st_ino
    state = _prepare_workdir(
        path, member_dir, segment_size, current_inode, on_bad
    )
    # _prepare_workdir resumes ANY checkpoint naming this inode and
    # policy, including one a previous, already committed delta left
    # when its best-effort sweep failed.  This run's progress always
    # sits strictly past the ledger boundary; a checkpoint at or before
    # it belongs to a previous run -- restart the member.
    if state["resumed"] and (
        state["offset"] < base_offset
        or (state["offset"] == base_offset
            and (state["count"] or state["skipped"]))
    ):
        shutil.rmtree(member_dir, ignore_errors=True)
        state = _prepare_workdir(
            path, member_dir, segment_size, current_inode, on_bad
        )
    writer = state["writer"]
    cp_fh = state["cp_fh"]
    offset = max(state["offset"], base_offset)
    good = state["count"]
    skipped = state["skipped"]
    lineno = base_line + good + skipped
    follow_window = max(1.0, quiesce * 20)
    line_index = _LineIndex(member_dir)
    line_index.begin(good + skipped, state["resumed"])
    rewritten = (_recovered_rewritten(path, member_dir)
                 if state["resumed"] else 0)

    def note_bad(raw, line_start):
        nonlocal skipped
        skipped += 1
        line_index.record(line_start, _classify_bad_line(raw), raw)

    def handle(raw):
        """Return emitted bytes, or None for a (deferred/skipped) bad row."""
        nonlocal rewritten
        try:
            out, bad = _emit(audit_stream, on_bad, raw, lineno)
        except BadRecordError:
            return None, True
        if bad:
            return None, True
        if raw != out:
            rewritten += 1
        return out, False

    src = open(path, "rb")
    try:
        # --- SCAN the new suffix with the standard tailing policy.
        for raw in _read_lines(src, quiesce, offset=offset,
                               follow=follow_window):
            lineno += 1
            line_start = offset
            offset += len(raw)
            out, bad = handle(raw)
            if bad:
                note_bad(raw, line_start)
                continue
            line_index.record(line_start, LINE_GOOD, out)
            writer.write(out)
            good += 1
            if writer.size >= writer.limit:
                name = writer.current_segment
                writer.close()
                line_index.sync()
                _publish_prefix_checkpoint(
                    cp_fh, member_dir, offset, good, skipped,
                    rewritten > 0 or skipped > 0, name,
                    os.path.getsize(os.path.join(member_dir, name)),
                )

        if writer.has_open_segment:
            name = writer.current_segment
            writer.close()
            line_index.sync()
            _publish_prefix_checkpoint(
                cp_fh, member_dir, offset, good, skipped,
                rewritten > 0 or skipped > 0, name,
                os.path.getsize(os.path.join(member_dir, name)),
            )
    except BaseException:
        writer.abort()
        cp_fh.close()
        src.close()
        line_index.close()
        raise
    writer.abort()
    cp_fh.close()

    # --- ASSEMBLE the suffix, then DRAIN the racing tail.  The
    # assembled ``final`` holds one migrated line per GOOD index entry
    # (canonical rows reproduce their raw bytes verbatim), so the
    # candidate builder always reads suffix rows from it.
    segments = sorted(
        n for n in os.listdir(member_dir) if n.startswith("seg-")
    )
    final = _assemble(member_dir, segments, os.path.join(member_dir, _FINAL))
    try:
        with open(final, "ab") as out_fh:
            for raw in _read_lines(src, quiesce, offset=offset,
                                   follow=follow_window):
                lineno += 1
                line_start = offset
                offset += len(raw)
                out, bad = handle(raw)
                if bad:
                    note_bad(raw, line_start)
                    continue
                line_index.record(line_start, LINE_GOOD, out)
                out_fh.write(out)
                good += 1
            out_fh.flush()
            os.fsync(out_fh.fileno())
    except BaseException:
        src.close()
        line_index.close()
        raise
    line_index.sync()
    line_index.close()

    member = _DeltaMember(
        index, group_index, local_index, path, member_dir, src,
        base_offset, base_line, offset, lineno, good, rewritten,
        skipped, None,
    )
    _write_delta_prepared(member, None)
    return member


# ---------------------------------------------------------------------------
# Candidate output: prefix verbatim + migrated suffix minus dropped rows
# ---------------------------------------------------------------------------


def _dropped_rewritten(member):
    """How many dropped suffix rows would have been rewritten.

    Only the (normally few) dropped rows are converted; surviving rows'
    changed flags were accumulated during prepare and stay untouched.
    """
    from .log_migration import _convert
    count = 0
    with open(member.path, "rb") as src:
        for j, offset, kind, _proj in _stream_line_entries(member.member_dir):
            if kind != LINE_GOOD or j not in member.dropped_local:
                continue
            src.seek(offset)
            raw = src.readline()
            if raw != _convert(raw):
                count += 1
    return count


def _build_delta_candidate(member, dropped_local):
    """Build prefix verbatim + migrated suffix minus dropped rows.

    Called only when the member is known to change (at least one
    rewritten row, skipped bad row or dropped row).  Returns the staged
    file name: ``linked-final`` when rows are dropped, else ``final``.
    Only complete lines are emitted; a record caught mid-append at the
    prepared boundary is excluded, exactly as a full migration's staged
    file -- post-commit convergence drains its completion from the
    pinned source inode (or from the new path a re-opening writer uses).
    """
    member_dir = member.member_dir
    name = _LINKED_FINAL if dropped_local else _FINAL
    candidate = os.path.join(member_dir, _CANDIDATE_TMP)
    with open(member.path, "rb") as src, open(candidate, "wb") as out, \
            open(os.path.join(member_dir, _FINAL), "rb") as migrated:
        if member.base_offset:
            out.write(src.read(member.base_offset))
        for j, _off, kind, _proj in _stream_line_entries(member_dir):
            if kind != LINE_GOOD:
                continue
            # Read one migrated line per GOOD entry (the final holds one
            # such line per GOOD index record); only then may it be
            # dropped, otherwise the read/write alignment shifts.
            line = migrated.readline()
            if j not in dropped_local:
                out.write(line)
        out.flush()
        os.fsync(out.fileno())
    os.replace(candidate, os.path.join(member_dir, name))
    _fsync_dir(member_dir)
    return name


# ---------------------------------------------------------------------------
# Reference index over ledger prefix + new suffix
# ---------------------------------------------------------------------------


def _add_ledger_prefix(refs, member, entry, dst_links):
    """Index the reconciled prefix: target nodes/keys only, never edges."""
    db = refs._db
    cur = db.cursor()
    keys, skips, badvs = [], [], []

    def flush():
        if keys:
            cur.executemany(
                "INSERT INTO node_key(link_id, value, node) VALUES (?,?,?)",
                keys)
            keys.clear()
        if skips:
            cur.executemany(
                "INSERT INTO skipped_key(link_id, value) VALUES (?,?)",
                skips)
            skips.clear()
        if badvs:
            cur.executemany(
                "INSERT INTO badv_key(link_id, value) VALUES (?,?)",
                badvs)
            badvs.clear()

    for rec in entry["good"]:
        cur.execute("INSERT INTO nodes(mi, lineno) VALUES (?,?)",
                    (member.index, int(rec["lineno"])))
        node_id = cur.lastrowid
        for link_id, field in dst_links:
            value = rec.get("keys", {}).get(field)
            if value is not None:
                keys.append((link_id, value, node_id))

    def key_rows(records, sink):
        for rec in records:
            for _link_id, field in dst_links:
                value = rec.get("keys", {}).get(field)
                if value is not None:
                    sink.append((link_id, value))

    key_rows(entry["skipped"], skips)
    key_rows(entry["badv"], badvs)
    flush()
    db.commit()


def _add_delta_suffix(refs, member, dst_links, src_links):
    """Index new rows with global line numbers and fresh=1 edges."""
    db = refs._db
    cur = db.cursor()
    keys, skips, badvs, edge_rows = [], [], [], []

    def flush():
        if keys:
            cur.executemany(
                "INSERT INTO node_key(link_id, value, node) VALUES (?,?,?)",
                keys)
            keys.clear()
        if skips:
            cur.executemany(
                "INSERT INTO skipped_key(link_id, value) VALUES (?,?)",
                skips)
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

    base_line = member.base_line
    for j, _offset, kind, proj in _stream_line_entries(member.member_dir):
        lineno = base_line + j
        if kind == LINE_GOOD:
            cur.execute("INSERT INTO nodes(mi, lineno) VALUES (?,?)",
                        (member.index, lineno))
            node_id = cur.lastrowid
            for link_id, field in dst_links:
                value = proj.get(field)
                if value is not None:
                    keys.append((link_id, value, node_id))
            for link_id, field in src_links:
                value = proj.get(field)
                if value is not None:
                    edge_rows.append((node_id, link_id, value, 1))
        else:
            sink = badvs if kind == LINE_BAD_VERSION else skips
            for link_id, field in dst_links:
                value = proj.get(field)
                if value is not None:
                    sink.append((link_id, value))
        if len(keys) + len(skips) + len(badvs) + len(edge_rows) >= _BATCH:
            flush()
    flush()
    db.commit()


def _raise_first_error(members, links, first):
    """Strict mode: raise the globally first problem of the new suffix.

    Bad lines were deferred during prepare; their index line numbers are
    suffix-local, so they are shifted by the member's prefix line count
    before the group/member/line comparison against bad references
    (whose resolver line numbers are already global).
    """
    bad_line = None
    for member in members:
        for lineno_local, offset, kind, _proj in _stream_line_entries(
                member.member_dir):
            if kind != LINE_GOOD:
                bad_line = (member, member.base_line + lineno_local,
                            offset)
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
        from .log_migration import _convert
        try:
            _convert(raw)
            cause = ValueError("undecodable record")
        except ValueError as exc:
            cause = exc
        raise GroupBadRecordError(member.path, lineno0 + 1, raw, cause)
    if first is not None:
        mi, lineno0, reason, link_id, value = first
        member = members[mi]
        _src_g, dst_g, _src_field, dst_field = links[link_id]
        local = lineno0 - member.base_line
        raw = b""
        for j, offset, _kind, _proj in _stream_line_entries(member.member_dir):
            if j == local:
                with open(member.path, "rb") as f:
                    f.seek(offset)
                    raw = f.readline()
                break
        raise LinkedBadReferenceError(
            member.path, lineno0 + 1, raw,
            f"{_REASON_TEXT[reason]}: {dst_field}={value!r} "
            f"in group {dst_g}",
        )


# ---------------------------------------------------------------------------
# Two-phase rename
# ---------------------------------------------------------------------------


def _stage_delta_member(member):
    path = member.path
    parent = os.path.dirname(os.path.abspath(path))
    d = _linked_debris(path)
    os.replace(os.path.join(member.member_dir, member.stage_name),
               d["staged"])
    _publish_member_marker(path)
    os.replace(path, d["backup"])
    os.replace(d["staged"], path)
    _fsync_dir(parent)


def _audit_delta_dropped(member, audit_stream):
    """One ``<file>:<lineno>:<first 32 bytes>`` entry per dropped new row."""
    from .log_migration import _audit_line
    prefix = _AuditPrefix(audit_stream, member.path)
    with open(member.path, "rb") as src:
        for j, offset, kind, _proj in _stream_line_entries(member.member_dir):
            if kind != LINE_GOOD or j not in member.dropped_local:
                continue
            src.seek(offset)
            prefix.write(_audit_line(member.base_line + j + 1,
                                     src.readline()))
    audit_stream.flush()


# ---------------------------------------------------------------------------
# Ledger publication
# ---------------------------------------------------------------------------


def _project_index_keys(proj, dst_links):
    return {field: proj[field] for _lid, field in dst_links
            if field in proj}


def _classify_bad_keys(raw, dst_links):
    kind = _classify_bad_line(raw)
    try:
        obj = json.loads(raw.decode("utf-8"))
    except ValueError:
        obj = None
    if not isinstance(obj, dict):
        obj = {}
    keys = {field: obj[field] for _lid, field in dst_links
            if field in obj and isinstance(obj[field], str)}
    return kind, keys


def _member_updates(member, dst_fields_links):
    """Next-ledger records for one member from its index + salvage rows.

    Good rows are numbered densely over committed lines (dropped and
    skipped rows are absent).  Rows dropped for bad references are
    excluded from the targets on purpose: they are absent from the
    committed file, so a later reference to their key is dangling.
    """
    good, bad = [], []
    lineno = member.base_line
    for _j, _off, kind, proj in _stream_line_entries(member.member_dir):
        keys = _project_index_keys(proj, dst_fields_links)
        if kind == LINE_GOOD:
            if _j in member.dropped_local:
                continue
            good.append({"lineno": lineno, "keys": keys})
            lineno += 1
        else:
            bad.append((kind, keys))
    for obj in member.salvage_good:
        keys = {field: obj[field] for _lid, field in dst_fields_links
                if field in obj}
        good.append({"lineno": lineno, "keys": keys})
        lineno += 1
    bad.extend(member.salvage_bad)
    return {"good": good, "bad": bad}


def _publish_ledger(group_dir, groups, links, on_bad, paths, dst_links,
                    old_bases, updates):
    """Publish the next ledger by extending the previous one.

    The reconciled prefix's target records are reused verbatim (the
    prefix is never decoded again); only the per-member suffix updates
    are folded in.  The claimed byte boundary is the committed file's
    first ``old lines + new surviving lines`` complete lines, so whole
    rows a writer lands after convergence stay for the next delta
    instead of being claimed unvalidated.
    """
    gi_by_path = {}
    for gi, group in enumerate(groups):
        for path in group:
            gi_by_path[os.path.abspath(path)] = gi
    entries = {}
    for path in paths:
        ap = os.path.abspath(path)
        old = old_bases.get(path) or _empty_baseline()
        upd = updates.get(path) or {"good": [], "bad": []}
        claimed_lines = old["lines"] + len(upd["good"])
        prefix_len = 0
        digest = hashlib.sha256()
        with open(path, "rb") as f:
            for _ in range(claimed_lines):
                line = f.readline()
                if not line.endswith(b"\n"):
                    break
                prefix_len += len(line)
                digest.update(line)
        entries[ap] = {
            "inode": os.stat(path).st_ino,
            "offset": prefix_len,
            "lines": claimed_lines,
            "prefix_sha256": digest.hexdigest(),
            "good": list(old["good"]) + list(upd["good"]),
            "skipped": list(old["skipped"]) + [
                {"lineno": -1, "keys": keys}
                for kind, keys in upd["bad"]
                if kind != LINE_BAD_VERSION],
            "badv": list(old["badv"]) + [
                {"lineno": -1, "keys": keys}
                for kind, keys in upd["bad"]
                if kind == LINE_BAD_VERSION],
        }
    ledger = {
        "version": 1,
        "on_bad": on_bad,
        "groups": [[os.path.abspath(p) for p in g] for g in groups],
        "links": [list(l) for l in links],
        "members": entries,
    }
    _write_json_atomic(_ledger_path(group_dir), ledger)


# ---------------------------------------------------------------------------
# Post-border finishing (rerun with the commit marker present)
# ---------------------------------------------------------------------------


def _entry_good_slice(path, first, last, dst_fields_links):
    """Target records of committed lines [first, last), densely numbered.

    Used only by the post-crash finishing path, which decodes the suffix
    the crashed run staged plus the rows the continuation moves; the
    reconciled prefix is never read here.
    """
    out = []
    with open(path, "rb") as f:
        lineno = 0
        while True:
            raw = f.readline()
            if not raw.endswith(b"\n"):
                break
            if first <= lineno < last:
                obj = loads(raw)
                keys = {field: obj[field] for _lid, field in dst_fields_links
                        if field in obj}
                out.append({"lineno": lineno, "keys": keys})
            lineno += 1
            if lineno >= last:
                break
    return out


def _newline_count(path):
    total = 0
    with open(path, "rb") as f:
        while True:
            chunk = f.read(1 << 20)
            if not chunk:
                return total
            total += chunk.count(b"\n")


def _finish_committed_delta(paths, groups, on_bad, quiesce, audit_stream,
                            dst_links, old_bases):
    """Finish a delta run whose commit marker already exists.

    The border was crossed, so every fault here is a warning.  Members
    the kill caught mid phase-1 are promoted from their surviving
    candidate; rows still living on a backup inode are spliced in before
    that backup is removed, and tails racing writers keep landing are
    converged.  Every good/bad row the continuation newly moves is
    observed through the convergence sinks, so the summary counts only
    this continuation's rows.  Returns
    ``(results, salvaged_total, warnings, blocked, updates)``.
    """
    warnings = []
    results = {}
    updates = {}
    salvaged_total = 0
    blocked = set()

    group_of = {}
    for gi, group in enumerate(groups):
        for li, path in enumerate(group):
            group_of[path] = (gi, li)

    for path in paths:
        gi, li = group_of[path]
        parent = os.path.dirname(os.path.abspath(path))
        d = _linked_debris(path)
        member_dir = _delta_member_dir(path)
        fields_links = dst_links.get(gi, [])
        added = changed = skipped = 0
        extra_gens = []
        bad_records = []
        info = _read_linked_prepared(member_dir)

        # Durable classification of the crashed run's suffix.
        crashed_bad = []
        crashed_surviving = 0
        if info is not None:
            try:
                dropped_lines = set(info.get("dropped_lines", ()))
                good_seen = 0
                for j, _off, kind, proj in _stream_line_entries(member_dir):
                    if kind == LINE_GOOD:
                        if j not in dropped_lines:
                            good_seen += 1
                    else:
                        crashed_bad.append(
                            (kind, _project_index_keys(proj, fields_links)))
                crashed_surviving = good_seen
            except OSError:
                crashed_bad = []

        def on_bad_row(raw):
            bad_records.append(_classify_bad_keys(raw, fields_links))

        try:
            # 1) A member the kill caught before its first rename still
            #    holds its candidate in the work directory: stage it.
            if info is not None and info.get("stage") \
                    and not os.path.exists(d["backup"]) \
                    and not os.path.exists(d["staged"]):
                candidate = os.path.join(member_dir, info["stage"])
                if os.path.isfile(candidate):
                    os.replace(candidate, d["staged"])
                    _publish_member_marker(path)
                    os.replace(path, d["backup"])
                    os.replace(d["staged"], path)
                    _fsync_dir(parent)

            # 2) Converge any surviving backup (rows existing nowhere
            #    else) before it is removed.
            if os.path.exists(d["backup"]) and info is not None:
                moved_box = [0]

                def on_moved_backup(raw, out, moved_box=moved_box):
                    moved_box[0] += 1
                    if raw != out:
                        member_changed[0] += 1

                member_changed = [0]
                salv, skip, gens, complete = _salvage_backup_tail(
                    path, d["backup"], int(info["offset"]),
                    int(info["lineno"]), on_bad,
                    _AuditPrefix(audit_stream, path), member_dir,
                    quiesce, moved_sink=on_moved_backup,
                    bad_sink=on_bad_row,
                )
                added += salv
                changed += member_changed[0]
                skipped += skip
                salvaged_total += salv
                extra_gens.extend(gens)
                if not complete:
                    warnings.append(
                        f"{path}: backup still receiving appends at the "
                        f"backstop; rerun to finish")
                    for fd, _off in extra_gens:
                        try:
                            fd.close()
                        except OSError:
                            pass
                    blocked.add(path)
                    results[path] = DeltaMemberResult(
                        path, gi, li, added, changed, skipped, 0, False)
                    continue
                _remove(d["backup"])
                _fsync_dir(parent)

            # 3) Drain the current inode's tail (old-format rows a
            #    path-reopening appender landed after the crash included).
            cur = open(path, "rb")
            start_off = os.fstat(cur.fileno()).st_size
            start_lineno = _newline_count(path)
            tail_changed = [0]

            def on_moved_tail(raw, out, tail_changed=tail_changed):
                if raw != out:
                    tail_changed[0] += 1

            try:
                salvaged, conv_skipped, _lineno, warning = _converge(
                    path, parent, member_dir, cur, start_off,
                    start_lineno, quiesce,
                    _AuditPrefix(audit_stream, path), on_bad,
                    extra_gens=extra_gens,
                    moved_sink=on_moved_tail, bad_sink=on_bad_row,
                )
            finally:
                cur.close()
                for fd, _off in extra_gens:
                    try:
                        fd.close()
                    except OSError:
                        pass
            added += salvaged
            changed += tail_changed[0]
            skipped += conv_skipped
            salvaged_total += salvaged
            if warning:
                warnings.append(f"{path}: {warning}")

            # 4) Next-ledger records: surviving crashed suffix plus the
            #    rows this continuation moved, read densely from the
            #    committed file; bad keys are the crashed run's plus
            #    this continuation's.  When the baseline itself had to
            #    be rebuilt from the committed file (a lost ledger), it
            #    already covers the crashed suffix, so only the rows this
            #    continuation moves are new.
            old_base = old_bases.get(path) or _empty_baseline()
            old_lines = old_base["lines"]
            if old_base.get("_rebuilt"):
                crashed_surviving = 0
                crashed_bad = []
            claimed = crashed_surviving + added
            updates[path] = {
                "good": _entry_good_slice(
                    path, old_lines, old_lines + claimed, fields_links),
                "bad": list(crashed_bad) + list(bad_records),
            }
        except (OSError, ValueError) as exc:
            warnings.append(
                f"{path}: post-commit finishing incomplete, rerun to "
                f"finish: {exc}")
            blocked.add(path)
        results[path] = DeltaMemberResult(
            path, gi, li, added, changed, skipped, 0, False)
    return results, salvaged_total, warnings, blocked, updates


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


def migrate_linked_delta(groups, *, links=(), on_bad="strict",
                         segment_size=DEFAULT_SEGMENT_SIZE,
                         quiesce=DEFAULT_QUIESCE, audit=None):
    """Migrate only the rows appended since the last linked migration.

    The baseline is the local reconciliation ledger the previous full or
    delta migration left next to the first member.  Only the new suffix
    of every member is scanned and resolved; the reconciled prefix is
    neither rescanned nor rewritten.  Per member the result gives the
    four reconciliation counters -- added, rewritten, skipped, dropped
    bad-reference rows -- and the final files are byte-for-byte identical
    to running :func:`migrate_linked_logs` over the same input.

    When the ledger is missing, corrupt, names another configuration, or
    a source inode changed, the run deterministically migrates the whole
    group set (an empty baseline through the same engine) and the result
    records ``fallback=True`` with the reason.  Leases, crash resume,
    strict first-error ordering, the validation taxonomy and the CLI exit
    codes are exactly the full linked run's.
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
    salvaged_total = 0
    post_commit_error = None
    fallback = False
    fallback_reason = None

    with _member_leases(paths):
        try:
            _crash_point("delta-lock")
            os.makedirs(group_dir, exist_ok=True)
            committed = os.path.exists(
                os.path.join(group_dir, _LINKED_COMMITTED))

            # Resolve any interrupted renames first: a kill mid
            # phase-1 left a member renamed though no marker exists, so
            # the original inode is restored before the ledger validates
            # inodes; with the marker present renames are completed.
            for path in paths:
                _reconcile_linked_member(path, committed)

            if committed:
                # Post-border resume: the staged renames changed member
                # inodes on purpose, so the ledger's inode/prefix checks
                # are relaxed here; the commit marker is the authority.
                bases, _incremental, _reason = _load_ledger(
                    group_dir, groups, links, on_bad, paths, lenient=True)
                fallback = False
                fallback_reason = None
            else:
                bases, incremental, fallback_reason = _load_ledger(
                    group_dir, groups, links, on_bad, paths)
                fallback = not incremental
                if not incremental:
                    # A missing/corrupt baseline must not reuse an
                    # earlier killed attempt's delta work dirs.
                    _sweep_delta_member_dirs(paths)

            if committed:
                by_path, salvaged_total, warnings, blocked, updates = (
                    _finish_committed_delta(
                        paths, groups, on_bad, quiesce, audit_stream,
                        dst_links, bases)
                )
                member_results = [by_path[p] for p in paths]
                if not blocked:
                    _publish_ledger(
                        group_dir, groups, links, on_bad, paths,
                        dst_links, bases, updates)
                    _sweep_delta_member_dirs(paths)
                    _remove(os.path.join(group_dir, _LINKED_COMMITTED))
                    _remove(os.path.join(group_dir, _DELTA_MANIFEST))
                    _remove(os.path.join(group_dir, _REFS_DB))
                    _fsync_dir(group_dir)
                return DeltaMigrationResult(
                    groups=tuple(tuple(g) for g in groups),
                    members=tuple(member_results),
                    records_added=sum(m.lines_added for m in member_results),
                    records_rewritten=sum(
                        m.lines_rewritten for m in member_results),
                    records_skipped=sum(
                        m.lines_skipped for m in member_results),
                    references_dropped=sum(
                        m.references_dropped for m in member_results),
                    records_salvaged=salvaged_total,
                    replaced=any(m.replaced for m in member_results),
                    fallback=fallback,
                    fallback_reason=fallback_reason,
                    post_commit_error="; ".join(warnings) or None,
                )

            _write_json_atomic(
                os.path.join(group_dir, _DELTA_MANIFEST),
                {"version": 1, "on_bad": on_bad,
                 "groups": [[os.path.abspath(p) for p in g] for g in groups],
                 "links": [list(l) for l in links]})
            _remove(os.path.join(group_dir, _LINKED_COMMITTED_TMP))

            try:
                # --- PREPARE only the new suffix of every member.
                index = 0
                for group_index, group in enumerate(groups):
                    for local_index, path in enumerate(group):
                        member_dir = _delta_member_dir(path)
                        member = _resume_delta_prepared(
                            index, group_index, local_index, path,
                            member_dir, bases[path])
                        if member is None:
                            member = _prepare_delta_member(
                                index, group_index, local_index, path,
                                member_dir, bases[path], on_bad,
                                segment_size, quiesce,
                                _AuditPrefix(audit_stream, path))
                        members.append(member)
                        index += 1
                _crash_point("delta-prepare")

                # --- RESOLVE references over prefix + new suffix.
                refs_path = os.path.join(group_dir, _REFS_DB)
                _remove(refs_path)
                refs = _RefsDb(refs_path)
                try:
                    for member in members:
                        _add_ledger_prefix(
                            refs, member, bases[member.path],
                            dst_links.get(member.group_index, []))
                        _add_delta_suffix(
                            refs, member,
                            dst_links.get(member.group_index, []),
                            src_links.get(member.group_index, []))
                    refs.validate()
                    _crash_point("delta-refs")
                    dropped_global = refs.dropped_lines()
                    first = refs.first_bad()
                finally:
                    refs.close()

                if on_bad == "strict":
                    _raise_first_error(members, links, first)

                # --- FILTER bad-reference rows out of the candidates.
                for member in members:
                    if getattr(member, "filtered", False):
                        # A kill after this member's candidate was built
                        # (and its audits emitted) left the complete
                        # candidate behind; resolution is deterministic,
                        # so reuse it verbatim -- never rebuild (which
                        # would double the prefix) or re-audit.
                        continue
                    drop_global = dropped_global.get(member.index, set())
                    drop_local = {g - member.base_line for g in drop_global}
                    member.dropped_local = drop_local
                    member.dropped_count = len(drop_local)
                    if drop_local:
                        _audit_delta_dropped(member, audit_stream)
                        # Rewritten counts only rows that survive;
                        # subtract the dropped rows that would otherwise
                        # have changed encoding.
                        member.rewritten -= _dropped_rewritten(member)
                    # Bytes change exactly when the suffix has a
                    # rewritten row, a skipped bad row or a dropped row;
                    # canonical appends alone leave the file untouched.
                    changed = (member.rewritten > 0 or member.skipped > 0
                               or member.dropped_count > 0)
                    stage_name = (
                        _build_delta_candidate(member, drop_local)
                        if changed else None)
                    member.stage_name = stage_name
                    _write_delta_prepared(member, stage_name,
                                          dropped_lines=drop_local)
                _crash_point("delta-filter")

                staged = [m for m in members if m.will_stage]

                # --- PHASE 1: stage every changed member.
                done = []
                if staged:
                    try:
                        for member in staged:
                            _stage_delta_member(member)
                            done.append(member)
                            _crash_point("delta-stage")
                        _crash_point("delta-staged")
                    except BaseException:
                        if not _undo_staged_members(done):
                            _sweep_delta_member_dirs(paths)
                        raise

                    # --- COMMIT POINT: publish the delta marker.
                    marker = os.path.join(group_dir, _LINKED_COMMITTED)
                    marker_tmp = os.path.join(group_dir,
                                              _LINKED_COMMITTED_TMP)
                    with open(marker_tmp, "wb") as f:
                        f.write(b"1\n")
                        f.flush()
                        os.fsync(f.fileno())
                    os.replace(marker_tmp, marker)
                    _fsync_dir(group_dir)
                    _crash_point("delta-marker")

                # --- PHASE 2: converge racing appenders on staged
                # members (canonical-only members hold no backup and
                # have nothing to converge).
                warnings = []
                for member in staged:
                    tail_changed = [0]

                    def on_moved(raw, out, member=member,
                                 tail_changed=tail_changed):
                        member.salvage_good.append(loads(out))
                        if raw != out:
                            tail_changed[0] += 1

                    def on_bad_salvage(raw, member=member):
                        member.salvage_bad.append(
                            _classify_bad_keys(
                                raw,
                                dst_links.get(member.group_index, [])))

                    try:
                        salvaged, conv_skipped, _lineno, warning = _converge(
                            member.path,
                            os.path.dirname(os.path.abspath(member.path)),
                            member.member_dir, member.src, member.offset,
                            member.lineno, quiesce,
                            _AuditPrefix(audit_stream, member.path),
                            on_bad, moved_sink=on_moved,
                            bad_sink=on_bad_salvage,
                        )
                    except (OSError, ValueError) as exc:
                        warnings.append(
                            f"{member.path}: post-commit convergence "
                            f"incomplete, rerun to finish: {exc}")
                        continue
                    member.salvaged = salvaged
                    member.rewritten += tail_changed[0]
                    member.skipped += conv_skipped
                    salvaged_total += salvaged
                    if warning:
                        warnings.append(f"{member.path}: {warning}")
                    try:
                        _remove(member.path + _BACKUP_SUFFIX)
                        _fsync_dir(
                            os.path.dirname(os.path.abspath(member.path)))
                    except OSError as exc:
                        warnings.append(
                            f"{member.path}: backup removal failed: {exc}")
                _crash_point("delta-converge")
                post_commit_error = "; ".join(warnings) or None

                # --- PUBLISH the next ledger over the committed files.
                updates = {
                    member.path: _member_updates(
                        member,
                        dst_links.get(member.group_index, []))
                    for member in members
                }
                _publish_ledger(
                    group_dir, groups, links, on_bad, paths,
                    dst_links, bases, updates)
                _crash_point("delta-ledger")
                _sweep_delta_member_dirs(paths)
                if staged:
                    _remove(os.path.join(group_dir, _LINKED_COMMITTED))
                _remove(os.path.join(group_dir, _DELTA_MANIFEST))
                _remove(os.path.join(group_dir, _REFS_DB))
                _fsync_dir(group_dir)
            except BaseException:
                # A handled pre-commit failure leaves no partial state;
                # a true kill never reaches here, so durable checkpoints
                # survive for the rerun.
                if not os.path.exists(
                        os.path.join(group_dir, _LINKED_COMMITTED)) \
                        and not any(os.path.exists(m.path + _BACKUP_SUFFIX)
                                    for m in members):
                    _sweep_delta_member_dirs(paths)
                raise
        finally:
            for member in members:
                try:
                    member.src.close()
                except OSError:
                    pass

    return _build_result(
        groups, members, salvaged_total,
        any(m.will_stage for m in members),
        fallback, fallback_reason, post_commit_error)


def _build_result(groups, members, salvaged_total, replaced, fallback,
                  fallback_reason, post_commit_error):
    member_results = tuple(
        DeltaMemberResult(
            path=m.path,
            group=m.group_index,
            member=m.local_index,
            lines_added=m.added,
            lines_rewritten=m.rewritten,
            lines_skipped=m.skipped,
            references_dropped=m.dropped_count,
            replaced=bool(replaced and m.will_stage),
        )
        for m in members
    )
    return DeltaMigrationResult(
        groups=tuple(tuple(g) for g in groups),
        members=member_results,
        records_added=sum(r.lines_added for r in member_results),
        records_rewritten=sum(r.lines_rewritten for r in member_results),
        records_skipped=sum(r.lines_skipped for r in member_results),
        references_dropped=sum(r.references_dropped for r in member_results),
        records_salvaged=salvaged_total,
        replaced=replaced,
        fallback=fallback,
        fallback_reason=fallback_reason,
        post_commit_error=post_commit_error,
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def run_delta_cli(argv):
    parser = argparse.ArgumentParser(
        prog="python3 -m proto_migrate delta-migrate-linked-logs",
        description="Migrate only the rows appended since the last "
        "linked migration, reconciled against a local ledger; falls "
        "back to a full migration when the ledger is unusable.",
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
                      "bad-reference rows, auditing each to stderr")
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
        result = migrate_linked_delta(
            args.group,
            links=links,
            on_bad=args.on_bad,
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
    for m in result.members:
        print(
            "member group=%d index=%d path=%s added=%d rewritten=%d "
            "skipped=%d refs_dropped=%d %s"
            % (m.group, m.member, m.path, m.lines_added,
               m.lines_rewritten, m.lines_skipped, m.references_dropped,
               "replaced" if m.replaced else "untouched")
        )
    print(
        "groups=%d members=%d added=%d rewritten=%d skipped=%d "
        "refs_dropped=%d salvaged=%d replaced=%s fallback=%s%s"
        % (
            len(result.groups),
            sum(len(g) for g in result.groups),
            result.records_added,
            result.records_rewritten,
            result.records_skipped,
            result.references_dropped,
            result.records_salvaged,
            "yes" if result.replaced else "no",
            "yes" if result.fallback else "no",
            ((" reason=%s" % result.fallback_reason)
             if result.fallback_reason else ""),
        )
    )
    if result.post_commit_error:
        print(f"warning: {result.post_commit_error}", file=sys.stderr)
    return EXIT_OK
