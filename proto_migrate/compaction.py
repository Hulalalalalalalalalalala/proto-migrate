"""Online compaction of linked-migration checkpoint and index state.

After a long log has been resumed through many checkpoints, each
member's work directory holds a long checkpoint log plus one segment
per rotation and a per-line index (``lines``) with one sizable record
per consumed source line.  :func:`compact_linked_state` folds that
state into a deterministic compact shape without changing a byte of
the migration outcome:

  * the checkpoint log collapses from one record per rotated segment
    to the original header plus a single record, and the rotated
    segments collapse to one assembled segment ``seg-compacted``.  The
    record still names exactly the consumed input offset, the
    migrated/skipped counts and the cumulative dirty flag, so resume
    offsets and summary counters are unchanged;
  * once reference resolution has decided which records survive, the
    per-member line indexes and the SQLite spill file are replaced by
    O(1)-per-member ``resolved.json`` markers (the filtered outputs
    themselves, ``final`` / ``linked-final``, already hold every
    surviving byte, so auxiliary per-line state is no longer needed).

Compaction is crash-recoverable and idempotent.  A run killed at any
point reruns without rescanning a member that finished prepare, and a
migration resumed from compacted state stages the very same
``final`` / ``linked-final`` bytes -- its output is byte-for-byte
identical to an uninterrupted run.

The resolution decision is published all-or-nothing: per-member
``resolved.json`` markers first, then the group marker
``resolved.json`` in the group work directory.  Only once the group
marker is durable do line indexes and the spill file become garbage;
a crash before that publication leaves every line index in place and
the next run simply starts over.

Like everything else here, compaction state is local files only, takes
the same non-blocking member leases a migration does (so it never runs
against a live instance), and can always be rebuilt.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import NamedTuple

from .log_migration import (
    DEFAULT_QUIESCE,
    DEFAULT_SEGMENT_SIZE,
    EXIT_BAD_RECORD,
    EXIT_ERROR,
    EXIT_OK,
    EXIT_USAGE,
    LINE_GOOD,
    _audit_line,
    _convert,
    _fsync_dir,
    _read_checkpoint_log,
)
from .linked_migration import (
    _FINAL,
    _LINKED_COMMITTED,
    _LINKED_FINAL,
    _LINES_NAME,
    _REASON_TEXT,
    _RefsDb,
    _linked_group_dir,
    _linked_member_dir,
    _member_leases,
    _normalize_groups,
    _normalize_links,
    _prepare_linked_member,
    _probe_members,
    _reconcile_linked_member,
    _remove,
    _resume_linked_prepared,
    _stream_line_entries,
    _write_linked_final,
    _write_linked_manifest,
)

__all__ = [
    "CompactResult",
    "compact_linked_state",
    "compact_workdir_checkpoint",
    "run_compact_cli",
]

_RESOLVED_GROUP = "resolved.json"
_RESOLVED_MEMBER = "resolved.json"
# Skip-mode audit bytes are spooled per member instead of going to the
# caller's audit stream: bad-line audits during prepare land in
# ``bad-audit-spool`` and dropped-record audits at filter time in
# ``audit-spool``.  The real migration replays both when it resumes the
# resolved state, so the migration's audit output is byte-identical to
# an uninterrupted run.  Both files are rewritten on every compaction
# attempt and only consumed once the group resolved marker exists.
_BAD_AUDIT_SPOOL = "bad-audit-spool"
_AUDIT_SPOOL = "audit-spool"
_REF_SPOOL = "refs.sqlite3"
# The compacted image keeps the ordinary ``seg-NNNNNN`` naming so the
# existing checkpoint parser, segment sorter and resume index math all
# work unchanged; as the single first segment any tail resumed later
# chains as ``seg-000001`` and assembles in the right order.
_COMPACTED_SEG = "seg-000000"


class CompactResult(NamedTuple):
    groups: tuple
    members_compacted: int
    checkpoint_records_before: int
    checkpoint_records_after: int
    line_index_bytes_freed: int
    resolved: bool
    post_commit_error: str | None = None


# ---------------------------------------------------------------------------
# Checkpoint-log / segment compaction
# ---------------------------------------------------------------------------


def compact_workdir_checkpoint(member_dir):
    """Collapse one member work directory's checkpoint log and segments.

    The validated checkpoint prefix (``_read_checkpoint_log`` already
    discards torn tails and records backed by missing/short segments)
    is rewritten as the original header plus a single record naming one
    assembled segment ``seg-compacted``; the old rotated segments are
    deleted only after the new checkpoint is durably published.

    Each checkpoint record names one rotation segment and that
    segment's own durable length, so the compacted image is the
    concatenation of the named segments (each bounded by its recorded
    size) and the new record's size is their sum.

    Returns ``(records_before, records_after)``.  A checkpoint with no
    records (a canonical member's bare header) reports ``(0, 0)`` and
    is left untouched.  The operation is idempotent and crash-safe: a
    half-written segment or checkpoint left by a kill is redone on the
    next call.
    """
    inode, mode, records, _good = _read_checkpoint_log(member_dir)
    if not records:
        return 0, 0
    cp_path = os.path.join(member_dir, "checkpoint")
    compact_seg = os.path.join(member_dir, _COMPACTED_SEG)

    if len(records) == 1 and records[0][4] == _COMPACTED_SEG:
        # Already compacted: sweep rotation segments an interrupted
        # cleanup may have left behind.
        on_disk = {
            n for n in os.listdir(member_dir)
            if n.startswith("seg-") and n != _COMPACTED_SEG
        }
        for name in on_disk:
            _remove(os.path.join(member_dir, name))
        if on_disk:
            _fsync_dir(member_dir)
        return 1, 1

    # Assemble the validated segments into one compacted segment.  The
    # file goes in via a temp name + atomic rename, so a kill never
    # leaves a half-written seg-compacted that recovery could trust.
    with open(compact_seg + ".tmp", "wb") as out:
        for _off, _c, _s, _d, name, size in records:
            with open(os.path.join(member_dir, name), "rb") as part:
                remaining = size
                while remaining:
                    chunk = part.read(min(1 << 20, remaining))
                    if not chunk:
                        break
                    out.write(chunk)
                    remaining -= len(chunk)
        out.flush()
        os.fsync(out.fileno())
    os.replace(compact_seg + ".tmp", compact_seg)
    _fsync_dir(member_dir)

    off, count, skipped, dirty, _last, _last_size = records[-1]
    total_size = sum(size for *_pre, name, size in records)
    cp_tmp = os.path.join(member_dir, "checkpoint.compact-tmp")
    with open(cp_tmp, "wb") as f:
        f.write(f"src {inode} {mode}\n".encode("ascii"))
        f.write(
            f"{off} {count} {skipped} {1 if dirty else 0} "
            f"{_COMPACTED_SEG} {total_size}\n".encode("ascii")
        )
        f.flush()
        os.fsync(f.fileno())
    os.replace(cp_tmp, cp_path)
    _fsync_dir(member_dir)

    for _off, _c, _s, _d, name, _size in records:
        if name != _COMPACTED_SEG:
            _remove(os.path.join(member_dir, name))
    _fsync_dir(member_dir)
    return len(records), 1


# ---------------------------------------------------------------------------
# Resolved markers
# ---------------------------------------------------------------------------


def group_resolved_path(group_dir):
    return os.path.join(group_dir, _RESOLVED_GROUP)


def member_resolved_path(member_dir):
    return os.path.join(member_dir, _RESOLVED_MEMBER)


def _write_json_atomic(path, data):
    with open(path + ".tmp", "wb") as f:
        f.write(json.dumps(data, sort_keys=True).encode("ascii"))
        f.flush()
        os.fsync(f.fileno())
    os.replace(path + ".tmp", path)
    _fsync_dir(os.path.dirname(path))


def read_resolved_json(path):
    try:
        with open(path, "rb") as f:
            return json.loads(f.read())
    except (OSError, ValueError):
        return None


def _link_maps(links):
    dst_links = {}
    src_links = {}
    for link_id, (src_g, dst_g, src_field, dst_field) in enumerate(links):
        dst_links.setdefault(dst_g, []).append((link_id, dst_field))
        src_links.setdefault(src_g, []).append((link_id, src_field))
    return dst_links, src_links


def _spool_member_audits(member, bad_path, drop_path, on_bad):
    """Rebuild both skip-mode audit spools from the durable line index.

    The index is the durable source of truth for which consumed lines
    were bad (kind != GOOD) and which good records are dropped for bad
    references, so rebuilding the spools every attempt makes
    compaction idempotent -- a retried prepare never duplicates an
    audit byte.  Format is the real audit stream's:
    ``<path>:<lineno>:<first 32 raw bytes>``.
    """
    prefix = os.fspath(member.path).encode("utf-8", "surrogateescape")

    def write_spool(path, predicate):
        with open(member.path, "rb") as src, open(path, "wb") as out:
            for lineno0, offset, kind, _proj in _stream_line_entries(
                member.member_dir
            ):
                if not predicate(lineno0, kind):
                    continue
                src.seek(offset)
                raw = src.readline()
                out.write(prefix + b":" + _audit_line(lineno0 + 1, raw))
            out.flush()
            os.fsync(out.fileno())

    if on_bad == "skip":
        write_spool(bad_path, lambda _l, kind: kind != LINE_GOOD)
        write_spool(
            drop_path,
            lambda lineno0, kind: kind == LINE_GOOD
            and lineno0 in member.dropped,
        )


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


def _raise_global_first_error(members, links, first, on_bad):
    """Strict mode: raise the same error the real migration would."""
    from .group_migration import GroupBadRecordError
    from .linked_migration import (
        LinkedBadReferenceError,
        _first_indexed_bad_line,
        _read_source_line,
    )

    bad_line = None
    for member in members:
        hit = _first_indexed_bad_line(member)
        if hit is not None:
            bad_line = (member, hit[0], hit[1])
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
        raise GroupBadRecordError(member.path, lineno0 + 1, raw, cause)
    if first is not None:
        mi, lineno0, reason, link_id, value = first
        member = members[mi]
        _src_g, dst_g, _src_field, dst_field = links[link_id]
        raw = _read_source_line(member, lineno0)
        raise LinkedBadReferenceError(
            member.path, lineno0 + 1, raw,
            f"{_REASON_TEXT[reason]}: "
            f"{dst_field}={value!r} in group {dst_g}",
        )


def compact_linked_state(groups, *, links=(), on_bad="strict",
                         segment_size=DEFAULT_SEGMENT_SIZE,
                         quiesce=DEFAULT_QUIESCE, audit=None):
    """Compact the durable state of a not-yet-committed linked migration.

    The arguments are exactly :func:`migrate_linked_logs`'s.  No member
    file is renamed and no migration commit marker is published;
    compaction only rewrites the scratch state a future migration
    resumes from.  Every member is first brought through the same
    prepare / reference / filter phases a real run performs (prepared
    members are resumed, never rescanned), the all-or-nothing resolved
    markers are published, and only then are checkpoint logs, segments
    and line indexes collapsed.

    A subsequent :func:`migrate_linked_logs` skips straight to staging
    and produces byte-identical output.  In strict mode the globally
    first bad line or bad reference raises the same
    :class:`GroupBadRecordError` / :class:`LinkedBadReferenceError` a
    real run would raise, before any resolved marker is published.  A
    member leased to a live instance raises
    :class:`MigrationLockedError`.
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
    dst_links, src_links = _link_maps(links)

    # Prepare emits skip-mode bad-line audits to its audit stream; in a
    # compaction those bytes belong to the spool (they are rebuilt from
    # the durable line index), so they never reach the caller directly.
    import io
    prepare_audit = io.BytesIO()

    with _member_leases(paths):
        os.makedirs(group_dir, exist_ok=True)
        if os.path.exists(os.path.join(group_dir, _LINKED_COMMITTED)):
            # Post-border state is swept by the migration itself; there
            # is nothing pre-commit to compact.
            return CompactResult(
                groups=tuple(tuple(g) for g in groups),
                members_compacted=0, checkpoint_records_before=0,
                checkpoint_records_after=0, line_index_bytes_freed=0,
                resolved=False,
            )

        for path in paths:
            _reconcile_linked_member(path, committed=False)

        if read_resolved_json(group_resolved_path(group_dir)) is not None:
            # A previous compaction published the decision and was
            # killed during the collapse tail: finish it.
            return _finish_compaction(group_dir, groups, paths)

        # Keep the group manifest coherent with this configuration.  The
        # directory is hash-scoped to the exact member lists; a surviving
        # manifest with a different policy/link set belongs to an earlier
        # uncommitted attempt over the same members, whose per-member
        # checkpoints are validated against the policy below.
        _write_linked_manifest(group_dir, groups, links, on_bad)

        # --- PREPARE every member, exactly like a real run.
        members = []
        index = 0
        for group_index, group in enumerate(groups):
            for path in group:
                member_dir = _linked_member_dir(path)
                member = _resume_linked_prepared(
                    index, group_index, path, member_dir, on_bad
                )
                if member is None:
                    member = _prepare_linked_member(
                        index, group_index, path, member_dir, on_bad,
                        segment_size, quiesce, prepare_audit,
                    )
                # A member resolved marker without the group marker is a
                # previous compaction killed before publication: redo
                # resolution from the intact line indexes.
                _remove(member_resolved_path(member_dir))
                members.append(member)
                index += 1
        _crash_point("compact-prepare")

        # --- RESOLVE references from the durable line indexes.
        refs_path = os.path.join(group_dir, _REF_SPOOL)
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
            dropped = refs.dropped_lines()
            first = refs.first_bad()
        finally:
            refs.close()

        if on_bad == "strict":
            _raise_global_first_error(members, links, first, on_bad)

        # --- FILTER: linked-final + per-member audit spools.
        for member in members:
            member.dropped = dropped.get(member.index, set())
            _spool_member_audits(
                member,
                os.path.join(member.member_dir, _BAD_AUDIT_SPOOL),
                os.path.join(member.member_dir, _AUDIT_SPOOL),
                on_bad,
            )
            if member.dropped:
                _write_linked_final(member)
        _crash_point("compact-filter")

        # --- PUBLISH the resolution decision, all-or-nothing.
        for member in members:
            _write_json_atomic(member_resolved_path(member.member_dir), {
                "version": 1,
                "inode": os.stat(member.path).st_ino,
                "mode": on_bad,
                "dirty": member.dirty,
                "offset": member.offset,
                "lineno": member.lineno,
                "dropped": len(member.dropped),
                "has_linked_final": bool(member.dropped),
            })
        _write_json_atomic(group_resolved_path(group_dir), {
            "version": 1,
            "groups": [[os.path.abspath(p) for p in g] for g in groups],
            "links": [list(l) for l in links],
            "on_bad": on_bad,
        })
        _crash_point("compact-resolve")

        # --- COLLAPSE storage while still holding every lease, so a
        # concurrent migration cannot stage while checkpoint logs and
        # line indexes are being rewritten.
        return _finish_compaction(group_dir, groups, paths)


def _finish_compaction(group_dir, groups, paths):
    """Idempotent collapse tail: cp/segments per member, free indexes."""
    cp_before = cp_after = 0
    index_freed = 0
    members_done = 0
    for path in paths:
        member_dir = _linked_member_dir(path)
        if read_resolved_json(member_resolved_path(member_dir)) is None:
            # The marker is consumed when the migration staged and swept
            # this directory; nothing left to compact.
            if not os.path.isdir(member_dir):
                continue
            # Directory present but marker missing mid-collapse cannot
            # happen (markers are published together); leave it alone.
            continue
        lines_path = os.path.join(member_dir, _LINES_NAME)
        if os.path.exists(lines_path):
            index_freed += os.path.getsize(lines_path)
        try:
            before, after = compact_workdir_checkpoint(member_dir)
        except (OSError, ValueError):
            before = after = 0
        cp_before += before
        cp_after += after
        _remove(lines_path)
        _fsync_dir(member_dir)
        members_done += 1
    _remove(os.path.join(group_dir, _REF_SPOOL))
    _fsync_dir(group_dir)
    return CompactResult(
        groups=tuple(tuple(g) for g in groups),
        members_compacted=members_done,
        checkpoint_records_before=cp_before,
        checkpoint_records_after=cp_after,
        line_index_bytes_freed=index_freed,
        resolved=True,
    )


def _crash_point(point):
    from .log_migration import _crash_point as _cp
    _cp(point)


# ---------------------------------------------------------------------------
# Migration resume from compacted, resolved state
# ---------------------------------------------------------------------------


def resume_resolved_members(groups, group_dir, links, on_bad, audit_stream):
    """Rebuild migration members from compacted resolved markers.

    Returns the list of :class:`proto_migrate.linked_migration._LinkedMember`
    a migration can stage straight away, ``None`` when no matching group
    resolved marker exists (the caller then runs its normal
    prepare/resolve/filter path).  A marker for a different group list,
    link set or policy is ignored.  Skip-mode drop audits persisted by
    compaction are replayed to *audit_stream* exactly once per attempt,
    so the resumed migration's audit is the uninterrupted run's.
    """
    from .linked_migration import _LinkedMember

    marker = read_resolved_json(group_resolved_path(group_dir))
    if marker is None:
        return None
    abs_groups = [[os.path.abspath(p) for p in g] for g in groups]
    if (
        marker.get("groups") != abs_groups
        or marker.get("links") != [list(l) for l in links]
        or marker.get("on_bad") != on_bad
    ):
        return None

    members = []
    opened_srcs = []
    index = 0

    def abandon():
        for src in opened_srcs:
            try:
                src.close()
            except OSError:
                pass

    for group_index, group in enumerate(groups):
        for path in group:
            member_dir = _linked_member_dir(path)
            data = read_resolved_json(member_resolved_path(member_dir))
            if data is None:
                # Member publication precedes the durable group marker,
                # so this only happens after external interference;
                # redo the member from scratch.
                abandon()
                return None
            try:
                current_inode = os.stat(path).st_ino
            except OSError:
                abandon()
                raise
            if data.get("inode") != current_inode or \
                    data.get("mode") != on_bad:
                abandon()
                return None
            if data.get("has_linked_final"):
                if not os.path.isfile(
                    os.path.join(member_dir, _LINKED_FINAL)
                ):
                    # The filtered output was consumed by a staging that
                    # got rolled back and cannot be rebuilt without the
                    # (compacted away) line index: fall back to a normal
                    # prepare, which rescans the member.
                    abandon()
                    return None
            elif data.get("dirty") and not os.path.isfile(
                os.path.join(member_dir, _FINAL)
            ):
                # The assembled final (scan segments *plus* the drain
                # tail) was consumed by a rolled-back staging.  The
                # compacted segment holds only the scan prefix, so let
                # the normal prepare path reassemble and re-drain (or
                # rescan if the line index was also compacted away).
                abandon()
                return None
            src = open(path, "rb")
            opened_srcs.append(src)
            member = _LinkedMember(
                index, group_index, path, member_dir, src,
                bool(data["dirty"]), int(data["offset"]),
                int(data["lineno"]), 0, 0, int(data["lineno"]),
                linked_final=bool(data.get("has_linked_final")),
            )
            member.resolved_dropped = int(data.get("dropped", 0))
            if on_bad == "skip":
                # A direct run emits bad-line audits during prepare and
                # drop audits at filter time; replay in that same order.
                for spool_name in (_BAD_AUDIT_SPOOL, _AUDIT_SPOOL):
                    spool = os.path.join(member_dir, spool_name)
                    if os.path.exists(spool):
                        with open(spool, "rb") as f:
                            audit_stream.write(f.read())
                audit_stream.flush()
            members.append(member)
            index += 1
    return members


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def run_compact_cli(argv):
    parser = argparse.ArgumentParser(
        prog="python3 -m proto_migrate compact-linked-logs",
        description="Compact the checkpoint logs and per-line indexes of "
        "a prepared linked migration without changing its outcome.",
    )
    parser.add_argument("--group", action="append", nargs="+",
                        required=True, metavar="FILE")
    parser.add_argument("--link", action="append", default=[],
                        metavar="SRC:DST:SRC_FIELD:DST_FIELD")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--strict", action="store_const", const="strict",
                      dest="on_bad")
    mode.add_argument("--skip", action="store_const", const="skip",
                      dest="on_bad")
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
    from .linked_migration import (
        LinkedBadReferenceError,
        MigrationLockedError,
    )
    from .log_migration import BadRecordError
    try:
        result = compact_linked_state(
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
        "members=%d checkpoint_records=%d->%d index_bytes_freed=%d "
        "resolved=%s"
        % (
            result.members_compacted,
            result.checkpoint_records_before,
            result.checkpoint_records_after,
            result.line_index_bytes_freed,
            "yes" if result.resolved else "no",
        )
    )
    return EXIT_OK
