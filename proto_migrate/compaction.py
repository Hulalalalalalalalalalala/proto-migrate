"""Online compaction of linked-migration checkpoint and index state.

A linked migration that is killed and resumed repeatedly accumulates
durable bookkeeping: one checkpoint record per output segment per run
in every member's ``checkpoint`` log, and one record per consumed
source line in every member's ``lines`` index.  :func:`compact_linked_logs`
folds that state into a deterministic compact form whose bookkeeping
grows with the *member count*, not with the total line count:

  * every member's checkpoint log is collapsed to its header plus a
    single record over one merged segment (deterministic: same input
    state always yields the same bytes);
  * the reference-resolution outcome is made durable once, in the group
    work directory's ``linked-resolved`` record (per-member bad lines
    and dropped records with their source offsets, the globally first
    bad reference, the bad-reference count);
  * each member's per-line index (``lines``) is then removed -- the
    resolution record replaces it -- and the member carries a small
    ``compacted`` receipt.

The migration itself is untouched: a later ``migrate_linked_logs`` run
finds the compact state, resumes every member from its receipt without
rescanning it, and finishes **byte-for-byte identical** to a run whose
state was never compacted.  Resume semantics and the summary counters
follow the usual fresh-only rules: a run that resumes compacted state
migrates and resolves nothing anew and reports zeros for the work the
earlier runs did.

Compaction is itself crash-recoverable.  The resolution record is
published before any member is compacted; each member's compaction is
an ordered, idempotent sequence (merge segments, collapse the
checkpoint log, publish the receipt, delete the line index) so a kill
at any point lets the rerun continue without rescanning members whose
receipt is already durable.  Compaction takes the same non-blocking
member leases as the migration, so it never runs concurrently with a
live migrator over the same members: a lease held by an active instance
raises :class:`MigrationLockedError`.

Everything compaction writes lives in local files next to the members
and can always be rebuilt: losing the resolution record or a receipt
simply forces the affected members to be prepared again.
"""

from __future__ import annotations

import argparse
import os
import shutil
import sys
from typing import NamedTuple

from .log_migration import (
    DEFAULT_QUIESCE,
    DEFAULT_SEGMENT_SIZE,
    EXIT_ERROR,
    EXIT_OK,
    EXIT_USAGE,
    _convert,
    _crash_point,
)
from .group_migration import GroupBadRecordError
from .linked_migration import (
    LinkedBadReferenceError,
    MigrationLockedError,
    _REASON_TEXT,
    _REFS_DB,
    _RefsDb,
    _apply_resolution_record,
    _collect_member_decisions,
    _compact_member_state,
    _linked_group_dir,
    _linked_group_state,
    _linked_member_dir,
    _member_leases,
    _normalize_groups,
    _normalize_links,
    _prepare_linked_member,
    _probe_members,
    _read_compacted,
    _read_resolution,
    _read_source_line,
    _remove,
    _resolution_key,
    _resume_compacted_member,
    _resume_linked_prepared,
    _write_resolution,
)

__all__ = [
    "CompactionResult",
    "compact_linked_logs",
    "run_compact_cli",
]


class CompactionResult(NamedTuple):
    groups: tuple
    members_prepared: int     # members this call prepared (scanned) anew
    members_compacted: int    # members this call compacted
    references_bad: int       # bad references this call newly resolved
    first_error: object | None  # pending strict-mode first problem, if any
    first_error_kind: str | None  # "bad_record" | "bad_reference" | None


def _first_error_from_decisions(members, first, links):
    """The strict-mode global first error for resolved decisions."""
    bad_line = None
    for member in members:
        if member.bad_lines:
            bad_line = (member, member.bad_lines[0])
            break
    if bad_line is not None and (
        first is None
        or (bad_line[0].index, bad_line[1]) < (first[0], first[1])
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
        return GroupBadRecordError(member.path, lineno0 + 1, raw, cause), \
            "bad_record"
    if first is not None:
        mi, lineno0, reason, link_id, value = first
        member = members[mi]
        _src_g, dst_g, _src_field, dst_field = links[link_id]
        return LinkedBadReferenceError(
            member.path, lineno0 + 1, _read_source_line(member, lineno0),
            f"{_REASON_TEXT[reason]}: {dst_field}={value!r} "
            f"in group {dst_g}",
        ), "bad_reference"
    return None, None


def compact_linked_logs(groups, *, links=(), on_bad="strict",
                        segment_size=DEFAULT_SEGMENT_SIZE,
                        quiesce=DEFAULT_QUIESCE, audit=None):
    """Compact the durable work state of an uncommitted linked migration.

    Prepares any not-yet-prepared member (resuming prepared ones from
    their checkpoints, never rescanning them), resolves references once
    if no durable resolution exists, then rewrites every member's
    checkpoint log and line index into the deterministic compact form
    described in the module docstring.  The original log files are
    never modified and no commit marker is published; a later
    :func:`~proto_migrate.migrate_linked_logs` run finishes from the
    compact state byte-identically to an uncompacted run.

    A kill at any point is safe: rerunning continues from the durable
    receipts and never rescans a member whose ``compacted`` receipt is
    already written.  If the group commit marker already exists there
    is nothing to compact (the post-border state is finished forward by
    the migration itself) and the call is a no-op.

    Raises the same validation errors as
    :func:`~proto_migrate.migrate_linked_logs`, and
    :class:`MigrationLockedError` while a needed member lease is held
    by a live instance.
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

    dst_links = {}
    src_links = {}
    for link_id, (src_g, dst_g, src_field, dst_field) in enumerate(links):
        dst_links.setdefault(dst_g, []).append((link_id, dst_field))
        src_links.setdefault(src_g, []).append((link_id, src_field))

    members = []
    try:
        with _member_leases(paths):
            _crash_point("compact-lock")
            committed = _linked_group_state(
                groups, links, on_bad, paths, group_dir
            )
            if committed:
                # Post-border state is finished forward by the
                # migration itself; there is nothing to compact.
                return CompactionResult(
                    groups=tuple(tuple(g) for g in groups),
                    members_prepared=0,
                    members_compacted=0,
                    references_bad=0,
                    first_error=None,
                    first_error_kind=None,
                )

            key = _resolution_key(groups, links, on_bad)
            resolution = _read_resolution(group_dir)
            if resolution is not None and (
                resolution.get("key") != key
                or len(resolution["members"]) != len(paths)
            ):
                resolution = None

            # --- Ensure every member is prepared (never rescanning a
            # prepared or compacted one).
            prepared_now = 0
            index = 0
            for group_index, group in enumerate(groups):
                for path in group:
                    member_dir = _linked_member_dir(path)
                    member = None
                    if resolution is not None:
                        member = _resume_compacted_member(
                            index, group_index, path, member_dir, on_bad
                        )
                    elif _read_compacted(member_dir) is not None:
                        # Compacted but the resolution record is gone:
                        # the line index is unrecoverable, so the
                        # member is prepared from scratch.
                        shutil.rmtree(member_dir, ignore_errors=True)
                    if member is None:
                        member = _resume_linked_prepared(
                            index, group_index, path, member_dir, on_bad
                        )
                    if member is None:
                        member = _prepare_linked_member(
                            index, group_index, path, member_dir,
                            on_bad, segment_size, quiesce, audit_stream,
                        )
                        prepared_now += 1
                    members.append(member)
                    index += 1
            _crash_point("compact-prepare")

            # --- Resolve references once; the outcome becomes durable.
            if resolution is None:
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
                    _crash_point("compact-refs")
                    references_bad = refs.fresh_bad_edges()
                    dropped = refs.dropped_lines()
                    first = refs.first_bad()
                finally:
                    refs.close()
                for member in members:
                    _collect_member_decisions(member, dropped)
                resolution = {
                    "version": 1,
                    "key": key,
                    "references_bad": references_bad,
                    "first_bad_ref": list(first) if first else None,
                    "members": [
                        {
                            "bad_lines": sorted(
                                [lineno0, member.bad_line_offsets[lineno0]]
                                for lineno0 in member.bad_lines
                            ),
                            "dropped": sorted(
                                [lineno0, member.dropped_offsets[lineno0]]
                                for lineno0 in dropped.get(member.index, ())
                            ),
                        }
                        for member in members
                    ],
                }
                _write_resolution(group_dir, resolution)
                _remove(refs_path)
            else:
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
            _crash_point("compact-resolve")

            # --- Compact every member (idempotent per member).
            compacted = 0
            for member in members:
                if _compact_member_state(member.member_dir):
                    compacted += 1
                _crash_point("compact-member")
            _crash_point("compact-done")
    finally:
        for member in members:
            try:
                member.src.close()
            except OSError:
                pass

    first_error = first_error_kind = None
    if on_bad == "strict":
        first_error, first_error_kind = _first_error_from_decisions(
            members, first, links
        )
    return CompactionResult(
        groups=tuple(tuple(g) for g in groups),
        members_prepared=prepared_now,
        members_compacted=compacted,
        references_bad=references_bad,
        first_error=first_error,
        first_error_kind=first_error_kind,
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def run_compact_cli(argv):
    parser = argparse.ArgumentParser(
        prog="python3 -m proto_migrate compact-linked-logs",
        description="Compact the durable checkpoint and line-index "
        "state of an uncommitted linked-group migration into a "
        "deterministic, member-count-sized form; crash-recoverable, "
        "never rescanning completed members.  The log files themselves "
        "are not modified.",
    )
    parser.add_argument("--group", action="append", nargs="+",
                        required=True, metavar="FILE",
                        help="one log group (repeat per group, in "
                        "group order)")
    parser.add_argument("--link", action="append", default=[],
                        metavar="SRC:DST:SRC_FIELD:DST_FIELD",
                        help="declare a reference (repeatable; same "
                        "syntax as migrate-linked-logs)")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--strict", action="store_const", const="strict",
                      dest="on_bad", help="use the strict bad-record "
                      "policy (default)")
    mode.add_argument("--skip", action="store_const", const="skip",
                      dest="on_bad", help="use the skip bad-record "
                      "policy")
    parser.set_defaults(on_bad="strict")
    parser.add_argument("--segment-size", type=int,
                        default=DEFAULT_SEGMENT_SIZE, metavar="BYTES",
                        help="rotate temp segments at this size when "
                        "preparing members (default: %(default)s)")
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
        result = compact_linked_logs(
            args.group,
            links=links,
            on_bad=args.on_bad,
            segment_size=args.segment_size,
            quiesce=args.quiesce_ms / 1000,
        )
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
        "groups=%d members=%d prepared=%d compacted=%d refs_bad=%d"
        % (
            len(result.groups),
            sum(len(g) for g in result.groups),
            result.members_prepared,
            result.members_compacted,
            result.references_bad,
        )
    )
    if result.first_error is not None:
        print(f"first-error[{result.first_error_kind}]: "
              f"{result.first_error}")
    return EXIT_OK
