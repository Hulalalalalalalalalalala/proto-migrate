"""Read-only migration rehearsal (dry run) for linked log groups.

:func:`rehearse_linked_logs` answers, for the exact group set and link
declarations a real :func:`~proto_migrate.migrate_linked_logs` run would
receive, *what that run would do* -- without touching anything:

  * it never creates, renames, truncates or deletes a file: no member
    lease is taken (a live instance migrating the same members is
    undisturbed and :class:`MigrationLockedError` is never raised), no
    lock file, work directory, checkpoint or marker is written, and the
    reference-resolution spill database lives in a private temporary
    directory that is removed when the call returns;
  * it never blocks an appender: members are opened read-only and read
    line by line, exactly like the streaming snapshot reader.

Snapshot semantics
------------------
Every member is pinned at open time (one inode, one frozen length) and
only complete, newline-terminated lines inside that pinned prefix are
considered, so the report's locations never drift while appenders keep
writing.  Because appenders only ever *append*, the pinned prefix is a
prefix of whatever a later real migration consumes: every per-record
verdict in the report (kept / skipped as a bad record / dropped over a
bad reference, each with its group, member and line location) matches
the real run's outcome for that same line, record for record.  When the
files are quiet between the rehearsal and the real run, the aggregate
counters and the predicted per-member rewrite counts match the real
run's result exactly.

Report contents
---------------
For every member (in group order, then member order):

  * ``records_migrated`` -- the number of records a real run would
    rewrite for this member (``MigrationResult.records_migrated``);
  * ``records_skipped`` -- bad records the run would skip;
  * ``bad_records`` -- every bad record, located by group, member and
    1-based line number, with its raw line and audit entry;
  * ``dropped`` -- every record that would be dropped over its bad
    references (skip mode's discard list), located the same way;
  * ``predicted_bytes`` -- the exact bytes the member would hold after
    the run (before any concurrent appends are converged).

Across the whole group set the report carries the bad records and the
bad references in the global group/member/line order, and -- mirroring
strict mode -- the attribution of the globally first problem: the same
exception type (:class:`GroupBadRecordError` or
:class:`LinkedBadReferenceError`), path, line number and cause the real
strict run would raise.  Bad-record and bad-reference classification,
the four bad-reference reason classes and the audit format are exactly
the real migration's.
"""

from __future__ import annotations

import argparse
import os
import sys
import tempfile
from typing import NamedTuple

from .log_migration import (
    DEFAULT_QUIESCE,
    EXIT_ERROR,
    EXIT_OK,
    EXIT_USAGE,
    LINE_GOOD,
    _audit_line,
    _classify_bad_line,
    _convert,
)
from .group_migration import GroupBadRecordError
from .linked_migration import (
    LinkedBadReferenceError,
    _normalize_groups,
    _normalize_links,
    _open_path_wait,
    _probe_members,
    _REASON_TEXT,
    _RefsDb,
)

__all__ = [
    "RehearsalEntry",
    "RehearsalMemberReport",
    "RehearsalReport",
    "rehearse_linked_logs",
    "run_rehearsal_cli",
]


class RehearsalEntry(NamedTuple):
    """One located verdict: a bad record, a bad reference or a drop."""

    group_index: int
    member_index: int
    path: str
    lineno: int          # 1-based, in the member's own file
    raw: bytes           # the complete raw source line (newline included)
    kind: str            # "bad_record" | "bad_reference" | "dropped"
    reason: str          # cause text / bad-reference reason, "" for drops
    audit: bytes         # the exact audit line a real run would emit


class RehearsalMemberReport(NamedTuple):
    path: str
    group_index: int
    member_index: int
    records_migrated: int   # records a real run would rewrite (0 if kept)
    records_skipped: int    # bad records a real run would skip
    records_dropped: int    # records a real skip run would drop
    would_replace: bool     # the real run would rename this member
    predicted_bytes: bytes  # exact post-run content of the pinned prefix
    bad_records: tuple      # RehearsalEntry, kind="bad_record", line order
    dropped: tuple          # RehearsalEntry, kind="dropped", line order


class RehearsalReport(NamedTuple):
    groups: tuple
    links: tuple
    on_bad: str
    members: tuple              # RehearsalMemberReport, flat member order
    bad_records: tuple          # global group/member/line order
    bad_references: tuple       # RehearsalEntry, kind="bad_reference"
    discarded: tuple            # skip mode's full discard list, in order
    references_bad: int
    first_error: object | None  # GroupBadRecordError/LinkedBadReferenceError
    first_error_kind: str | None  # "bad_record" | "bad_reference" | None


class _ShimMember:
    """The slice of ``_LinkedMember`` the reference index consumes."""

    def __init__(self, index, group_index, path):
        self.index = index
        self.group_index = group_index
        self.path = path
        self.lineno = 0
        self.dirty = False
        self.dropped = set()
        self.fresh_from = 0
        # Per-line mirrors of the durable line index.
        self.kinds = {}           # lineno0 -> LINE_* kind
        self.raws = {}            # lineno0 -> raw bytes (bad lines only)
        self.outputs = {}         # lineno0 -> migrated bytes (good lines)
        self.offsets = {}         # lineno0 -> source offset


def _render_audit(path, lineno, raw):
    """The exact audit bytes a real run writes for this line."""
    prefix = os.fspath(path).encode("utf-8", "surrogateescape")
    return prefix + b":" + _audit_line(lineno, raw)


def _pinned_lines(path, quiesce):
    """Yield ``(offset, raw)`` for every complete line of the pinned prefix.

    The member is opened read-only (riding out a mid-rename gap exactly
    like the streaming snapshot reader); its length is frozen at open
    time, and only newline-terminated lines inside that frozen prefix
    are yielded -- a record caught mid-append is excluded.  Nothing is
    locked, created or written, and a concurrent migrator's atomic
    replacement is invisible to the pinned descriptor.
    """
    fd = _open_path_wait(path, quiesce)
    try:
        end = os.fstat(fd.fileno()).st_size
        offset = 0
        while offset < end:
            fd.seek(offset)
            line = fd.readline(end - offset)
            if not line or not line.endswith(b"\n"):
                break
            yield offset, line
            offset += len(line)
    finally:
        fd.close()


def _scan_member(index, group_index, path, quiesce):
    """Classify one member's pinned prefix; return its shim member."""
    shim = _ShimMember(index, group_index, path)
    lineno0 = 0
    for offset, raw in _pinned_lines(path, quiesce):
        shim.offsets[lineno0] = offset
        try:
            out = _convert(raw)
        except ValueError:
            shim.kinds[lineno0] = _classify_bad_line(raw)
            shim.raws[lineno0] = raw
            shim.dirty = True
        else:
            shim.kinds[lineno0] = LINE_GOOD
            shim.outputs[lineno0] = out
            if out != raw:
                shim.dirty = True
        lineno0 += 1
    shim.lineno = lineno0
    return shim


def _bad_cause(raw):
    try:
        _convert(raw)
    except ValueError as exc:
        return exc
    return ValueError("undecodable record")


def rehearse_linked_logs(groups, *, links=(), on_bad="strict",
                         quiesce=DEFAULT_QUIESCE):
    """Rehearse a linked-group migration without changing anything.

    Takes the same ``groups`` and ``links`` declarations as
    :func:`~proto_migrate.migrate_linked_logs` and returns a
    :class:`RehearsalReport` describing, record by record, what the real
    run would do to the pinned prefix of every member.  The call is
    strictly read-only: it takes no lease (never raises
    :class:`MigrationLockedError`), writes no file, and never blocks an
    appender or a concurrently migrating instance.  Validation errors
    (:class:`TypeError`, :class:`ValueError`, :class:`FileNotFoundError`)
    match the real entry point exactly.
    """
    if on_bad not in ("strict", "skip"):
        raise ValueError(f"on_bad must be 'strict' or 'skip', got {on_bad!r}")
    groups = _normalize_groups(groups)
    links = _normalize_links(links, len(groups))
    paths = [path for group in groups for path in group]
    _probe_members(paths)

    # Per-group source/target link views (same shape as the real run).
    dst_links = {}
    src_links = {}
    for link_id, (src_g, dst_g, src_field, dst_field) in enumerate(links):
        dst_links.setdefault(dst_g, []).append((link_id, dst_field))
        src_links.setdefault(src_g, []).append((link_id, src_field))

    # --- Scan every member's pinned prefix (read-only, streaming).
    members = []
    index = 0
    for group_index, group in enumerate(groups):
        for path in group:
            members.append(_scan_member(index, group_index, path, quiesce))
            index += 1

    # --- Resolve references over a private spill image.  The scratch
    # directory is removed wholesale on return; nothing is written next
    # to any member.
    with tempfile.TemporaryDirectory(prefix="proto-migrate-rehearse-") \
            as scratch:
        refs = _RefsDb(os.path.join(scratch, "refs.sqlite3"))
        try:
            for shim in members:
                refs.build_scanned_member(
                    shim.index,
                    ((n, shim.kinds[n],
                      shim.outputs.get(n, shim.raws.get(n)))
                     for n in sorted(shim.kinds)),
                    dst_links.get(shim.group_index, []),
                    src_links.get(shim.group_index, []),
                    fresh_from=0,
                )
            refs.validate()
            references_bad = refs.fresh_bad_edges()
            dropped = refs.dropped_lines()
            first = refs.first_bad()
            bad_edges = list(refs._db.execute(
                "SELECT nodes.mi, nodes.lineno, edges.reason,"
                " edges.link_id, edges.value FROM edges"
                " JOIN nodes ON nodes.node_id=edges.src"
                " WHERE edges.reason IS NOT NULL"
                " ORDER BY nodes.mi, nodes.lineno, edges.reason"
            ))
        finally:
            refs.close()

    # --- Assemble the report.
    flat_index = {}   # flat member index -> (group_index, member_index)
    pos = 0
    for group_index, group in enumerate(groups):
        for member_index, _path in enumerate(group):
            flat_index[pos] = (group_index, member_index)
            pos += 1

    def raw_of(shim, lineno0):
        """The raw source line of one pinned line (good or bad)."""
        if lineno0 in shim.raws:
            return shim.raws[lineno0]
        # Good lines were not retained verbatim; re-read the pinned
        # prefix (appenders only append, so offsets never drift).
        with open(shim.path, "rb") as f:
            f.seek(shim.offsets[lineno0])
            return f.readline()

    member_reports = []
    bad_records_global = []   # (flat index, lineno0, entry)
    for shim in members:
        group_index, member_index = flat_index[shim.index]
        shim.dropped = dropped.get(shim.index, set())
        bad_entries = []
        for lineno0 in sorted(shim.raws):
            raw = shim.raws[lineno0]
            entry = RehearsalEntry(
                group_index=group_index, member_index=member_index,
                path=shim.path, lineno=lineno0 + 1, raw=raw,
                kind="bad_record", reason=str(_bad_cause(raw)),
                audit=_render_audit(shim.path, lineno0 + 1, raw),
            )
            bad_entries.append(entry)
            bad_records_global.append((shim.index, lineno0, entry))
        drop_entries = []
        for lineno0 in sorted(shim.dropped):
            raw = raw_of(shim, lineno0)
            entry = RehearsalEntry(
                group_index=group_index, member_index=member_index,
                path=shim.path, lineno=lineno0 + 1, raw=raw,
                kind="dropped", reason="",
                audit=_render_audit(shim.path, lineno0 + 1, raw),
            )
            drop_entries.append(entry)
        would_replace = shim.dirty or bool(shim.dropped)
        good = sum(1 for k in shim.kinds.values() if k == LINE_GOOD)
        bad = sum(1 for k in shim.kinds.values() if k != LINE_GOOD)
        predicted = b"".join(
            shim.outputs[n] for n in sorted(shim.outputs)
            if n not in shim.dropped
        )
        member_reports.append(RehearsalMemberReport(
            path=shim.path,
            group_index=group_index,
            member_index=member_index,
            records_migrated=(good - len(shim.dropped))
            if would_replace else 0,
            records_skipped=bad if would_replace else 0,
            records_dropped=len(shim.dropped),
            would_replace=would_replace,
            predicted_bytes=predicted,
            bad_records=tuple(bad_entries),
            dropped=tuple(drop_entries),
        ))

    bad_reference_entries = []
    seen_bad_record = set()
    for mi, lineno0, reason, link_id, value in bad_edges:
        # A record carrying several bad references is located once, at
        # its first bad edge (the same reason tie-break the real run's
        # global first-error comparison uses); the edge count stays the
        # separate ``references_bad`` tally.
        loc = (mi, lineno0)
        if loc in seen_bad_record:
            continue
        seen_bad_record.add(loc)
        shim = members[mi]
        group_index, member_index = flat_index[mi]
        _src_g, dst_g, _src_field, dst_field = links[link_id]
        raw = raw_of(shim, lineno0)
        bad_reference_entries.append(RehearsalEntry(
            group_index=group_index, member_index=member_index,
            path=shim.path, lineno=lineno0 + 1, raw=raw,
            kind="bad_reference",
            reason=f"{_REASON_TEXT[reason]}: "
                   f"{dst_field}={value!r} in group {dst_g}",
            audit=_render_audit(shim.path, lineno0 + 1, raw),
        ))

    # Strict-mode global first-error attribution: the earliest bad line
    # and the earliest bad reference in group/member/line order, exactly
    # the comparison the real run performs.
    first_error = None
    first_error_kind = None
    first_bad_line = None
    for shim in members:
        hit = None
        for lineno0 in sorted(shim.kinds):
            if shim.kinds[lineno0] != LINE_GOOD:
                hit = lineno0
                break
        if hit is not None:
            first_bad_line = (shim, hit)
            break
    if first_bad_line is not None and (
        first is None
        or (first_bad_line[0].index, first_bad_line[1])
        < (first[0], first[1])
    ):
        shim, lineno0 = first_bad_line
        raw = shim.raws[lineno0]
        first_error = GroupBadRecordError(
            shim.path, lineno0 + 1, raw, _bad_cause(raw))
        first_error_kind = "bad_record"
    elif first is not None:
        mi, lineno0, reason, link_id, value = first
        shim = members[mi]
        _src_g, dst_g, _src_field, dst_field = links[link_id]
        first_error = LinkedBadReferenceError(
            shim.path, lineno0 + 1, raw_of(shim, lineno0),
            f"{_REASON_TEXT[reason]}: {dst_field}={value!r} "
            f"in group {dst_g}",
        )
        first_error_kind = "bad_reference"

    # The discard list a skip run would audit, in the real run's
    # emission order: bad records first (audited while each member is
    # prepared, in group/member/line order), then the dropped
    # bad-reference records (audited while filtering, same order).
    discarded = tuple(e for _mi, _ln, e in sorted(bad_records_global)) \
        + tuple(
            entry for report in member_reports for entry in report.dropped
        )

    return RehearsalReport(
        groups=tuple(tuple(g) for g in groups),
        links=tuple(tuple(link) for link in links),
        on_bad=on_bad,
        members=tuple(member_reports),
        bad_records=tuple(e for _mi, _ln, e in sorted(bad_records_global)),
        bad_references=tuple(bad_reference_entries),
        discarded=discarded,
        references_bad=references_bad,
        first_error=first_error,
        first_error_kind=first_error_kind,
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def run_rehearsal_cli(argv):
    parser = argparse.ArgumentParser(
        prog="python3 -m proto_migrate rehearse-linked-logs",
        description="Read-only rehearsal of a linked-group migration: "
        "reports, record by record, what migrate-linked-logs would do "
        "with the same groups and links, without writing anything.",
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
                      dest="on_bad", help="report the globally first bad "
                      "line or bad reference the strict run would raise "
                      "(default)")
    mode.add_argument("--skip", action="store_const", const="skip",
                      dest="on_bad", help="report the records a skip run "
                      "would discard, auditing each to stderr exactly "
                      "as the real run would")
    parser.set_defaults(on_bad="strict")
    parser.add_argument("--quiesce-ms", type=float,
                        default=DEFAULT_QUIESCE * 1000, metavar="MS",
                        help="open-retry window for members mid-rename "
                        "(default: %(default)s)")
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
        report = rehearse_linked_logs(
            args.group, links=links, on_bad=args.on_bad,
            quiesce=args.quiesce_ms / 1000,
        )
    except (TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_USAGE
    except OSError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_ERROR

    if args.on_bad == "skip":
        # The discard list a real skip run would audit, byte-identical.
        for entry in report.discarded:
            sys.stderr.buffer.write(entry.audit)
        sys.stderr.buffer.flush()

    migrated = sum(m.records_migrated for m in report.members)
    skipped = sum(m.records_skipped for m in report.members)
    dropped = sum(m.records_dropped for m in report.members)
    print(
        "groups=%d members=%d migrated=%d skipped=%d dropped=%d "
        "refs_bad=%d replaced=%s"
        % (
            len(report.groups),
            sum(len(g) for g in report.groups),
            migrated,
            skipped,
            dropped,
            report.references_bad,
            "yes" if any(m.would_replace for m in report.members)
            else "no",
        )
    )
    if report.first_error is not None:
        print(f"first-error[{report.first_error_kind}]: "
              f"{report.first_error}")
    return EXIT_OK
