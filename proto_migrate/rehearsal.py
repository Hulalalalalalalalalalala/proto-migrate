"""Read-only rehearsal (preflight) for a cross-group linked migration.

:func:`rehearse_linked_logs` accepts exactly the same group sets, link
declarations and bad-record policy as
:func:`proto_migrate.migrate_linked_logs`, but it never modifies a file:
it reports what a real run *would* do.  A rehearsal

  * takes no leases and no locks -- a real migration instance may run,
    even against the very same members, while a rehearsal is in flight;
    a lease held by an active instance never raises
    :class:`MigrationLockedError`, and appenders are never blocked;
  * pins every member's inode for the whole pass and tails it with the
    same read-only line follower a real scan uses, so findings keep
    their group / member / line positions even while writers append or a
    real migration renames members underneath the rehearsal;
  * rebuilds its classification purely from the pinned bytes through
    the *same* durable per-line index format and the *same* streaming
    reference resolver the migration uses, so every finding is the one
    the real run reaches;
  * spills its intermediate state to local files under a private
    ``.migrate-rehearse-tmp-<hash>/`` directory next to the first
    member (the hash covers the member lists, the policy and the link
    set).  The directory is removed at the end and can always be
    rebuilt; no migration work directory is ever read or touched.

The returned :class:`RehearsalReport` gives, for every member, the
number of records the real run would write into its migrated output,
every bad record and bad reference located in group / member / line
order, the global first error a strict run would attribute, and -- in
skip mode -- the exact list of records a skip run would drop (bad lines
plus records holding bad references).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sys
from typing import NamedTuple

from .log_migration import (
    DEFAULT_QUIESCE,
    EXIT_BAD_RECORD,
    EXIT_ERROR,
    EXIT_OK,
    EXIT_USAGE,
    LINE_GOOD,
    _classify_bad_line,
    _convert,
    _read_lines,
)
from .linked_migration import (
    _REASON_TEXT,
    _LineIndex,
    _RefsDb,
    _normalize_groups,
    _normalize_links,
    _open_path_wait,
    _probe_members,
    _stream_line_entries,
)

__all__ = [
    "RehearsalDropped",
    "RehearsalFinding",
    "RehearsalMember",
    "RehearsalReport",
    "rehearse_linked_logs",
    "run_rehearse_cli",
]

_REHEARSE_DIR_NAME = ".migrate-rehearse-tmp"
_AUDIT_SNIPPET = 32


class RehearsalFinding(NamedTuple):
    """One bad record or bad reference, uniquely located.

    ``group`` / ``member`` are list indices (0-based, the member index
    is within its group); ``lineno`` is the 1-based source line.
    ``kind`` is ``"bad_record"`` or ``"bad_reference"``.  ``path`` is
    the member path as given; ``raw`` is the source line including its
    trailing newline when present; ``detail`` is exactly the cause text
    the corresponding migration error carries.
    """

    group: int
    member: int
    lineno: int
    kind: str
    path: str
    raw: bytes
    detail: str

    @property
    def snippet(self):
        line = self.raw[:-1] if self.raw.endswith(b"\n") else self.raw
        return line[:_AUDIT_SNIPPET]


class RehearsalDropped(NamedTuple):
    """One record a skip-mode run would discard, uniquely located."""

    group: int
    member: int
    lineno: int
    path: str
    raw: bytes
    why: str  # "bad_record" or "bad_reference"

    @property
    def snippet(self):
        line = self.raw[:-1] if self.raw.endswith(b"\n") else self.raw
        return line[:_AUDIT_SNIPPET]


class RehearsalMember(NamedTuple):
    """The per-member rehearsal result.

    ``records_migrated`` is exactly what the real run reports for this
    member: the number of good records written into its migrated output
    when the member changes (canonical lines are re-emitted verbatim,
    bad-reference records are excluded), or ``0`` for a member the real
    run would leave byte-for-byte untouched.
    """

    path: str
    group: int
    member: int
    total_lines: int
    records_migrated: int
    bad_records: int
    bad_reference_lines: int
    dropped_lines: int
    replaced: bool


class RehearsalReport(NamedTuple):
    on_bad: str
    groups: tuple
    links: tuple
    members: tuple
    findings: tuple
    first_error: RehearsalFinding | None
    dropped: tuple
    total_migrated: int
    total_bad_records: int
    total_bad_references: int
    total_dropped: int
    replaced: bool


# ---------------------------------------------------------------------------
# Work directory
# ---------------------------------------------------------------------------


def _rehearse_dir(groups, links, on_bad):
    """Private, hash-scoped scratch directory next to the first member.

    Keyed like the linked migration's work directory but under a
    distinct name, so rehearsals never touch migration state and
    rehearsals over disjoint group sets run side by side.
    """
    first_dir = os.path.dirname(os.path.abspath(groups[0][0]))
    key = json.dumps(
        {
            "g": [[os.path.abspath(p) for p in g] for g in groups],
            "l": [list(l) for l in links],
            "b": on_bad,
        },
        separators=(",", ":"),
    )
    digest = hashlib.sha1(key.encode("utf-8")).hexdigest()[:16]
    return os.path.join(first_dir, f"{_REHEARSE_DIR_NAME}-{digest}")


class _IndexShim:
    """The attributes ``_RefsDb.build_member`` needs from a member."""

    def __init__(self, index, member_dir, fresh_from=0):
        self.index = index
        self.member_dir = member_dir
        self.fresh_from = fresh_from


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


def rehearse_linked_logs(groups, *, links=(), on_bad="strict",
                         quiesce=DEFAULT_QUIESCE):
    """Report what :func:`migrate_linked_logs` would do, without doing it.

    The arguments have exactly the same meaning and validation as
    :func:`migrate_linked_logs`; the call is strictly read-only: it
    acquires no leases or locks, creates no migration markers and never
    renames, truncates or appends to a member.  A lease held by an
    active migration instance is irrelevant -- no
    :class:`MigrationLockedError` is raised -- and concurrent appenders
    are never blocked or delayed.

    Every member's inode is pinned for the whole rehearsal and tailed
    with the migration's own read-only line follower, so findings keep
    their group / member / line positions while writers append or a real
    migration runs concurrently.  Returns a :class:`RehearsalReport`.
    """
    if on_bad not in ("strict", "skip"):
        raise ValueError(f"on_bad must be 'strict' or 'skip', got {on_bad!r}")
    groups = _normalize_groups(groups)
    links = _normalize_links(links, len(groups))
    paths = [path for group in groups for path in group]
    _probe_members(paths)

    dst_links = {}
    src_links = {}
    for link_id, (src_g, dst_g, src_field, dst_field) in enumerate(links):
        dst_links.setdefault(dst_g, []).append((link_id, dst_field))
        src_links.setdefault(src_g, []).append((link_id, src_field))

    scratch = _rehearse_dir(groups, links, on_bad)
    shutil.rmtree(scratch, ignore_errors=True)
    os.makedirs(scratch, exist_ok=True)

    # info per global member order: path, group/local indices, scratch
    # member directory, line counters and the dirty flag a real prepare
    # would compute.
    infos = []
    pinned = []
    try:
        # Pin every member's inode up front: one consistent
        # (group, member, line) space for the whole report, even against
        # a real migration renaming the paths while we work.  The open
        # rides out the sub-millisecond path->backup->path rename gap
        # exactly like a streaming snapshot does.
        for group in groups:
            for path in group:
                pinned.append(_open_path_wait(path, quiesce))

        global_index = 0
        for gi, group in enumerate(groups):
            for mi, path in enumerate(group):
                member_dir = os.path.join(scratch, f"m{global_index:06d}")
                os.makedirs(member_dir, exist_ok=True)
                src = pinned[global_index]
                index = _LineIndex(member_dir)
                index.begin(0, False)
                follow = max(1.0, quiesce * 20)
                offset = 0
                lineno = 0
                good = bad_lines = changed = 0
                try:
                    # The same read-only follower the real scan uses:
                    # whole lines only, one bounded follow window for an
                    # active appender, torn tail excluded.
                    for raw in _read_lines(src, quiesce, offset=0,
                                           follow=follow):
                        line_start = offset
                        offset += len(raw)
                        lineno += 1
                        try:
                            out = _convert(raw)
                        except ValueError:
                            kind = _classify_bad_line(raw)
                            index.record(line_start, kind, raw)
                            bad_lines += 1
                            changed += 1
                            continue
                        if raw != out:
                            changed += 1
                        index.record(line_start, LINE_GOOD, out)
                        good += 1
                    index.sync()
                finally:
                    index.close()
                infos.append({
                    "path": path, "gi": gi, "mi": mi, "gi_global": global_index,
                    "dir": member_dir, "lines": lineno, "good": good,
                    "bad": bad_lines, "changed": changed, "offset": offset,
                })
                global_index += 1

        # Resolve references through the same streaming SQLite spill the
        # migration builds from its durable line indexes.
        refs_path = os.path.join(scratch, "refs.sqlite3")
        refs = _RefsDb(refs_path)
        try:
            for info in infos:
                refs.build_member(
                    _IndexShim(info["gi_global"], info["dir"]),
                    dst_links.get(info["gi"], []),
                    src_links.get(info["gi"], []),
                )
            refs.validate()
            dropped_sets = refs.dropped_lines()
            bad_rows = refs._db.execute(
                "SELECT nodes.mi, nodes.lineno, edges.reason, "
                "edges.link_id, edges.value, edges.edge_id FROM edges "
                "JOIN nodes ON nodes.node_id=edges.src "
                "WHERE edges.reason IS NOT NULL "
                "ORDER BY nodes.mi, nodes.lineno, edges.reason, "
                "edges.edge_id"
            ).fetchall()
        finally:
            refs.close()

        def raw_at(global_mi, offset):
            fd = pinned[global_mi]
            fd.seek(offset)
            return fd.readline()

        # Bad-record findings: every bad line in global order, with the
        # exact cause a real strict run reports.
        findings = []
        for info in infos:
            for lineno0, offset, kind, _proj in _stream_line_entries(
                info["dir"]
            ):
                if kind == LINE_GOOD:
                    continue
                raw = raw_at(info["gi_global"], offset)
                try:
                    _convert(raw)
                    cause = "undecodable record"
                except ValueError as exc:
                    cause = str(exc)
                findings.append(RehearsalFinding(
                    group=info["gi"], member=info["mi"],
                    lineno=lineno0 + 1, kind="bad_record",
                    path=info["path"], raw=raw, detail=cause,
                ))

        # Bad-reference findings: one per bad edge, globally ordered.
        offsets_by_member = {}
        for info in infos:
            offsets_by_member[info["gi_global"]] = {
                lineno0: offset
                for lineno0, offset, kind, _proj in _stream_line_entries(
                    info["dir"])
                if kind == LINE_GOOD
            }
        for order, lineno0, reason, link_id, value, _eid in bad_rows:
            info = infos[order]
            _src_g, dst_g, _src_field, dst_field = links[link_id]
            detail = (
                f"{_REASON_TEXT[reason]}: "
                f"{dst_field}={value!r} in group {dst_g}"
            )
            raw = raw_at(order, offsets_by_member[order][lineno0])
            findings.append(RehearsalFinding(
                group=info["gi"], member=info["mi"],
                lineno=lineno0 + 1, kind="bad_reference",
                path=info["path"], raw=raw, detail=detail,
            ))

        # A bad line at the same position sorts ahead of a bad
        # reference (the two never actually share a line, but this is
        # the migration's tie rule).
        findings.sort(key=lambda f: (
            f.group, f.member, f.lineno,
            0 if f.kind == "bad_record" else 1,
        ))

        # Strict mode attributes the globally first problem; the bad
        # line / bad reference comparison is the one the migration
        # performs.
        first_error = (
            findings[0] if on_bad == "strict" and findings else None
        )

        # Skip-mode drop list: every bad line plus every good record
        # holding a bad reference (including cascades), globally
        # ordered.  Raws come from the pinned inodes, so this is built
        # before the descriptors are released.
        dropped = []
        if on_bad == "skip":
            positions = {}  # (global_mi, lineno0) -> ("why", raw)
            for info in infos:
                for lineno0, offset, kind, _proj in _stream_line_entries(
                    info["dir"]
                ):
                    if kind != LINE_GOOD:
                        positions[(info["gi_global"], lineno0)] = (
                            "bad_record", raw_at(info["gi_global"], offset)
                        )
            for order, line_set in dropped_sets.items():
                good_offsets = offsets_by_member[order]
                for lineno0 in line_set:
                    positions.setdefault(
                        (order, lineno0),
                        ("bad_reference",
                         raw_at(order, good_offsets[lineno0])),
                    )
            info_by_global = {info["gi_global"]: info for info in infos}
            for (order, lineno0), (why, raw) in sorted(positions.items()):
                info = info_by_global[order]
                dropped.append(RehearsalDropped(
                    group=info["gi"], member=info["mi"],
                    lineno=lineno0 + 1, path=info["path"], raw=raw, why=why,
                ))
    finally:
        for fd in pinned:
            try:
                fd.close()
            except OSError:
                pass
        # The scratch state is purely derived and always rebuildable;
        # never leave it behind, including on an error.
        shutil.rmtree(scratch, ignore_errors=True)

    members = []
    dropped_by_member = {}
    for d in dropped:
        dropped_by_member.setdefault((d.group, d.member), set()).add(d.lineno)
    bad_ref_lines_by_member = {}
    for f in findings:
        if f.kind == "bad_reference":
            bad_ref_lines_by_member.setdefault(
                (f.group, f.member), set()).add(f.lineno)
    any_replaced = False
    for info in infos:
        key = (info["gi"], info["mi"])
        dropped_here = dropped_by_member.get(key, set())
        # Mirrors migrate_linked_logs: staged iff the prepare was dirty
        # (a re-encoded or bad line) or at least one record is dropped.
        replaced = bool(info["changed"]) or bool(dropped_here)
        any_replaced = any_replaced or replaced
        if replaced:
            migrated = info["good"] - len(
                dropped_here & bad_ref_lines_by_member.get(key, set())
            )
        else:
            migrated = 0
        members.append(RehearsalMember(
            path=info["path"], group=info["gi"], member=info["mi"],
            total_lines=info["lines"], records_migrated=migrated,
            bad_records=info["bad"],
            bad_reference_lines=len(bad_ref_lines_by_member.get(key, set())),
            dropped_lines=len(dropped_here), replaced=replaced,
        ))

    report = RehearsalReport(
        on_bad=on_bad,
        groups=tuple(tuple(g) for g in groups),
        links=tuple(links),
        members=tuple(members),
        findings=tuple(findings),
        first_error=first_error,
        dropped=tuple(dropped),
        total_migrated=sum(m.records_migrated for m in members),
        total_bad_records=sum(m.bad_records for m in members),
        total_bad_references=sum(
            1 for f in findings if f.kind == "bad_reference"),
        total_dropped=len(dropped),
        replaced=any_replaced,
    )
    return report


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def run_rehearse_cli(argv):
    parser = argparse.ArgumentParser(
        prog="python3 -m proto_migrate rehearse-linked-logs",
        description="Read-only rehearsal of a linked migration: report "
        "per-member migration counts, bad records/references in global "
        "order, the strict-mode first error and the skip-mode drop "
        "list. Never modifies a file.",
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
                      dest="on_bad", help="attribute the first bad line "
                      "or bad reference a strict run would stop at "
                      "(default)")
    mode.add_argument("--skip", action="store_const", const="skip",
                      dest="on_bad", help="also list every record a skip "
                      "run would drop")
    parser.set_defaults(on_bad="strict")
    parser.add_argument("--quiesce-ms", type=float,
                        default=DEFAULT_QUIESCE * 1000, metavar="MS",
                        help="EOF must hold this long for a line to be "
                        "considered complete while appenders run "
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
            quiesce=args.quiesce_ms / 1000.0,
        )
    except (TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_USAGE
    except OSError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_ERROR

    for m in report.members:
        print(
            "member group=%d index=%d path=%s lines=%d migrated=%d "
            "bad_records=%d bad_reference_lines=%d dropped=%d %s"
            % (
                m.group, m.member, m.path, m.total_lines,
                m.records_migrated, m.bad_records,
                m.bad_reference_lines, m.dropped_lines,
                "would-replace" if m.replaced else "untouched",
            )
        )
    for d in report.dropped:
        print("drop group=%d index=%d %s:%d %s %r"
              % (d.group, d.member, d.path, d.lineno, d.why, d.snippet))
    if report.first_error is not None:
        f = report.first_error
        print(
            "error: first %s at group=%d member=%d %s:%d: %s"
            % (f.kind.replace("_", " "), f.group, f.member,
               f.path, f.lineno, f.detail),
            file=sys.stderr,
        )
        return EXIT_BAD_RECORD
    print(
        "totals migrated=%d bad_records=%d bad_references=%d dropped=%d "
        "replaced=%s"
        % (
            report.total_migrated, report.total_bad_records,
            report.total_bad_references, report.total_dropped,
            "yes" if report.replaced else "no",
        )
    )
    return EXIT_OK
