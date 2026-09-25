"""Group-level, all-or-nothing migration of a set of append-only JSONL logs.

This is the batch companion of :mod:`proto_migrate.log_migration`: the
baseline migrates one file online; :func:`migrate_log_group` migrates a
whole *group* of log files in one shot.  The group is indivisible --
either every member ends up at the current version or every member
stays exactly as it was; no partially migrated group is ever left in
durable state after a handled failure.

Members are named explicitly by the caller, and every member may be
appended to by a different process while the migration runs; each
member keeps the same segmented temp output and durable local
checkpoint as a single-file run, so the whole group remains losslessly
rollback-able until the commit phase.

Group commit protocol
---------------------
The unified rename is a cross-file, recoverable two-phase protocol.
Every rename is inside the member's own directory (atomic on one
filesystem, no cross-mount moves).  For every member whose bytes
actually change, the phase-1 sequence is:

  1. ``rename(<member-workdir>/final, <path>.migrate-group-staged)``
  2. publish the per-file commit marker ``<path>.committed``
  3. ``rename(<path>, <path>.migrate-group-backup)``
  4. ``rename(<path>.migrate-group-staged, <path>)``

Only once *every* member has completed phase 1 does the run publish
the group commit marker ``group-committed`` in the group work
directory.  That publication is the single success/failure border.
Phase 2 converges racing appenders, deletes the backups and sweeps the
work directories; any durability or cleanup failure from the border on
is reported as a warning, never as a failure (the run exits 0).

A kill at any point reruns deterministically:

  * group marker missing  -> every member rename is reversed (whatever
    stage it had reached), member checkpoints resume without a rescan
    and the renames are performed again;
  * group marker present  -> remaining renames are completed forward:
    the staged file is promoted, late writes on the backup inode are
    converged in order, then the backup is removed.

Checkpoints and resume
----------------------
Group metadata (lock, manifest, group marker) lives in
``<dir-of-first-member>/.migrate-logs-tmp/``; each member's segments,
checkpoint, assembled ``final`` and ``prepared`` marker live in that
member's own sibling directory ``<path>.migrate-group-tmp/``.  A member
that finished its prepare phase records a durable ``prepared`` marker;
on rerun its scan, assembly and drain are skipped entirely -- its
checkpoint is not rescanned and its output is not rewritten -- and the
finished group is byte-for-byte identical to one uninterrupted run.
(A handled pre-commit failure, like a strict-mode bad record, sweeps
this state instead, exactly as the single-file entry does; only a real
process kill preserves it for resume.)

Bad records use the exact single-file rules.  In ``"strict"`` mode the
first bad line of the whole group (members in list order, lines in
file order) raises :class:`GroupBadRecordError` (a :class:`ValueError`)
before any original is touched; in ``"skip"`` mode one audit line per
skipped record is written to the audit stream as
``<filename>:<lineno>:<first 32 raw bytes>``.

:func:`read_log_group` returns one consistent snapshot of the whole
group in a single call: field versions never mix between members or
inside a member, even while a path-reopening appender writes old
records after wrap-up.
"""

from __future__ import annotations

import argparse
import fcntl
import json
import os
import shutil
import sys
import time
from collections.abc import Sequence
from typing import NamedTuple

from . import CURRENT_VERSION, migrate
from .log_migration import (
    DEFAULT_QUIESCE,
    DEFAULT_SEGMENT_SIZE,
    EXIT_BAD_RECORD,
    EXIT_ERROR,
    EXIT_OK,
    EXIT_USAGE,
    LINE_GOOD,
    BadRecordError,
    MigrationResult,
    _assemble,
    _classify_bad_line,
    _commit_marker_path,
    _complete_lines,
    _converge,
    _crash_point,
    _emit,
    _fsync_dir,
    _prepare_workdir,
    _publish_prefix_checkpoint,
    _read_checkpoint_log,
    _read_lines,
)

__all__ = [
    "GroupBadRecordError",
    "GroupMigrationResult",
    "migrate_log_group",
    "read_log_group",
    "run_group_cli",
]

_GROUP_DIR_NAME = ".migrate-logs-tmp"
_GROUP_LOCK_NAME = ".migrate-logs.lock"
_GROUP_COMMITTED = "group-committed"
_GROUP_COMMITTED_TMP = "group-committed.tmp"
_MANIFEST = "manifest.json"
_PREPARED = "prepared"
_FINAL = "final"

_MEMBER_TMP_SUFFIX = ".migrate-group-tmp"
_BACKUP_SUFFIX = ".migrate-group-backup"
_STAGED_SUFFIX = ".migrate-group-staged"

# Mirrors log_migration._FAULT_ENV; post-border cleanup faults injected
# through this hook are warnings rather than failures.
_FAULT_ENV = "PROTO_MIGRATE_FAULT_AT"


class GroupBadRecordError(BadRecordError):
    """Strict-mode bad record, annotated with the member path."""

    def __init__(self, path, lineno, raw, cause):
        self.path = path
        super().__init__(lineno, raw, cause)

    def __str__(self):
        return f"{self.path}: line {self.lineno}: {self.cause}"


class GroupMigrationResult(NamedTuple):
    paths: tuple
    records_migrated: int
    records_skipped: int
    records_salvaged: int
    replaced: bool
    members: tuple
    post_commit_error: str | None = None


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def _normalize_paths(paths):
    """Validate the caller-given member list; return paths as given.

    A non-sequence (including a single path string) is a TypeError; an
    empty list or a repeated path is a ValueError.
    """
    if isinstance(paths, (str, bytes, os.PathLike)) or not isinstance(
        paths, Sequence
    ):
        raise TypeError("paths must be a sequence of path-like members")
    out = []
    seen = set()
    for item in paths:
        path = os.fspath(item)
        if isinstance(path, bytes):
            path = os.fsdecode(path)
        absolute = os.path.abspath(path)
        if absolute in seen:
            raise ValueError(f"duplicate group member: {path!r}")
        seen.add(absolute)
        out.append(path)
    if not out:
        raise ValueError("group member list is empty")
    return out


def _probe_members(paths):
    """Every member must exist and be readable; else FileNotFoundError."""
    for path in paths:
        try:
            with open(path, "rb"):
                pass
        except OSError as exc:
            # Missing *or* unreadable: both entrances report the same
            # FileNotFoundError per contract.
            raise FileNotFoundError(
                f"group member missing or unreadable: {path!r}: {exc}"
            ) from exc


class _AuditPrefix:
    """Prefix every single-file audit entry with the member file name."""

    def __init__(self, stream, path):
        self._stream = stream
        self._prefix = os.fspath(path).encode("utf-8", "surrogateescape")

    def write(self, data):
        # log_migration._audit_line emits one b"<lineno>:<snippet>\n".
        self._stream.write(self._prefix + b":" + data)

    def flush(self):
        self._stream.flush()


# ---------------------------------------------------------------------------
# Work-directory layout and bookkeeping
# ---------------------------------------------------------------------------


def _group_dir_for(paths):
    first_dir = os.path.dirname(os.path.abspath(paths[0]))
    return os.path.join(first_dir, _GROUP_DIR_NAME)


def _member_workdir(path):
    # Adjacent to the member so every rename in the protocol is atomic
    # within one directory (members may live on different filesystems).
    return path + _MEMBER_TMP_SUFFIX


def _read_manifest(group_dir):
    try:
        with open(os.path.join(group_dir, _MANIFEST), "rb") as f:
            return json.loads(f.read())
    except (OSError, ValueError):
        return None


def _write_manifest(group_dir, paths, on_bad):
    data = {
        "version": 1,
        "on_bad": on_bad,
        "members": [os.path.abspath(p) for p in paths],
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
        for key in ("dirty", "offset", "lineno", "migrated", "skipped"):
            if key not in data:
                return None
        return data
    except (OSError, ValueError):
        return None


def _debris(path):
    marker = _commit_marker_path(path)
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


def _sweep_dirs(paths, group_dir):
    """Post-border best-effort sweep; OSEXT errors stay with the caller."""
    for path in paths:
        shutil.rmtree(_member_workdir(path), ignore_errors=True)
    shutil.rmtree(group_dir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Rename debris reconciliation (crash recovery)
# ---------------------------------------------------------------------------
#
# Phase 1 per member: final -> staged ; publish marker ;
#                     path -> backup ; staged -> path
#
# Surviving state                 committed forward      uncommitted undo
# staged only, path is original   finish staging         rm staged (+marker)
# staged + backup, no path        staged -> path         backup -> path; rm staged
# backup only, path is migrated   keep for convergence   path -> staged; backup -> path
# nothing                         (already finished)     untouched


def _reconcile_member(path, committed):
    """Resolve one member's interrupted rename state, deterministically."""
    d = _debris(path)
    parent = os.path.dirname(os.path.abspath(path))
    staged = os.path.exists(d["staged"])
    backup = os.path.exists(d["backup"])
    live = os.path.exists(path)
    touched = staged or backup

    if committed:
        # Complete the rename forward; backups are retained until the
        # finishing run has converged late writes on the old inode.
        if staged and live and not backup:
            os.replace(path, d["backup"])
            os.replace(d["staged"], path)
            _fsync_dir(parent)
        elif staged and backup and not live:
            os.replace(d["staged"], path)
            _fsync_dir(parent)
        # backup+live (normal pre-phase-2 state) and the no-debris state
        # need no rename.
        _remove(d["marker_tmp"])
        _fsync_dir(parent)
        return

    # No group commit marker: undo this attempt's renames if any.  The
    # per-file commit marker is deliberately LEFT in place whether this
    # attempt just published it or an earlier run had: with the
    # original bytes restored, read_log's post-marker policy normalizes
    # them in memory (uniform current-version view, exactly the content
    # the next rerun commits) instead of exposing a raw mix.
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
# Per-member prepare (scan, assemble, drain) with checkpoint resume
# ---------------------------------------------------------------------------


class _PreparedMember:
    def __init__(self, path, member_dir, src, dirty, offset, lineno,
                 run_migrated, run_skipped):
        self.path = path
        self.member_dir = member_dir
        self.src = src
        self.dirty = dirty
        # Drain-end offset / line number on the source inode: the start
        # state the convergence phase continues from.
        self.offset = offset
        self.lineno = lineno
        # Records this invocation newly migrates; salvaged is added
        # after the border.  Fast-resumed/finishing runs count zero.
        self.run_migrated = run_migrated
        self.run_skipped = run_skipped
        self.salvaged = 0


def _resume_prepared(path, member_dir, on_bad):
    """Fast path: a member fully prepared before the kill is not redone."""
    info = _read_prepared(member_dir)
    if info is None:
        return None
    inode, mode, _records, _good = _read_checkpoint_log(member_dir)
    if inode != os.stat(path).st_ino or mode != on_bad:
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
    return _PreparedMember(path, member_dir, src, bool(info["dirty"]),
                           int(info["offset"]), int(info["lineno"]), 0, 0)


def _prepare_member(path, member_dir, on_bad, segment_size, quiesce,
                    audit_stream, line_index=None):
    """Scan one member to a durable ``final`` (or prove it is canonical).

    When *line_index* is given (the linked-group migration) every
    consumed source line is recorded in it -- offset, kind and a
    projection of the link-involved fields -- and the index is fsynced
    before each checkpoint record so a checkpoint never covers lines
    the index has not durably classified.
    """
    current_inode = os.stat(path).st_ino
    state = _prepare_workdir(
        path, member_dir, segment_size, current_inode, on_bad
    )
    writer = state["writer"]
    cp_fh = state["cp_fh"]
    offset = state["offset"]
    migrated = state["count"]
    skipped = state["skipped"]
    start_migrated = migrated
    start_skipped = skipped
    dirty = state["dirty"]
    lineno = migrated + skipped
    follow_window = max(1.0, quiesce * 20)
    if line_index is not None:
        line_index.begin(migrated + skipped, state["resumed"])

    src = open(path, "rb")
    try:
        # --- SCAN: the same tailing policy as a single-file run.
        for raw in _read_lines(src, quiesce, offset=offset,
                               follow=follow_window):
            lineno += 1
            line_start = offset
            offset += len(raw)
            try:
                out, bad = _emit(audit_stream, on_bad, raw, lineno)
            except BadRecordError as exc:
                raise GroupBadRecordError(
                    path, exc.lineno, exc.raw, exc.cause
                ) from exc
            if bad:
                skipped += 1
                dirty = True
                if line_index is not None:
                    line_index.record(line_start, _classify_bad_line(raw),
                                      raw)
                continue
            if raw != out:
                dirty = True
            if line_index is not None:
                line_index.record(line_start, LINE_GOOD, out)
            writer.write(out)
            migrated += 1
            if writer.size >= writer.limit:
                name = writer.current_segment
                writer.close()
                if line_index is not None:
                    line_index.sync()
                _publish_prefix_checkpoint(
                    cp_fh, member_dir, offset, migrated, skipped,
                    dirty, name,
                    os.path.getsize(os.path.join(member_dir, name)),
                )

        if writer.has_open_segment:
            name = writer.current_segment
            writer.close()
            if line_index is not None:
                line_index.sync()
            _publish_prefix_checkpoint(
                cp_fh, member_dir, offset, migrated, skipped, dirty,
                name, os.path.getsize(os.path.join(member_dir, name)),
            )
    except BaseException:
        writer.abort()
        cp_fh.close()
        src.close()
        if line_index is not None:
            line_index.close()
        raise
    writer.abort()
    cp_fh.close()

    if not dirty:
        # Canonical member (and nothing skipped): it is never renamed.
        if line_index is not None:
            line_index.sync()
            line_index.close()
        _write_prepared(member_dir, {
            "dirty": False, "offset": offset, "lineno": lineno,
            "migrated": migrated, "skipped": skipped,
        })
        return _PreparedMember(
            path, member_dir, src, False, offset, lineno,
            migrated - start_migrated, skipped - start_skipped,
        )

    # --- ASSEMBLE + DRAIN: final is the rename source in phase 1.
    segments = sorted(
        n for n in os.listdir(member_dir) if n.startswith("seg-")
    )
    final = _assemble(member_dir, segments,
                      os.path.join(member_dir, _FINAL))
    try:
        with open(final, "ab") as out_fh:
            for raw in _read_lines(src, quiesce, offset=offset,
                                   follow=follow_window):
                lineno += 1
                line_start = offset
                offset += len(raw)
                try:
                    out, bad = _emit(audit_stream, on_bad, raw, lineno)
                except BadRecordError as exc:
                    raise GroupBadRecordError(
                        path, exc.lineno, exc.raw, exc.cause
                    ) from exc
                if bad:
                    skipped += 1
                    if line_index is not None:
                        line_index.record(line_start,
                                          _classify_bad_line(raw), raw)
                    continue
                if line_index is not None:
                    line_index.record(line_start, LINE_GOOD, out)
                out_fh.write(out)
                migrated += 1
            out_fh.flush()
            os.fsync(out_fh.fileno())
    except BaseException:
        src.close()
        if line_index is not None:
            line_index.close()
        raise

    if line_index is not None:
        line_index.sync()
        line_index.close()
    _write_prepared(member_dir, {
        "dirty": True, "offset": offset, "lineno": lineno,
        "migrated": migrated, "skipped": skipped,
    })
    return _PreparedMember(
        path, member_dir, src, True, offset, lineno,
        migrated - start_migrated, skipped - start_skipped,
    )


# ---------------------------------------------------------------------------
# Two-phase rename
# ---------------------------------------------------------------------------


def _publish_member_marker(path):
    marker = _commit_marker_path(path)
    parent = os.path.dirname(os.path.abspath(path))
    with open(marker + ".tmp", "wb") as f:
        f.write(b"group\n")
        f.flush()
        os.fsync(f.fileno())
    os.replace(marker + ".tmp", marker)
    _fsync_dir(parent)


def _stage_member(member):
    """Phase 1: promote this member's final over its path; backup held."""
    path = member.path
    parent = os.path.dirname(os.path.abspath(path))
    d = _debris(path)
    os.replace(os.path.join(member.member_dir, _FINAL), d["staged"])
    _publish_member_marker(path)
    os.replace(path, d["backup"])
    os.replace(d["staged"], path)
    _fsync_dir(parent)


def _undo_staged_members(members):
    """Reverse phase 1 for members already staged (best effort)."""
    failed = False
    for member in members:
        try:
            _reconcile_member(member.path, committed=False)
        except OSError:
            # The backup is still on disk; a rerun completes rollback.
            failed = True
    return failed


def _acquire_member_locks(paths):
    """Take every member's single-file lock (list order, all-or-nothing)."""
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
# Post-border finishing: convergence, backups, cleanup
# ---------------------------------------------------------------------------


def _finish_committed_group(paths, group_dir, on_bad, segment_size,
                            quiesce, audit_stream):
    """Finish a group whose commit marker already exists (a rerun).

    The border was already crossed, so every fault here is a warning
    and nothing is allowed to turn the committed migration into a
    failure.  Reconciliation already promoted every member's renamed
    inode; this finishes exactly like an idempotent single-file rerun
    per member: old per-member checkpoints name the pre-rename inode so
    they are discarded, the current inode is rescanned (re-encoding a
    canonical file changes no bytes), any old-format tail a
    path-reopening appender landed after wrap-up is migrated through a
    fresh atomic replacement, and racing appenders are converged.
    Counters count only records *this* invocation newly moves (the
    post-commit tails); bytes the killed run migrated are not counted.
    """
    warnings = []
    member_results = []
    salvaged_total = skipped_total = migrated_total = 0

    for path in paths:
        parent = os.path.dirname(os.path.abspath(path))
        d = _debris(path)
        # The backup is the pre-rename inode: every record reachable
        # through a path was drained before staging, so writes still
        # landing on it after the kill are the same "preempted between
        # open and write" boundary a single-file rerun documents.
        try:
            _remove(d["backup"])
            _fsync_dir(parent)
        except OSError as exc:
            warnings.append(f"{path}: backup removal failed: {exc}")

        member_dir = _member_workdir(path)
        shutil.rmtree(member_dir, ignore_errors=True)
        try:
            member = _prepare_member(
                path, member_dir, on_bad, segment_size, quiesce,
                _AuditPrefix(audit_stream, path),
            )
            member_replaced = False
            if member.dirty:
                _stage_member(member)
                member_replaced = True
                salvaged, conv_skipped, lineno, warning = _converge(
                    path, parent, member_dir, member.src, member.offset,
                    member.lineno, quiesce,
                    _AuditPrefix(audit_stream, path), on_bad,
                )
                member.salvaged = salvaged
                member.run_skipped = conv_skipped
                member.lineno = lineno
                salvaged_total += salvaged
                migrated_total += salvaged
                skipped_total += conv_skipped
                if warning:
                    warnings.append(f"{path}: {warning}")
                _remove(path + _BACKUP_SUFFIX)
                _fsync_dir(parent)
            try:
                member.src.close()
            except OSError:
                pass
            member_results.append(MigrationResult(
                path=path, records_migrated=member.salvaged,
                records_skipped=member.run_skipped,
                records_salvaged=member.salvaged,
                replaced=member_replaced,
            ))
        except (OSError, ValueError) as exc:
            # Border already crossed: even a bad tail record cannot
            # un-commit the group; report it as a warning (rerun with
            # --skip to migrate past it).
            warnings.append(
                f"{path}: post-commit finishing incomplete, rerun to "
                f"finish: {exc}"
            )
            try:
                shutil.rmtree(member_dir, ignore_errors=True)
            except OSError:
                pass

    try:
        _sweep_dirs(paths, group_dir)
    except OSError as exc:
        warnings.append(f"cleanup failed: {exc}")
    replaced = any(m.replaced for m in member_results)
    return migrated_total, skipped_total, salvaged_total, \
        tuple(member_results), warnings, replaced


def _converge_and_finalize(dirty_members, paths, group_dir, on_bad,
                           quiesce, audit_stream):
    """Converge racing appenders, delete backups, sweep work dirs."""
    warnings = []
    salvaged_total = skipped_total = 0
    for member in dirty_members:
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

    _crash_point("group-backups")
    try:
        _crash_point("group-cleanup")
        if os.environ.get(_FAULT_ENV) == "group-cleanup":
            raise OSError("injected group cleanup fault")
        _sweep_dirs(paths, group_dir)
    except OSError as exc:
        warnings.append(f"cleanup failed: {exc}")
    return salvaged_total, skipped_total, warnings


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


def migrate_log_group(paths, *, on_bad="strict",
                      segment_size=DEFAULT_SEGMENT_SIZE,
                      quiesce=DEFAULT_QUIESCE, audit=None):
    """Migrate an explicitly listed group of JSONL logs as one unit.

    Either every member finishes at the current version or every member
    is left unchanged.  Members may be appended to by different
    processes; each member uses segmented temp output plus durable local
    checkpoints, and a killed run resumes so prepared members are
    neither rescanned nor rewritten.  Returns
    :class:`GroupMigrationResult` whose counters count only records this
    invocation newly migrates.

    Raises :class:`TypeError` for a non-sequence member list,
    :class:`ValueError` for an empty or duplicate list (and
    :class:`GroupBadRecordError` on the first bad line in strict mode),
    and :class:`FileNotFoundError` when a member is missing or
    unreadable.
    """
    if on_bad not in ("strict", "skip"):
        raise ValueError(f"on_bad must be 'strict' or 'skip', got {on_bad!r}")
    if segment_size <= 0:
        raise ValueError("segment_size must be positive")
    paths = _normalize_paths(paths)
    _probe_members(paths)

    audit_stream = audit if audit is not None else sys.stderr.buffer
    first_dir = os.path.dirname(os.path.abspath(paths[0]))
    group_dir = _group_dir_for(paths)
    lock_path = os.path.join(first_dir, _GROUP_LOCK_NAME)

    members = []
    dirty_members = []
    replaced = False
    post_commit_error = None

    with open(lock_path, "a+b") as group_lock:
        fcntl.flock(group_lock, fcntl.LOCK_EX)
        member_locks = _acquire_member_locks(paths)
        try:
            _crash_point("group-lock")

            manifest = _read_manifest(group_dir)
            committed = os.path.exists(
                os.path.join(group_dir, _GROUP_COMMITTED)
            )
            abs_paths = [os.path.abspath(p) for p in paths]
            if manifest is not None:
                same_group = (
                    manifest.get("members") == abs_paths
                    and manifest.get("on_bad") == on_bad
                )
                if not same_group:
                    if committed:
                        raise ValueError(
                            "group commit marker exists for a different "
                            "member list or policy; refusing to mix groups"
                        )
                    # An earlier uncommitted attempt for a different
                    # group used the same anchor directory: reverse its
                    # renames, then sweep its work directories.
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
                _write_manifest(group_dir, paths, on_bad)
            _remove(os.path.join(group_dir, _GROUP_COMMITTED_TMP))

            # Resolve interrupted renames before touching content.
            for path in paths:
                _reconcile_member(path, committed)

            if committed:
                mig, conv_skipped, salvaged, member_res, warnings, \
                    any_replaced = _finish_committed_group(
                        paths, group_dir, on_bad, segment_size, quiesce,
                        audit_stream
                    )
                return GroupMigrationResult(
                    paths=tuple(paths),
                    records_migrated=mig,
                    records_skipped=conv_skipped,
                    records_salvaged=salvaged,
                    replaced=any_replaced,
                    members=member_res,
                    post_commit_error="; ".join(warnings) or None,
                )

            # --- PREPARE every member (resumed via local checkpoints).
            try:
                for index, path in enumerate(paths):
                    member_dir = _member_workdir(path)
                    member_audit = _AuditPrefix(audit_stream, path)
                    prepared = _resume_prepared(path, member_dir, on_bad)
                    if prepared is None:
                        prepared = _prepare_member(
                            path, member_dir, on_bad, segment_size,
                            quiesce, member_audit,
                        )
                    members.append(prepared)
                _crash_point("group-prepare")

                dirty_members = [m for m in members if m.dirty]
                if not dirty_members:
                    # Whole group already canonical: nothing renamed; no
                    # group marker is published and all work is swept
                    # (mirrors single-file idempotency).
                    _sweep_dirs(paths, group_dir)
                    replaced = False
                else:
                    # --- PHASE 1: stage every member.
                    staged = []
                    try:
                        for member in dirty_members:
                            _stage_member(member)
                            staged.append(member)
                            _crash_point("group-stage")
                        _crash_point("group-staged")
                    except BaseException:
                        if not _undo_staged_members(staged):
                            _sweep_dirs(paths, group_dir)
                        raise

                    # --- COMMIT POINT: publish the group marker -----
                    # This rename is the single success/failure border.
                    marker = os.path.join(group_dir, _GROUP_COMMITTED)
                    marker_tmp = os.path.join(
                        group_dir, _GROUP_COMMITTED_TMP
                    )
                    with open(marker_tmp, "wb") as f:
                        f.write(b"1\n")
                        f.flush()
                        os.fsync(f.fileno())
                    os.replace(marker_tmp, marker)
                    _fsync_dir(group_dir)
                    _crash_point("group-marker")
                    replaced = True

                    # --- PHASE 2: converge, drop backups, sweep.
                    _salvaged, _conv_skipped, warnings = (
                        _converge_and_finalize(
                            dirty_members, paths, group_dir, on_bad,
                            quiesce, audit_stream
                        )
                    )
                    post_commit_error = "; ".join(warnings) or None
            except BaseException:
                # A handled pre-commit failure leaves no partial
                # intermediate in durable state, and no original is
                # renamed past this point (rollback above restores any
                # staged member).  A true kill (SIGKILL/os._exit) never
                # reaches here, so durable checkpoints survive for the
                # rerun.
                if not os.path.exists(
                        os.path.join(group_dir, _GROUP_COMMITTED)) \
                        and not any(os.path.exists(m.path + _BACKUP_SUFFIX)
                                    for m in members):
                    _sweep_dirs(paths, group_dir)
                raise
        finally:
            for member in members:
                try:
                    member.src.close()
                except OSError:
                    pass
            for fh in member_locks:
                fcntl.flock(fh, fcntl.LOCK_UN)
                fh.close()

    totals_migrated = sum(m.run_migrated for m in dirty_members)
    totals_skipped = sum(m.run_skipped for m in dirty_members)
    totals_salvaged = sum(m.salvaged for m in dirty_members)
    member_results = tuple(
        MigrationResult(
            path=m.path,
            records_migrated=m.run_migrated,
            records_skipped=m.run_skipped,
            records_salvaged=m.salvaged,
            replaced=replaced and m.dirty,
        )
        for m in members
    )
    return GroupMigrationResult(
        paths=tuple(paths),
        records_migrated=totals_migrated,
        records_skipped=totals_skipped,
        records_salvaged=totals_salvaged,
        replaced=replaced,
        members=member_results,
        post_commit_error=post_commit_error,
    )


# ---------------------------------------------------------------------------
# Consistent group snapshot
# ---------------------------------------------------------------------------


def _read_member_gate(path, quiesce):
    """One pinned-inode read; return ``(records, post_commit_side)``.

    Mirrors :func:`proto_migrate.log_migration.read_log`'s marker gate:
    the descriptor pins a single inode for the whole read, the marker
    is rechecked afterwards, and only newline-terminated lines count.
    The member path is absent for the sub-millisecond window between
    the two renames of the group protocol; that is retried rather than
    surfacing as a spurious FileNotFoundError.
    """

    def open_blob():
        try:
            with open(path, "rb") as fh:
                return fh.read()
        except FileNotFoundError:
            return None

    marker = _commit_marker_path(path)
    marker_present = os.path.exists(marker)
    if not marker_present:
        blob = open_blob()
        if blob is None:
            # Mid-rename: the marker must have just been published.
            blob = _wait_path(path, quiesce)
            return (
                [migrate(rec, CURRENT_VERSION)
                 for rec in _complete_lines(blob)],
                True,
            )
        if not os.path.exists(marker):
            return _complete_lines(blob), False
    blob = open_blob()
    if blob is None:
        blob = _wait_path(path, quiesce)
    return (
        [migrate(rec, CURRENT_VERSION) for rec in _complete_lines(blob)],
        True,
    )


def _wait_path(path, quiesce):

    deadline = time.monotonic() + max(0.05, quiesce * 10)
    while True:
        try:
            with open(path, "rb") as fh:
                return fh.read()
        except FileNotFoundError:
            if time.monotonic() >= deadline:
                raise
            time.sleep(max(quiesce, 0.001))


def read_log_group(paths, *, quiesce=DEFAULT_QUIESCE):
    """Return one version-consistent snapshot of every member at once.

    A flat list of decoded records is returned: members in list order,
    then lines in file order.  Across the whole snapshot pre-current
    (v1/v2) and current (v3) field shapes never mix:

      * while no member has crossed its commit border and every stored
        record is pre-current, records are handed back exactly as
        stored (v1 and v2 together are the uniform "old" world, exactly
        as in :func:`~proto_migrate.log_migration.read_log`);
      * otherwise -- at least one member has crossed its border or
        already holds current-version bytes -- every member is
        normalized to the current version in memory.  Deterministic
        migration makes that equal to the eventual committed content
        even while the cross-file rename is in flight.

    A pinned inode per member plus the post-read marker recheck means a
    single snapshot can never straddle two inodes of one member, and a
    path-reopening appender's old-format tail after wrap-up is served
    normalized.  The member list is validated exactly like
    :func:`migrate_log_group`; a complete but undecodable line raises
    ValueError, like :func:`proto_migrate.loads`.

    The *quiesce* argument is accepted for API symmetry; a snapshot
    never blocks on an active appender.
    """
    paths = _normalize_paths(paths)
    _probe_members(paths)

    snapshots = [_read_member_gate(path, quiesce) for path in paths]
    any_post = any(post for _records, post in snapshots)
    all_old = all(
        not post and all(rec["v"] != CURRENT_VERSION for rec in records)
        for records, post in snapshots
    )
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


def run_group_cli(argv):
    parser = argparse.ArgumentParser(
        prog="python3 -m proto_migrate migrate-logs",
        description="Migrate a group of append-only JSONL logs as one "
        "atomic unit: all members end at the current version or all "
        "stay untouched; online (writers keep appending), resumable "
        "from durable checkpoints.",
    )
    parser.add_argument("paths", nargs="+", metavar="FILE",
                        help="group member log files (explicit list)")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--strict", action="store_const", const="strict",
                      dest="on_bad", help="abort on the first bad line "
                      "of the group (default; exit %d, files untouched)"
                      % EXIT_BAD_RECORD)
    mode.add_argument("--skip", action="store_const", const="skip",
                      dest="on_bad", help="skip bad lines, auditing "
                      "each to stderr as "
                      "'<file>:<lineno>:<first 32 bytes>'")
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

    for path in args.paths:
        if not os.path.isfile(path):
            print(f"error: not a file: {path}", file=sys.stderr)
            return EXIT_ERROR
    try:
        result = migrate_log_group(
            args.paths,
            on_bad=args.on_bad,
            segment_size=args.segment_size,
            quiesce=args.quiesce_ms / 1000,
        )
    except BadRecordError as exc:
        print(f"error: bad record at {exc}", file=sys.stderr)
        return EXIT_BAD_RECORD
    except (TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_USAGE
    except OSError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_ERROR
    print(
        "members=%d migrated=%d skipped=%d salvaged=%d replaced=%s"
        % (
            len(result.paths),
            result.records_migrated,
            result.records_skipped,
            result.records_salvaged,
            "yes" if result.replaced else "no",
        )
    )
    if result.post_commit_error:
        # The atomic group commit already succeeded: later faults show
        # up as a warning, never as a non-zero exit.
        print(f"warning: {result.post_commit_error}", file=sys.stderr)
    return EXIT_OK
