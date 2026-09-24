"""Crash-safe, resumable online migration of append-only JSONL logs.

Reads a log of mixed-version records (one compact JSON object per line,
as produced by :func:`proto_migrate.dumps`) in a single streaming pass,
rewrites every record at the current version, and atomically replaces
the original file -- while other processes keep appending and reading.

Online migration: the migration never asks appenders to stop or wait
for a quiet window.  Records appended while the migration runs are
drained into the output (the reader keeps consuming the file until it
stays at EOF for a quiesce window), so appended records are neither
lost nor duplicated and keep their original order.  After the atomic
rename a bounded convergence phase handles two writer shapes: a writer
still holding the old append descriptor lands records on the unlinked
old inode, which is drained and appended, migrated, to the new file; a
writer opening the path by name after the rename may land an old-format
record directly on the new inode, in which case that tail is rewritten
through one more atomic replacement.  Either way the path never names a
file with mixed old/new encodings.  Appenders must write whole records
in one O_APPEND write.  A writer that appends non-stop past the
convergence deadline (``quiesce * 20``, at least 1 s) is finished by a
plain idempotent rerun instead of waiting forever.

Crash safety and resume (断点续传): output is written to segmented
temporary files next to the target, every segment is fsynced, and after
each segment a checkpoint -- source inode, consumed source offset,
record counts, segment inventory -- is appended to a checkpoint file
and fsynced.  If the process is killed at any point and rerun, the run
resumes from the newest checkpoint that still validates: records
already migrated are not re-scanned and not re-written, and the resumed
result is byte-for-byte identical to a single uninterrupted run.  A
torn checkpoint tail, a corrupt checkpoint line, a missing segment
file, or an over-long assembled file (killed mid tail-drain) is
detected at validation time and rolled back to the newest checkpoint
that still validates -- all the way back to a fresh start if none
does.  A source file whose identity changed (dev/inode) invalidates
every checkpoint.  The assembled output is fsynced and atomically
renamed over the original (``os.replace``), followed by an fsync of the
containing directory.  The rename is the commit point: durability or
cleanup failures after it are reported as warnings only, never as a
failed migration.

Concurrent readers: ``os.replace`` is atomic, so at any instant a
reader opening the path sees either the whole original file or the
whole migrated file -- never a mix of old and new record versions in
one view.  Readers holding an already-open file descriptor keep reading
the old inode, which is the usual rename semantics.

Bad records: a line that fails to decode or validate (bad JSON, missing
or non-integer ``v``, unsupported version, wrong field types, NaN or
Infinity amounts -- including numeric literals such as ``1e999`` that
overflow to infinity) is handled according to ``on_bad``: ``"strict"``
aborts the whole run with :class:`BadRecordError` (a ValueError) and
leaves the original file untouched; ``"skip"`` skips the line and emits
an audit entry ``<lineno>:<first 32 raw bytes>`` per skipped line.

Idempotency: if every record is already at the current version in
canonical encoding and nothing is skipped, the file is left byte-for-byte
untouched (no rename, no rewrite).
"""

from __future__ import annotations

import argparse
import fcntl
import json
import os
import shutil
import sys
import time
import warnings
from typing import NamedTuple

from . import CURRENT_VERSION, dumps, loads, migrate

__all__ = [
    "BadRecordError",
    "MigrationResult",
    "migrate_log_file",
    "run_cli",
    "EXIT_OK",
    "EXIT_ERROR",
    "EXIT_USAGE",
    "EXIT_BAD_RECORD",
]

DEFAULT_SEGMENT_SIZE = 16 * 1024 * 1024
DEFAULT_QUIESCE = 0.05

EXIT_OK = 0
EXIT_ERROR = 1
EXIT_USAGE = 2
EXIT_BAD_RECORD = 3

# Testing hook: when this environment variable names a checkpoint, the
# process kills itself (os._exit, no cleanup) upon reaching it, to
# simulate a crash mid-migration.  Checkpoints: "lock", "segment",
# "segments", "assemble", "replace".
_CRASH_ENV = "PROTO_MIGRATE_CRASH_AT"

_AUDIT_SNIPPET = 32

# Checkpoint file (one JSON object per line) inside the tmp dir.  Each
# line is a self-contained commit record; the newest line that still
# validates wins, so a torn or corrupt tail simply rolls back to an
# earlier complete checkpoint.
_CKPT_NAME = "checkpoint"
_CKPT_VERSION = 1
_FINAL_NAME = "final"


def _crash_point(point):
    if os.environ.get(_CRASH_ENV) == point:
        os._exit(1)


class BadRecordError(ValueError):
    """Raised in strict mode for the first undecodable log line."""

    def __init__(self, lineno, raw, cause):
        self.lineno = lineno
        self.raw = raw
        self.cause = cause
        super().__init__(f"line {lineno}: {cause}")


class MigrationResult(NamedTuple):
    path: str
    records_migrated: int
    records_skipped: int
    records_salvaged: int
    replaced: bool


def _fsync_dir(dirpath):
    fd = os.open(dirpath, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _read_lines(f, quiesce, offset=0, deadline=None):
    """Yield raw lines from *f* starting at *offset*, following appends.

    Stops once the file has stayed at EOF for one quiesce window (or
    once *deadline*, a time.monotonic() instant, is exceeded).  A final
    line without a trailing newline is yielded once it is stable.
    """
    while True:
        f.seek(offset)
        line = f.readline()
        if line.endswith(b"\n"):
            offset += len(line)
            yield line
            continue
        size = os.fstat(f.fileno()).st_size
        if deadline is not None and time.monotonic() >= deadline:
            return
        if size > offset + len(line):
            continue  # grew underneath us; re-read from the same offset
        time.sleep(quiesce)
        if os.fstat(f.fileno()).st_size == offset + len(line):
            if line:
                offset += len(line)
                yield line
            return


def _convert(raw):
    """Decode one raw log line and re-encode it at the current version."""
    message = loads(raw)
    upgraded = migrate(message, CURRENT_VERSION)
    return dumps({k: v for k, v in upgraded.items() if k != "v"})


def _audit_line(lineno, raw):
    line = raw[:-1] if raw.endswith(b"\n") else raw
    return str(lineno).encode("ascii") + b":" + line[:_AUDIT_SNIPPET] + b"\n"


class _SegmentWriter:
    """Writes output records into size-bounded fsynced segment files.

    ``on_segment(name)`` is invoked after each segment is fsynced and
    closed; the migrator uses it to commit a checkpoint covering the
    segment.  ``segments`` may be pre-populated by a resumed run, in
    which case numbering continues after the existing segments.
    """

    def __init__(self, tmp_dir, max_bytes, on_segment=None):
        self._tmp_dir = tmp_dir
        self._max = max(1, max_bytes)
        self._on_segment = on_segment
        self._fh = None
        self._size = 0
        self.segments = []

    def write(self, data):
        if self._fh is not None and self._size >= self._max:
            self._close_segment()
        if self._fh is None:
            name = f"seg-{len(self.segments):06d}"
            self._fh = open(os.path.join(self._tmp_dir, name), "wb")
            self._size = 0
            self.segments.append(name)
        self._fh.write(data)
        self._size += len(data)

    def _close_segment(self):
        if self._fh is None:
            return
        name = self.segments[-1]
        self._fh.flush()
        os.fsync(self._fh.fileno())
        self._fh.close()
        self._fh = None
        if self._on_segment is not None:
            self._on_segment(name)
        _crash_point("segment")

    def close(self):
        self._close_segment()

    def abort(self):
        if self._fh is not None:
            self._fh.close()
            self._fh = None


def _assemble(tmp_dir, segments):
    """Concatenate segments into the final temp file and fsync it."""
    final = os.path.join(tmp_dir, _FINAL_NAME)
    if len(segments) == 1:
        os.rename(os.path.join(tmp_dir, segments[0]), final)
    else:
        with open(final, "wb") as out:
            for name in segments:
                with open(os.path.join(tmp_dir, name), "rb") as part:
                    shutil.copyfileobj(part, out)
            out.flush()
            os.fsync(out.fileno())
        for name in segments:
            os.remove(os.path.join(tmp_dir, name))
    # The single-segment rename inherits that segment's fsync; fsync
    # again unconditionally so "final" is durable either way.
    fd = os.open(final, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
    return final


def _emit(audit_stream, on_bad, raw, lineno):
    """Convert one line; return (output bytes or None, was_bad)."""
    try:
        out = _convert(raw)
    except ValueError as exc:
        if on_bad == "strict":
            raise BadRecordError(lineno, raw, exc) from exc
        audit_stream.write(_audit_line(lineno, raw))
        audit_stream.flush()
        return None, True
    return out, False


def _is_count(value):
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def _validate_checkpoint(line, tmp_dir, src_stat, on_bad):
    """Parse and validate one checkpoint line against the live files.

    Returns a normalized state dict, or None if the line is unusable:
    torn or corrupt JSON, wrong source identity, mismatched on_bad,
    missing/short segment files, or a missing/short assembled file.
    """
    try:
        obj = json.loads(line)
        if not isinstance(obj, dict) or obj.get("v") != _CKPT_VERSION:
            return None
        if obj["ino"] != src_stat.st_ino or obj["dev"] != src_stat.st_dev:
            return None
        if obj["on_bad"] != on_bad:
            return None
        offset = obj["offset"]
        lineno = obj["lineno"]
        migrated = obj["migrated"]
        skipped = obj["skipped"]
        dirty = obj["dirty"]
        for value in (offset, lineno, migrated, skipped):
            if not _is_count(value):
                return None
        if lineno != migrated + skipped or not isinstance(dirty, bool):
            return None
        if offset > src_stat.st_size:
            return None
        final = obj.get("final")
        segments = []
        if final is None:
            raw_segments = obj["segments"]
            if not isinstance(raw_segments, list):
                return None
            for index, entry in enumerate(raw_segments):
                name, size = entry
                if name != "seg-%06d" % index or not _is_count(size):
                    return None
                # Segments are closed before they are committed, so the
                # on-disk size must match the checkpoint exactly.
                if os.path.getsize(os.path.join(tmp_dir, name)) != size:
                    return None
                segments.append(name)
        else:
            if not _is_count(final):
                return None
            fpath = os.path.join(tmp_dir, _FINAL_NAME)
            # Larger is fine: the run was killed while draining the
            # pre-rename tail into "final"; the excess is truncated back
            # to the fsynced checkpoint size and re-derived from the
            # source.  Smaller means the file is incomplete.
            if not os.path.isfile(fpath) or os.path.getsize(fpath) < final:
                return None
        return {
            "offset": offset,
            "lineno": lineno,
            "migrated": migrated,
            "skipped": skipped,
            "dirty": dirty,
            "segments": segments,
            "final": final,
        }
    except (ValueError, KeyError, TypeError, OSError):
        return None


def _select_checkpoint(ckpt_path, tmp_dir, src_stat, on_bad):
    """Return the newest checkpoint state that still validates, or None.

    A torn final line (the process was killed mid checkpoint write) is
    dropped first; each remaining line is then validated on its own, so
    a corrupt line or a missing segment rolls back to the newest line
    whose referenced files are all intact.
    """
    try:
        with open(ckpt_path, "rb") as fh:
            data = fh.read()
    except OSError:
        return None
    if data.endswith(b"\n"):
        data = data[:-1]
    elif b"\n" in data:
        data = data[: data.rfind(b"\n")]
    else:
        return None
    chosen = None
    for line in data.split(b"\n"):
        candidate = _validate_checkpoint(line, tmp_dir, src_stat, on_bad)
        if candidate is not None:
            chosen = candidate
    return chosen


def _apply_checkpoint(tmp_dir, state):
    """Bring the tmp dir in line with the checkpoint being resumed.

    Truncates an over-long "final" back to the fsynced checkpoint size
    and removes uncommitted files (segments written after the last
    committed checkpoint, stale assembly output).
    """
    if state["final"] is not None:
        fpath = os.path.join(tmp_dir, _FINAL_NAME)
        if os.path.getsize(fpath) > state["final"]:
            os.truncate(fpath, state["final"])
    keep = set(state["segments"]) | {_CKPT_NAME}
    if state["final"] is not None:
        keep.add(_FINAL_NAME)
    for entry in os.listdir(tmp_dir):
        if entry not in keep:
            try:
                os.remove(os.path.join(tmp_dir, entry))
            except OSError:
                pass


def _converge(path, src, prog, parent, tmp_dir, quiesce, audit_stream, on_bad):
    """Converge with racing appenders after the atomic rename.

    After the rename there are two kinds of inode a writer may land on:
      * stale inodes unlinked by our renames -- writers that held an
        append descriptor keep writing there, and we keep each one open
        to drain it;
      * the current inode -- writers opening by path land here,
        possibly still in an old format.
    Each round drains every stale inode, reads the current inode's tail
    past the migrated prefix, and does one more atomic replacement with
    all tails migrated.  Iteration ends when a round finds no new
    bytes; a deadline bounds a writer that appends nonstop.  At every
    instant the path names either a fully old or fully new file, never
    a mix.  Returns the number of salvaged records.
    """
    salvaged = 0
    deadline = time.monotonic() + max(1.0, quiesce * 20)
    # Inodes unlinked by our renames that a racing writer may still
    # hold an append descriptor to: [fd, consumed offset].  src (the
    # original inode) is also closed by the caller's with-block, so it
    # is not added to extra_fds.
    stale = [[src, prog["offset"]]]
    extra_fds = []
    prefix_size = os.path.getsize(path)
    tail_tmp = os.path.join(tmp_dir, "tail-final")
    try:
        while time.monotonic() < deadline:
            tails = []
            for entry in stale:
                fd, off = entry[0], entry[1]
                pending = []
                for raw in _read_lines(fd, quiesce, offset=off, deadline=deadline):
                    prog["lineno"] += 1
                    out, bad = _emit(
                        audit_stream, on_bad, raw, prog["lineno"]
                    )
                    if bad:
                        prog["skipped"] += 1
                    else:
                        pending.append(out)
                        salvaged += 1
                    off += len(raw)
                entry[1] = off
                tails.append(b"".join(pending))

            # Records a writer appended straight onto the current inode
            # (it opened the path after our rename).  A non-canonical
            # line there makes the file mixed; repair it with another
            # atomic replacement.
            cur = open(path, "rb")
            cur_entries = []
            cur_tail_bytes = 0
            for raw in _read_lines(
                cur, quiesce, offset=prefix_size, deadline=deadline
            ):
                prog["lineno"] += 1
                out, bad = _emit(audit_stream, on_bad, raw, prog["lineno"])
                if bad:
                    prog["skipped"] += 1
                cur_entries.append((raw, out, bad))
                cur_tail_bytes += len(raw)

            if not any(tails) and not cur_entries:
                # Every inode stayed at EOF for a full quiesce window:
                # writers have converged.
                cur.close()
                break

            mixed = any(
                not bad and raw != out for raw, out, bad in cur_entries
            )
            if mixed:
                # Stable migrated prefix [0, prefix_size), then the
                # migrated current-inode tail, then migrated stale-inode
                # tails -- one atomic swap.  cur becomes a stale inode
                # next round.
                with open(tail_tmp, "wb") as out_fh:
                    with open(path, "rb") as prefix:
                        remaining = prefix_size
                        while remaining:
                            chunk = prefix.read(min(1024 * 1024, remaining))
                            out_fh.write(chunk)
                            remaining -= len(chunk)
                    for raw, out, bad in cur_entries:
                        if not bad:
                            out_fh.write(out)
                    out_fh.write(b"".join(tails))
                    out_fh.flush()
                    os.fsync(out_fh.fileno())
                consumed = prefix_size + cur_tail_bytes
                prefix_size = os.path.getsize(tail_tmp)
                os.replace(tail_tmp, path)
                _fsync_dir(parent)
                stale.append([cur, consumed])
                extra_fds.append(cur)
            else:
                # Current-inode tail is already canonical; just append
                # the migrated stale-inode tails.
                cur.close()
                prefix_size += cur_tail_bytes
                extra = b"".join(tails)
                if extra:
                    with open(path, "ab") as tail:
                        tail.write(extra)
                        tail.flush()
                        os.fsync(tail.fileno())
                    prefix_size += len(extra)
    finally:
        for fd in extra_fds:
            fd.close()
    return salvaged


def migrate_log_file(path, *, on_bad="strict", segment_size=DEFAULT_SEGMENT_SIZE,
                     quiesce=DEFAULT_QUIESCE, audit=None):
    """Migrate a JSONL log file in place to the current record version.

    Returns a MigrationResult whose counts cover only the work done by
    this call: when the call resumes a killed run, records the earlier
    run already migrated are not re-scanned, not re-written and not
    counted again.  In strict mode raises BadRecordError (a ValueError)
    on the first bad line, leaving the file untouched.
    """
    if on_bad not in ("strict", "skip"):
        raise ValueError(f"on_bad must be 'strict' or 'skip', got {on_bad!r}")
    path = os.fspath(path)
    parent = os.path.dirname(os.path.abspath(path))
    lock_path = path + ".migrate.lock"
    tmp_dir = path + ".migrate-tmp"
    ckpt_path = os.path.join(tmp_dir, _CKPT_NAME)
    audit_stream = audit if audit is not None else sys.stderr.buffer

    salvaged = 0
    replaced = False
    prog = None

    with open(lock_path, "a+b") as lock_fh:
        fcntl.flock(lock_fh, fcntl.LOCK_EX)
        try:
            _crash_point("lock")
            with open(path, "rb") as src:
                src_stat = os.fstat(src.fileno())
                # With the lock held, any leftover tmp dir belongs to a
                # killed run.  Resume from its newest valid checkpoint
                # if one survives; otherwise sweep and start fresh.
                state = None
                if os.path.isdir(tmp_dir):
                    state = _select_checkpoint(
                        ckpt_path, tmp_dir, src_stat, on_bad
                    )
                if state is None:
                    if os.path.isdir(tmp_dir):
                        shutil.rmtree(tmp_dir)
                    os.mkdir(tmp_dir)
                else:
                    _apply_checkpoint(tmp_dir, state)
                prog = {
                    "offset": state["offset"] if state else 0,
                    "lineno": state["lineno"] if state else 0,
                    "migrated": state["migrated"] if state else 0,
                    "skipped": state["skipped"] if state else 0,
                    "dirty": state["dirty"] if state else False,
                    "segments": list(state["segments"]) if state else [],
                    "final": state["final"] if state else None,
                }
                base_migrated = prog["migrated"]
                base_skipped = prog["skipped"]

                def commit():
                    record = {
                        "v": _CKPT_VERSION,
                        "ino": src_stat.st_ino,
                        "dev": src_stat.st_dev,
                        "on_bad": on_bad,
                        "offset": prog["offset"],
                        "lineno": prog["lineno"],
                        "migrated": prog["migrated"],
                        "skipped": prog["skipped"],
                        "dirty": prog["dirty"],
                        # Once assembled, the segment files are gone and
                        # "final" is the durable state.
                        "segments": [] if prog["final"] is not None else [
                            [
                                name,
                                os.path.getsize(os.path.join(tmp_dir, name)),
                            ]
                            for name in prog["segments"]
                        ],
                    }
                    if prog["final"] is not None:
                        record["final"] = prog["final"]
                    _fsync_dir(tmp_dir)
                    with open(ckpt_path, "ab") as fh:
                        fh.write(
                            json.dumps(record, separators=(",", ":"))
                            .encode("ascii")
                            + b"\n"
                        )
                        fh.flush()
                        os.fsync(fh.fileno())

                # Canonical baseline: also re-commits a rolled-back
                # state as the newest checkpoint line.
                commit()
                writer = None
                try:
                    if prog["final"] is None:
                        writer = _SegmentWriter(
                            tmp_dir, segment_size,
                            on_segment=lambda name: commit(),
                        )
                        writer.segments = prog["segments"]
                        # Main pass, then extra drain rounds: each round
                        # consumes records appended since the previous
                        # one and stops once the file holds at EOF for a
                        # quiesce window.  A round that finds nothing
                        # new means appenders are quiescent -> proceed.
                        while True:
                            extra = 0
                            for raw in _read_lines(
                                src, quiesce, offset=prog["offset"]
                            ):
                                lineno = prog["lineno"] + 1
                                out, bad = _emit(
                                    audit_stream, on_bad, raw, lineno
                                )
                                if bad:
                                    prog["lineno"] = lineno
                                    prog["offset"] += len(raw)
                                    prog["skipped"] += 1
                                    prog["dirty"] = True
                                    continue
                                # Progress advances only after the
                                # record's bytes are in a segment: a
                                # rotation commit inside write() must
                                # snapshot a state where every consumed
                                # record is already on disk.
                                writer.write(out)
                                prog["lineno"] = lineno
                                prog["offset"] += len(raw)
                                if raw != out:
                                    prog["dirty"] = True
                                prog["migrated"] += 1
                                extra += 1
                            if not extra:
                                break

                        writer.close()
                        _crash_point("segments")

                        if not prog["dirty"]:
                            # Already current-version, canonical and
                            # clean: leave every byte of the original in
                            # place.
                            return MigrationResult(
                                path=path,
                                records_migrated=(
                                    prog["migrated"] - base_migrated
                                ),
                                records_skipped=prog["skipped"] - base_skipped,
                                records_salvaged=0,
                                replaced=False,
                            )

                        final = _assemble(tmp_dir, writer.segments)
                        prog["final"] = os.path.getsize(final)
                        commit()
                        _crash_point("assemble")
                    else:
                        final = os.path.join(tmp_dir, _FINAL_NAME)

                    # Final drain immediately before the rename.  New
                    # records are appended, migrated, to "final", so
                    # old and new encodings are never interleaved.
                    tail_added = 0
                    with open(final, "ab") as out_fh:
                        for raw in _read_lines(
                            src, quiesce, offset=prog["offset"]
                        ):
                            prog["lineno"] += 1
                            prog["offset"] += len(raw)
                            out, bad = _emit(
                                audit_stream, on_bad, raw, prog["lineno"]
                            )
                            if bad:
                                prog["skipped"] += 1
                                continue
                            out_fh.write(out)
                            prog["migrated"] += 1
                            tail_added += 1
                        if tail_added:
                            out_fh.flush()
                            os.fsync(out_fh.fileno())

                    os.replace(final, path)
                    replaced = True
                    # The rename is the commit point: the migration has
                    # succeeded.  Durability/cleanup steps after it
                    # (directory fsync, appender convergence, tmp
                    # removal) must not turn that success into a
                    # reported failure.
                    try:
                        _fsync_dir(parent)
                    except OSError as exc:
                        warnings.warn(
                            f"post-commit directory fsync failed for "
                            f"{path}: {exc}"
                        )
                    _crash_point("replace")

                    try:
                        salvaged = _converge(
                            path, src, prog, parent, tmp_dir, quiesce,
                            audit_stream, on_bad,
                        )
                    except OSError as exc:
                        warnings.warn(
                            f"post-commit convergence incomplete for "
                            f"{path}: {exc}"
                        )
                except BaseException:
                    if writer is not None:
                        writer.abort()
                    raise
                finally:
                    # On success the tmp dir holds only spent
                    # checkpoints; on a strict abort it holds partial
                    # segments.  Either way the target is intact or
                    # fully replaced, so removal is safe.  A kill
                    # (SIGKILL/os._exit) skips this cleanup, which is
                    # exactly what lets the next run resume.
                    shutil.rmtree(tmp_dir, ignore_errors=True)
        finally:
            fcntl.flock(lock_fh, fcntl.LOCK_UN)

    return MigrationResult(
        path=path,
        records_migrated=prog["migrated"] - base_migrated,
        records_skipped=prog["skipped"] - base_skipped,
        records_salvaged=salvaged,
        replaced=replaced,
    )


def run_cli(argv):
    parser = argparse.ArgumentParser(
        prog="python3 -m proto_migrate migrate-log",
        description="Migrate an append-only JSONL log to the current "
        "record version, crash-safe and atomically.",
    )
    parser.add_argument("path", help="log file to migrate in place")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--strict", action="store_const", const="strict",
                      dest="on_bad", help="abort on the first bad line "
                      "(default; exit %d, file untouched)" % EXIT_BAD_RECORD)
    mode.add_argument("--skip", action="store_const", const="skip",
                      dest="on_bad", help="skip bad lines, auditing each "
                      "to stderr as '<lineno>:<first 32 bytes>'")
    parser.set_defaults(on_bad="strict")
    parser.add_argument("--segment-size", type=int,
                        default=DEFAULT_SEGMENT_SIZE, metavar="BYTES",
                        help="rotate temp segments at this size "
                        "(default: %(default)s)")
    parser.add_argument("--quiesce-ms", type=float,
                        default=DEFAULT_QUIESCE * 1000, metavar="MS",
                        help="EOF must hold this long before the input "
                        "is considered complete (default: %(default)s)")
    args = parser.parse_args(argv)

    if not os.path.isfile(args.path):
        print(f"error: not a file: {args.path}", file=sys.stderr)
        return EXIT_ERROR
    try:
        result = migrate_log_file(
            args.path,
            on_bad=args.on_bad,
            segment_size=args.segment_size,
            quiesce=args.quiesce_ms / 1000.0,
        )
    except BadRecordError as exc:
        print(f"error: bad record at {exc}", file=sys.stderr)
        return EXIT_BAD_RECORD
    except OSError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_ERROR
    print(
        "migrated=%d skipped=%d salvaged=%d replaced=%s"
        % (
            result.records_migrated,
            result.records_skipped,
            result.records_salvaged,
            "yes" if result.replaced else "no",
        )
    )
    return EXIT_OK
