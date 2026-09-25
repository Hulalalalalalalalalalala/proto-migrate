"""Online, resumable migration of append-only JSONL log files.

Reads a log of mixed-version records (one compact JSON object per line,
as produced by :func:`proto_migrate.dumps`) that another process may be
appending to *while the migration runs*, rewrites every record at the
current version, and atomically replaces the original file.  The writer
never has to stop or wait for a silent window: concurrently appended
records are migrated in their original order, with no loss and no
duplication.

Online protocol
---------------
The migrator tails the live inode.  Each pass consumes whole lines past
its current offset and only advances once EOF has held for one quiesce
window, so a partially written (torn) record is never consumed.

  SCAN      output goes to size-bounded segment files in
            ``<file>.migrate-tmp/``; every completed segment is fsynced
            and covered by a durable checkpoint recording the consumed
            input offset, the migrated record count, the skipped count,
            the cumulative "needs rewrite" flag and the segment size.
  DRAIN     the quiesced tail is appended, migrated, to the assembled
            final file.
  COMMIT    a commit marker (``<file>.committed``, a sibling of the
            target) is published and fsynced *before* ``os.replace`` so
            :func:`read_log` never opens the post-rename inode under the
            pre-commit policy; the rename plus a directory fsync is the
            single success/failure border -- every later durability or
            cleanup failure is reported as success.
  CONVERGE  appenders racing the commit are drained; old-format records
            a path-reopening writer landed on the new inode are rewritten
            through further atomic replacements.  It ends only after two
            consecutive fully empty rounds (final operations are reads,
            never a rename); a writer appending non-stop past the hard
            backstop (``max(5 s, quiesce*200)``) is finished by an
            idempotent rerun instead of waiting forever.

Checkpoints and resume
----------------------
A process killed at *any* point reruns and continues from the last
durable checkpoint: already migrated input is neither rescanned nor
rewritten, and the result is byte-for-byte identical to one uninterrupted
run.  Recovery validates the checkpoint log before trusting it:

  * the checkpoint's source-inode header must name the inode the path
    currently resolves to, and its bad-record policy must match the
    rerun's (a different inode means an earlier run already committed;
    a different policy means strict must re-examine lines a skip run
    passed -- either case restarts as a fresh, idempotent pass);
  * only newline-terminated records are committed -- a torn half line at
    the end of the checkpoint log is discarded;
  * every referenced segment must still exist with at least the recorded
    number of durable bytes;
  * offsets and counts must be strictly monotonic.

Anything past the newest valid record is rolled back: the checkpoint
log is truncated to its good prefix, retained segments are truncated to
their recorded sizes and unknown segments are deleted, then the input
range is migrated again.  A corrupt checkpoint file, a missing segment
or a torn tail therefore falls back to the previous complete checkpoint
automatically.

Bad records: a line that fails to decode or validate (bad JSON, missing
or non-integer ``v``, an unsupported version, wrong field types, or an
amount that is NaN/Infinity or a literal overflowing to Infinity, e.g.
``1e999``) is handled per ``on_bad``: ``"strict"`` raises
:class:`BadRecordError` (a ValueError) at the first bad line and leaves
the original file untouched; ``"skip"`` skips the line and emits one
audit entry ``<lineno>:<first 32 raw bytes>`` per skipped line to the
audit stream (stderr by default).  An amount of ``-0.0`` is preserved
verbatim.

Concurrent readers use :func:`read_log`, which hands back exactly one
complete view: either the whole pre-migration content or the whole
post-migration content (normalized to the current version) -- field
versions are never mixed inside one view, even while a writer reopens
the path by name during wrap-up.

Idempotency: if every record is already at the current version in
canonical encoding and nothing is skipped, the file is left byte-for-byte
untouched (no rename, no rewrite, no mtime change).
"""

from __future__ import annotations

import argparse
import fcntl
import json
import os
import shutil
import sys
import time
from typing import NamedTuple

from . import CURRENT_VERSION, VERSIONS, dumps, loads, migrate

__all__ = [
    "BadRecordError",
    "MigrationResult",
    "migrate_log_file",
    "read_log",
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
# simulate a crash mid-migration.  Checkpoints: "lock", "marker",
# "checkpoint" (alias "segment"), "segments", "assemble", "drain",
# "replace", "dirfsync", "committed", "converge", "cleanup".
_CRASH_ENV = "PROTO_MIGRATE_CRASH_AT"
# Testing hook: when this names a post-commit step ("dirfsync" or
# "cleanup"), that step raises OSError instead of running, so tests can
# prove a durability/cleanup failure after the atomic rename is reported
# as a successful migration (exit 0).
_FAULT_ENV = "PROTO_MIGRATE_FAULT_AT"

_AUDIT_SNIPPET = 32
_CP_NAME = "checkpoint"


def _commit_marker_path(path):
    """Sibling marker naming the most recent committed migration.

    Unlike the work directory this marker is kept after a successful
    run, so :func:`read_log` keeps serving normalized (uniform) views
    even when a writer reopens the path and appends old-format records
    after wrap-up; a plain rerun then migrates those bytes.
    """
    return path + ".committed"


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
    post_commit_error: str | None = None


def _fsync_dir(dirpath):
    fd = os.open(dirpath, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _read_lines(f, quiesce, offset=0, deadline=None, follow=None):
    """Yield raw lines from *f* starting at *offset*, following appends.

    Stopping rules:
      * once EOF has held for one quiesce window, the generator returns
        (a final line without a trailing newline is yielded once
        stable);
      * *deadline*, an absolute ``time.monotonic()`` instant, always
        bounds the generator;
      * *follow*, a duration in seconds, bounds only the append-following
        tail: it is armed on the *first* encounter with EOF, so an
        arbitrarily large pre-existing body is consumed in full even
        when records keep arriving, while a writer that never pauses
        still cannot stall the run forever.  Records appended after the
        window ends are handled by the drain/convergence phases (or by
        an idempotent rerun) and are never lost.
    """
    follow_deadline = None
    while True:
        f.seek(offset)
        line = f.readline()
        if line.endswith(b"\n"):
            offset += len(line)
            yield line
            now = time.monotonic()
            if deadline is not None and now >= deadline:
                return
            if follow_deadline is not None and now >= follow_deadline:
                return
            continue
        # Short read: at EOF or on a record still being appended.
        if follow_deadline is None and follow is not None:
            follow_deadline = time.monotonic() + follow
        size = os.fstat(f.fileno()).st_size
        if deadline is not None and time.monotonic() >= deadline:
            return
        if follow_deadline is not None and time.monotonic() >= follow_deadline:
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


# Line kinds recorded by group_migration._prepare_member when the caller
# (the linked-group migration) keeps a durable per-line index of every
# consumed source line, so a rerun can reclassify lines without
# rescanning the source.
LINE_GOOD = 0          # decoded and migrated normally
LINE_SKIPPED = 1       # bad record skipped under on_bad="skip"
LINE_BAD_VERSION = 2   # skipped because the version key itself is illegal


def _reject_json_constant(_value):
    raise ValueError("non-JSON constant")


def _classify_bad_line(raw):
    """Classify a skipped bad line for the linked-group line index.

    Returns LINE_BAD_VERSION when the payload is a JSON object whose
    version key is missing, non-integer or unsupported (the record's
    remaining fields may still identify it as a reference target);
    LINE_SKIPPED for every other failure.
    """
    try:
        obj = json.loads(
            raw.decode("utf-8"), parse_constant=_reject_json_constant
        )
    except (UnicodeDecodeError, ValueError):
        return LINE_SKIPPED
    if not isinstance(obj, dict):
        return LINE_SKIPPED
    version = obj.get("v")
    if not (isinstance(version, int) and not isinstance(version, bool)) \
            or version not in VERSIONS:
        return LINE_BAD_VERSION
    return LINE_SKIPPED


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


# ---------------------------------------------------------------------------
# Checkpoint log
# ---------------------------------------------------------------------------
#
# First line:  "src <inode> <mode>\n" -- the inode the scan was started
# against and the bad-record policy of the run ("strict"/"skip"); a
# rerun with a different policy starts over so strict cannot inherit a
# skip run's silently passed bad lines.
# Each following line records one durable output prefix:
#   "<input-offset> <good-count> <skipped-count> <dirty> <seg> <size>\n"
# dirty is cumulative (1 as soon as any record was re-encoded or any bad
# line skipped); <seg> is the segment holding the newest output bytes and
# <size> its durable length.  Records are trusted only when
# newline-terminated, strictly monotonic, and backed by an existing
# segment of at least <size> bytes.


def _parse_cp_record(text):
    parts = text.split(" ")
    if len(parts) != 6:
        raise ValueError("malformed checkpoint record")
    off_s, count_s, skip_s, dirty_s, name, size_s = parts
    if not (off_s.isdigit() and count_s.isdigit()
            and skip_s.isdigit() and size_s.isdigit()):
        raise ValueError("malformed checkpoint numbers")
    if dirty_s not in ("0", "1"):
        raise ValueError("malformed checkpoint dirty flag")
    if not name.startswith("seg-") or "/" in name:
        raise ValueError("malformed checkpoint segment name")
    try:
        int(name[4:])
    except ValueError:
        raise ValueError("malformed checkpoint segment name") from None
    return (int(off_s), int(count_s), int(skip_s), int(dirty_s),
            name, int(size_s))


def _read_checkpoint_log(tmp_dir):
    """Return ``(src_inode, mode, records, good_byte_prefix)``.

    Parsing stops at the first torn/garbled line or any record whose
    segment is missing or shorter than recorded; everything before it
    remains the valid prefix the caller rolls back to.
    """
    path = os.path.join(tmp_dir, _CP_NAME)
    try:
        with open(path, "rb") as f:
            data = f.read()
    except FileNotFoundError:
        return None, None, [], b""
    inode = None
    mode = None
    records = []
    good = b""
    header_seen = False
    for line in data.splitlines(keepends=True):
        if not line.endswith(b"\n"):
            break  # torn checkpoint append: roll back
        body = line[:-1].decode("ascii", errors="strict")
        if not header_seen:
            if not body.startswith("src "):
                break
            parts = body[4:].split(" ")
            if len(parts) != 2 or not parts[0].isdigit() \
                    or parts[1] not in ("strict", "skip"):
                break
            inode = int(parts[0])
            mode = parts[1]
            header_seen = True
            good += line
            continue
        try:
            rec = _parse_cp_record(body)
        except (ValueError, UnicodeDecodeError):
            break
        off, count, skipped, _dirty, name, size = rec
        if records:
            prev = records[-1]
            if off <= prev[0] or count < prev[1] or skipped < prev[2]:
                break
        seg_path = os.path.join(tmp_dir, name)
        try:
            st = os.stat(seg_path)
        except OSError:
            break  # missing segment: roll back to previous checkpoint
        if size <= 0 or st.st_size < size or not os.path.isfile(seg_path):
            break
        records.append(rec)
        good += line
    return inode, mode, records, good


def _append_checkpoint(cp_fh, tmp_dir, line):
    cp_fh.seek(0, os.SEEK_END)
    cp_fh.write(line)
    cp_fh.flush()
    os.fsync(cp_fh.fileno())
    # Make the new segment's directory entry and the checkpoint bytes
    # durable before this record may be trusted on recovery.
    _fsync_dir(tmp_dir)
    _crash_point("checkpoint")
    _crash_point("segment")  # backwards-compatible crash point name


def _publish_prefix_checkpoint(cp_fh, tmp_dir, offset, count, skipped,
                               dirty, seg_name, seg_size):
    line = (f"{offset} {count} {skipped} {1 if dirty else 0} "
            f"{seg_name} {seg_size}\n").encode("ascii")
    _append_checkpoint(cp_fh, tmp_dir, line)


class _SegmentWriter:
    """Size-bounded segment writer; a segment is created on first write."""

    def __init__(self, tmp_dir, max_bytes, start_index):
        self._tmp_dir = tmp_dir
        self._max = max(1, max_bytes)
        self._index = start_index
        self._fh = None
        self._size = 0
        self.segments = []

    @property
    def size(self):
        return self._size

    @property
    def limit(self):
        return self._max

    @property
    def current_segment(self):
        return self.segments[-1] if self.segments else None

    @property
    def has_open_segment(self):
        return self._fh is not None

    def write(self, data):
        if self._fh is None:
            name = f"seg-{self._index:06d}"
            self._fh = open(os.path.join(self._tmp_dir, name), "wb")
            self._size = 0
            self.segments.append(name)
            self._index += 1
        self._fh.write(data)
        self._size += len(data)

    def fsync_open(self):
        if self._fh is not None:
            self._fh.flush()
            os.fsync(self._fh.fileno())

    def close(self):
        """Seal the open segment (fsync); the driver checkpoints it."""
        if self._fh is not None:
            self._fh.flush()
            os.fsync(self._fh.fileno())
            self._fh.close()
            self._fh = None

    def abort(self):
        if self._fh is not None:
            self._fh.close()
            self._fh = None


def _assemble(tmp_dir, segments, into):
    """Concatenate segments into *into* (fsynced); keep the segments."""
    if os.path.exists(into):
        os.remove(into)
    with open(into, "wb") as out:
        for name in segments:
            with open(os.path.join(tmp_dir, name), "rb") as part:
                shutil.copyfileobj(part, out)
        out.flush()
        os.fsync(out.fileno())
    fd = os.open(into, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
    return into


def _segment_names(tmp_dir):
    return sorted(
        n for n in os.listdir(tmp_dir)
        if n.startswith("seg-")
        and os.path.isfile(os.path.join(tmp_dir, n))
    )


def _wipe_workdir(tmp_dir):
    for name in os.listdir(tmp_dir):
        p = os.path.join(tmp_dir, name)
        if os.path.isdir(p) and not os.path.islink(p):
            shutil.rmtree(p, ignore_errors=True)
        else:
            try:
                os.remove(p)
            except FileNotFoundError:
                pass


def _prepare_workdir(path, tmp_dir, segment_size, current_inode, on_bad):
    """Create/open the work directory and recover the last checkpoint.

    Returns a dict with the resume state.  The scan checkpoint is
    resumed exactly when it was taken against the inode the path still
    names *and* under the same bad-record policy.  A changed inode means
    an earlier run already committed; a changed policy means a strict
    run must re-examine lines the earlier skip run passed -- either way
    the run starts as a fresh pass.
    """
    os.makedirs(tmp_dir, exist_ok=True)
    cp_path = os.path.join(tmp_dir, _CP_NAME)
    inode, mode, records, good = _read_checkpoint_log(tmp_dir)

    if records and inode == current_inode and mode == on_bad:
        keep = {name: size for off, count, skipped, dirty, name, size
                in records}
        on_disk = set(_segment_names(tmp_dir))
        # Segments beyond the validated prefix belong to an interrupted
        # run: delete them so their input range is redone.
        for name in on_disk - keep.keys():
            os.remove(os.path.join(tmp_dir, name))
        # Wipe bytes a killed run flushed into a retained segment past
        # its last durable checkpoint.
        for name, size in keep.items():
            seg_path = os.path.join(tmp_dir, name)
            if os.path.getsize(seg_path) != size:
                with open(seg_path, "r+b") as f:
                    f.truncate(size)
        # Rewrite the checkpoint log to exactly its good prefix.
        with open(cp_path, "r+b") as cp_fh:
            cp_fh.seek(0)
            if cp_fh.read() != good:
                cp_fh.seek(0)
                cp_fh.truncate()
                cp_fh.write(good)
                cp_fh.flush()
                os.fsync(cp_fh.fileno())
                _fsync_dir(tmp_dir)
        offset, count, skipped, dirty, _last_seg, _size = records[-1]
        next_index = max(int(n[4:]) for n in keep) + 1
        return {
            "offset": offset,
            "count": count,
            "skipped": skipped,
            "dirty": bool(dirty),
            "writer": _SegmentWriter(tmp_dir, segment_size, next_index),
            "cp_fh": open(cp_path, "ab"),
            "resumed": True,
        }

    # Fresh start.  The target is intact or already committed; the lock
    # guarantees no live migrator's artifacts are being swept.
    _wipe_workdir(tmp_dir)
    cp_fh = open(cp_path, "ab")
    cp_fh.write(f"src {current_inode} {on_bad}\n".encode("ascii"))
    cp_fh.flush()
    os.fsync(cp_fh.fileno())
    _fsync_dir(tmp_dir)
    return {
        "offset": 0,
        "count": 0,
        "skipped": 0,
        "dirty": False,
        "segments": [],
        "writer": _SegmentWriter(tmp_dir, segment_size, 0),
        "cp_fh": cp_fh,
        "resumed": False,
    }


def _available_lines(fd, off):
    """Read every complete line available on *fd* right now, no sleep.

    Returns ``(lines, new_off)``.  A short read at EOF (a record still
    being appended, or simply EOF) ends the batch; the caller performs a
    shared quiesce wait and probes again.
    """
    lines = []
    fd.seek(off)
    while True:
        line = fd.readline()
        if not line or not line.endswith(b"\n"):
            break
        off += len(line)
        lines.append(line)
    return lines, off


def _converge(path, parent, tmp_dir, src, offset, lineno, quiesce,
              audit_stream, on_bad):
    """Drain appenders across the commit.

    Returns ``(salvaged, skipped, lineno, warning)``.  A repair I/O
    failure after the commit is reported as a warning, never a failure
    of the already-committed migration.

    Ordering model: every inode created by one of our renames is one
    *generation*.  A whole-record write lands on the inode the writer's
    ``open`` resolved to, so a record on an older-generation inode was
    opened before the rename that unlinked it and globally precedes
    every record on a younger inode -- including a record that lands on
    the older inode *late*, because the writer was preempted between
    ``open`` and its single ``write``.

    The post-commit file is split into an immutable ``base`` (the bytes
    present at convergence entry, always re-copied verbatim) plus one
    ordered migrated byte buffer per generation.  Each repair rebuilds
    ``base + gen[0] + gen[1] + ... + current tail`` in one atomic
    replacement, so a late straddle write drained into an older
    generation's buffer on the next round moves back to its true
    position instead of trailing already-promised records.

    Each round drains all stale inodes and the live current tail,
    sleeps once for the quiesce window, and probes again (that second
    probe catches a write whose open straddled a rename).  Convergence
    exits only after TWO consecutive rounds with no bytes anywhere --
    and the final operations are always reads, never a rename, so an
    inode cannot be unlinked while a straddle write is still in flight.
    A non-stop writer is bounded by a hard backstop; path-reopening
    appends after that are migrated by an idempotent rerun.
    """
    warning = None
    start = time.monotonic()
    # Convergence normally ends after two consecutive fully empty
    # rounds.  The hard backstop only bounds a writer that appends
    # non-stop for seconds; on hitting it the run stops issuing new
    # renames (never renaming after its final read, so it never unlinks
    # an inode with a straddle write in flight) and reports a warning --
    # path-reopening appends after that are migrated by a rerun.
    hard_deadline = start + max(5.0, quiesce * 200)

    base_size = os.path.getsize(path)
    tail_tmp = os.path.join(tmp_dir, "tail-final")

    # One entry per unlinked generation still tracked:
    # [fd, consumed-offset-in-that-inode, migrated-byte-buffer].
    # The source inode is generation 0; its consumed offset is the
    # drain offset handed over from the commit phase, and its buffer
    # starts empty (the records drained before the commit are already
    # in the immutable base).
    gens = [[src, offset, bytearray()]]
    extra_fds = []
    # Physical size of the rebuilt suffix at the time of the most recent
    # replacement -- exactly where live appends on the current inode
    # begin.  This must not use the (meanwhile grown) buffers, whose new
    # straddle bytes live on stale inodes, not in the current file.
    suffix_size = 0
    salvaged_records = 0
    lineno_box = [lineno]
    skipped_box = [0]

    def convert_into(buf, raw):
        nonlocal salvaged_records
        out, bad = _emit(audit_stream, on_bad, raw, lineno_box[0])
        lineno_box[0] += 1
        if bad:
            skipped_box[0] += 1
            return False
        buf.extend(out)
        salvaged_records += 1
        return True

    def do_rebuild(cur_tail):
        """Atomically publish base + all gen buffers + *cur_tail*."""
        rebuilt = base_size
        with open(tail_tmp, "wb") as out_fh:
            with open(path, "rb") as prefix:
                remaining = base_size
                while remaining:
                    chunk = prefix.read(min(1024 * 1024, remaining))
                    out_fh.write(chunk)
                    remaining -= len(chunk)
            for entry in gens:
                out_fh.write(entry[2])
                rebuilt += len(entry[2])
            out_fh.write(cur_tail)
            rebuilt += len(cur_tail)
            out_fh.flush()
            os.fsync(out_fh.fileno())
        os.replace(tail_tmp, path)
        _fsync_dir(parent)
        return rebuilt - base_size

    empty_rounds = 0
    try:
        while True:
            if time.monotonic() >= hard_deadline:
                warning = (
                    "writer still appending at convergence backstop; "
                    "committed all records drained so far, rerun to catch "
                    "up later appends"
                )
                break

            # 1) First probes of every stale inode and the live tail.
            moved = 0
            staged = []  # (gen-entry, first-lines)
            for entry in gens:
                lines, new_off = _available_lines(entry[0], entry[1])
                staged.append((entry, lines, new_off))
            cur = open(path, "rb")
            cur_first, cur_off_after = _available_lines(
                cur, base_size + suffix_size
            )

            # 2) A single shared quiesce wait for all fds, then second
            #    probes that catch an open->write straddle.
            time.sleep(quiesce)
            cur_bytes = 0
            for entry, lines, new_off in staged:
                for raw in lines:
                    if convert_into(entry[2], raw):
                        moved += 1
                more, new_off2 = _available_lines(entry[0], new_off)
                entry[1] = new_off2
                for raw in more:
                    if convert_into(entry[2], raw):
                        moved += 1
            cur_tail = bytearray()
            for raw in cur_first:
                if convert_into(cur_tail, raw):
                    cur_bytes += 1
            cur_second, cur_off_final = _available_lines(
                cur, cur_off_after
            )
            for raw in cur_second:
                if convert_into(cur_tail, raw):
                    cur_bytes += 1

            if moved == 0 and cur_bytes == 0:
                cur.close()
                empty_rounds += 1
                if empty_rounds >= 2:
                    break
                continue
            empty_rounds = 0

            # 3) Rebuild and promote; the old current inode becomes the
            #    next stale generation (its consumed offset already
            #    skips the folded tail).
            try:
                suffix_size = do_rebuild(cur_tail)
            except OSError as exc:
                cur.close()
                warning = (
                    "post-commit convergence incomplete, rerun to finish: "
                    f"{exc}"
                )
                break
            gens.append([cur, cur_off_final, bytearray(cur_tail)])
            extra_fds.append(cur)
    finally:
        for fd in extra_fds:
            try:
                fd.close()
            except OSError:
                pass

    return salvaged_records, skipped_box[0], lineno_box[0], warning


# ---------------------------------------------------------------------------
# Post-commit backup recovery
# ---------------------------------------------------------------------------
#
# The group/linked protocols rename the original inode aside as
# ``<path>...backup`` while phase 2 converges appenders and only then
# delete it.  A kill after the commit marker but before that deletion
# leaves whole appended records stranded on the backup inode.  A rerun
# must fold them into the live migrated file *before* removing the
# backup, with no loss, no duplication and in the exact order an
# uninterrupted run would have produced.
#
# ``_converge`` rebuilds the live file as
#
#     <staged base F> + G0 + G1 + ... + current tail
#
# where G0 is the cumulative buffer of records drained from the source
# (now backup) inode and every later G* comes from a younger inode
# generation.  G0 is therefore a *contiguous prefix* of the live suffix,
# grows only by appending, and migration is deterministic -- so the
# fold boundary is recoverable from file contents alone:
#
#   1. converting the backup's complete lines through the recorded
#      drain-end offset reproduces F byte for byte (the live file must
#      start with it; otherwise the backup is left untouched);
#   2. the remaining converted backup records are matched one by one
#      against the live suffix *from its first line* -- the first
#      mismatch is exactly how far the killed convergence had folded G0
#      (a younger-generation record that merely shares the same bytes
#      can only sit past that boundary and so cannot fool a strict
#      prefix match);
#   3. the rebuild writes F + matched prefix + every not-yet-folded
#      backup record + the younger-generation suffix bytes, restoring
#      the serial generation order.  A later rerun is a fixed point:
#      every backup record then matches the prefix and nothing is
#      rewritten.
#
# A backup whose drain point cannot be established, whose base does not
# match, or which still holds a record mid-append is *retained* (and a
# warning returned) rather than risking a lost or duplicated record:
# post-border faults are never failures.


def _recover_backup_appends(path, backup_path, offset, on_bad, quiesce,
                            audit_stream, base_size=None,
                            filtered=False):
    """Fold a surviving committed backup's appends into the live file.

    *offset* is the source-byte offset the killed run had drained
    through before staging (always a line boundary).  *base_size* is the
    staged-output length recorded durably right before staging: the live
    file starts with exactly those bytes.  *filtered_base* (the linked
    skip-mode case) says some good base-region records were dropped from
    the staged output, so converted base lines are aligned against
    ``live[:base_size]`` as an order-preserving subsequence; otherwise
    every converted base line must appear.

    Returns ``(appended, skipped, retain, warning)``: *appended* records
    were newly moved by this call (zero on an idempotent rerun),
    *skipped* counts bad post-drain lines newly audited, *retain* means
    the backup (and the member work directory) must be kept for another
    rerun -- it is set for a torn tail or any warning.
    """
    try:
        backup = open(backup_path, "rb")
    except OSError as exc:
        return 0, 0, True, (
            f"cannot open surviving backup {backup_path!r}: {exc}"
        )

    try:
        # Two quiet probes delimit a record whose single whole-record
        # write may still be in flight; re-read on any growth so a
        # straddle write is never classified as a complete line.
        backup.seek(0)
        blob = backup.read()
        time.sleep(quiesce)
        backup.seek(0)
        blob2 = backup.read()
        if blob2 != blob:
            time.sleep(quiesce)
            backup.seek(0)
            blob2 = backup.read()
        blob = blob2
        torn = bool(blob) and not blob.endswith(b"\n")

        base_lines = []   # converted records of the staged base, in order
        tail_records = []  # backup-origin records past the drain point
        pos = 0
        lineno0 = 0
        skipped = 0
        for raw in blob.splitlines(keepends=True):
            end = pos + len(raw)
            in_base = end <= offset
            if not raw.endswith(b"\n"):
                break
            try:
                out = _convert(raw)
            except ValueError as exc:
                if on_bad == "strict":
                    return 0, skipped, True, (
                        f"bad record on surviving backup at source line "
                        f"{lineno0 + 1}; backup retained, rerun with "
                        f"--skip to finish: {exc}"
                    )
                if not in_base:
                    # Base-region skips were audited before the border;
                    # only post-drain ones are newly audited here.
                    audit_stream.write(_audit_line(lineno0 + 1, raw))
                    audit_stream.flush()
                    skipped += 1
                pos = end
                lineno0 += 1
                continue
            if in_base:
                base_lines.append(out)
            else:
                tail_records.append(out)
            pos = end
            lineno0 += 1

        if pos < offset:
            return (0, skipped, True,
                    "surviving backup is shorter than the recorded drain "
                    "point; backup retained for manual inspection")

        with open(path, "rb") as live_fh:
            live = live_fh.read()
        if base_size is None:
            base_size = sum(len(line) for line in base_lines)
        if base_size > len(live):
            return (0, skipped, True,
                    "live file is shorter than the recorded staged base; "
                    "backup retained for manual inspection")
        staged = live[:base_size]
        suffix = live[base_size:]

        # Verify the converted base matches the staged bytes.  A
        # filtered base (linked skip mode drops bad-reference records)
        # omits base lines, so the staged lines must form an
        # order-preserving subsequence of the converted base; otherwise
        # the two must be identical line for line.
        staged_lines = staged.splitlines(keepends=True)
        if filtered:
            bi = 0
            for line in staged_lines:
                found = -1
                for j in range(bi, len(base_lines)):
                    if base_lines[j] == line:
                        found = j
                        break
                if found < 0:
                    return (0, skipped, True,
                            "staged base is not a subsequence of the "
                            "surviving backup; backup retained for manual "
                            "inspection")
                bi = found + 1
        else:
            if len(staged_lines) != len(base_lines) or any(
                    a != b for a, b in zip(staged_lines, base_lines)):
                return (0, skipped, True,
                        "live file no longer starts with the staged "
                        "base; backup retained for manual inspection")

        # Strict prefix match of the backup-origin tail against the live
        # suffix: the first line that differs is the exact fold boundary.
        suffix_lines = suffix.splitlines(keepends=True)
        matched = 0
        matched_bytes = 0
        for line in suffix_lines:
            if matched < len(tail_records) and line == tail_records[matched]:
                matched += 1
                matched_bytes += len(line)
            else:
                break
        younger = suffix[matched_bytes:]
        leftover = tail_records[matched:]

        warning = None
        if torn:
            warning = (
                "record still being appended to the surviving backup; "
                "the backup is retained, rerun to finish the last record"
            )
        if leftover:
            tail_tmp = backup_path + ".rebuild-tmp"
            with open(tail_tmp, "wb") as out_fh:
                out_fh.write(staged)
                out_fh.write(suffix[:matched_bytes])
                for out in leftover:
                    out_fh.write(out)
                out_fh.write(younger)
                out_fh.flush()
                os.fsync(out_fh.fileno())
            os.replace(tail_tmp, path)
            _fsync_dir(os.path.dirname(os.path.abspath(path)))
        return len(leftover), skipped, bool(torn), warning
    except OSError as exc:
        return 0, 0, True, (
            f"backup recovery failed, backup retained: {exc}"
        )
    finally:
        backup.close()


# Sidecar recording, right before a member is staged, everything a
# committed-run recovery needs to fold appends stranded on the backup:
# the staged output size, the source drain-end offset/line count, the
# bad-record policy and the source inode.  Published into the member
# work directory (temp file + fsync + rename + directory fsync) so it is
# either fully present or absent; consumed (deleted) only once the
# backup itself is gone.

_STAGE_SIDECAR = "stage.json"


def _write_stage_sidecar(member_dir, info):
    path = os.path.join(member_dir, _STAGE_SIDECAR)
    tmp = path + ".tmp"
    with open(tmp, "wb") as f:
        f.write(json.dumps(info).encode("ascii"))
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)
    _fsync_dir(member_dir)


def _read_stage_sidecar(member_dir):
    try:
        with open(os.path.join(member_dir, _STAGE_SIDECAR), "rb") as f:
            data = json.loads(f.read())
        info = {
            "staged_size": int(data["staged_size"]),
            "offset": int(data["offset"]),
            "lineno": int(data["lineno"]),
            "mode": str(data["mode"]),
            "inode": int(data["inode"]),
        }
        if "filtered" in data:
            info["filtered"] = bool(data["filtered"])
        return info
    except (OSError, ValueError, KeyError, TypeError):
        return None


def migrate_log_file(path, *, on_bad="strict", segment_size=DEFAULT_SEGMENT_SIZE,
                     quiesce=DEFAULT_QUIESCE, audit=None):
    """Migrate a JSONL log file in place to the current record version.

    The file may be appended to while this runs; appended records are
    migrated in order, without loss or duplication.  A killed run
    resumes from its last durable checkpoint and finishes byte-identical
    to an uninterrupted run.

    Returns a :class:`MigrationResult`.  In strict mode raises
    :class:`BadRecordError` on the first bad line and leaves the target
    file (and its source bytes) untouched.
    """
    if on_bad not in ("strict", "skip"):
        raise ValueError(f"on_bad must be 'strict' or 'skip', got {on_bad!r}")
    if segment_size <= 0:
        raise ValueError("segment_size must be positive")
    path = os.fspath(path)
    parent = os.path.dirname(os.path.abspath(path))
    lock_path = path + ".migrate.lock"
    tmp_dir = path + ".migrate-tmp"
    audit_stream = audit if audit is not None else sys.stderr.buffer

    migrated = skipped = salvaged = 0
    replaced = False
    post_commit_error = None

    with open(lock_path, "a+b") as lock_fh:
        fcntl.flock(lock_fh, fcntl.LOCK_EX)
        try:
            _crash_point("lock")
            # A marker.tmp left by a crash between its creation and the
            # atomic rename is stale and would otherwise linger.
            try:
                os.unlink(_commit_marker_path(path) + ".tmp")
            except FileNotFoundError:
                pass
            state = _prepare_workdir(
                path, tmp_dir, segment_size, os.stat(path).st_ino, on_bad
            )
            writer: _SegmentWriter = state["writer"]
            cp_fh = state["cp_fh"]
            offset = state["offset"]
            migrated = state["count"]
            skipped = state["skipped"]
            dirty = state["dirty"]
            lineno = state["count"] + state["skipped"]
            try:
                with open(path, "rb") as src:
                    # --- SCAN: tail the live inode.  The pre-existing
                    # body is always consumed in full; the follow
                    # window (armed at the first EOF) bounds how long
                    # the scan keeps tailing a still-active appender so
                    # the run never waits for a silent window, and a
                    # writer that appends non-stop cannot stall it.
                    follow_window = max(1.0, quiesce * 20)
                    for raw in _read_lines(src, quiesce, offset=offset,
                                           follow=follow_window):
                        lineno += 1
                        offset += len(raw)
                        out, bad = _emit(audit_stream, on_bad, raw, lineno)
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
                            _publish_prefix_checkpoint(
                                cp_fh, tmp_dir, offset, migrated,
                                skipped, dirty, name,
                                os.path.getsize(
                                    os.path.join(tmp_dir, name)),
                            )

                    if writer.has_open_segment:
                        # The last segment never hit the rotation limit;
                        # cover it once so a crash during assemble/drain
                        # rescans nothing.  A segment sealed at rotation
                        # already has its checkpoint and must not get a
                        # duplicate (same-offset) record.
                        name = writer.current_segment
                        writer.close()
                        _publish_prefix_checkpoint(
                            cp_fh, tmp_dir, offset, migrated, skipped,
                            dirty, name,
                            os.path.getsize(os.path.join(tmp_dir, name)),
                        )
                    _crash_point("segments")

                    if not dirty:
                        # Canonical current-version file, nothing
                        # skipped: leave every byte untouched.
                        return MigrationResult(
                            path=path,
                            records_migrated=migrated,
                            records_skipped=skipped,
                            records_salvaged=0,
                            replaced=False,
                        )

                    final = _assemble(
                        tmp_dir, _segment_names(tmp_dir),
                        os.path.join(tmp_dir, "final"),
                    )
                    _crash_point("assemble")

                    # --- DRAIN: migrate everything appended since the
                    # scan ended straight onto "final", bounded by the
                    # same follow window so a non-stop appender defers
                    # its remainder to convergence / a rerun instead of
                    # blocking the commit.
                    with open(final, "ab") as out_fh:
                        tail_added = 0
                        for raw in _read_lines(src, quiesce, offset=offset,
                                               follow=follow_window):
                            lineno += 1
                            offset += len(raw)
                            out, bad = _emit(audit_stream, on_bad, raw, lineno)
                            if bad:
                                skipped += 1
                                continue
                            out_fh.write(out)
                            migrated += 1
                            tail_added += 1
                        if tail_added:
                            out_fh.flush()
                            os.fsync(out_fh.fileno())
                    _crash_point("drain")

                    # --- COMMIT: the single success/failure border -----
                    # Publish the commit marker *before* the rename and
                    # fsync it durably: once it exists read_log serves
                    # only normalized post-commit views, so it can never
                    # open the post-rename inode under the pre-commit
                    # policy.  A crash here leaves marker + original
                    # file behind; the rerun resumes the checkpoint
                    # (same source inode) and finishes.
                    marker = _commit_marker_path(path)
                    marker_tmp = marker + ".tmp"
                    with open(marker_tmp, "wb") as mf:
                        mf.write(b"1\n")
                        mf.flush()
                        os.fsync(mf.fileno())
                    os.replace(marker_tmp, marker)
                    _fsync_dir(parent)
                    _crash_point("marker")

                    os.replace(final, path)
                    _crash_point("replace")
                    try:
                        if os.environ.get(_FAULT_ENV) == "dirfsync":
                            raise OSError("injected directory fsync fault")
                        _fsync_dir(parent)
                    except OSError as exc:
                        post_commit_error = f"directory fsync failed: {exc}"
                    _crash_point("dirfsync")
                    replaced = True
                    _crash_point("committed")

                    # --- CONVERGE with racing appenders ----------------
                    salvaged, conv_skipped, lineno, warning = _converge(
                        path, parent, tmp_dir, src, offset, lineno,
                        quiesce, audit_stream, on_bad,
                    )
                    skipped += conv_skipped
                    if warning and post_commit_error is None:
                        post_commit_error = warning
                    _crash_point("converge")
            except BaseException:
                writer.abort()
                raise
            finally:
                writer.abort()
                try:
                    cp_fh.close()
                finally:
                    if replaced:
                        # Commit landed: cleanup faults are warnings.
                        try:
                            _crash_point("cleanup")
                            if os.environ.get(_FAULT_ENV) == "cleanup":
                                raise OSError("injected cleanup fault")
                            shutil.rmtree(tmp_dir, ignore_errors=True)
                        except OSError as exc:
                            if post_commit_error is None:
                                post_commit_error = f"cleanup failed: {exc}"
                    else:
                        # Target untouched (strict abort or clean file):
                        # sweep partial work, as in the batch design.
                        shutil.rmtree(tmp_dir, ignore_errors=True)
        finally:
            fcntl.flock(lock_fh, fcntl.LOCK_UN)

    return MigrationResult(
        path=path,
        records_migrated=migrated,
        records_skipped=skipped,
        records_salvaged=salvaged,
        replaced=replaced,
        post_commit_error=post_commit_error,
    )


# ---------------------------------------------------------------------------
# Public consistent read entry
# ---------------------------------------------------------------------------


def _complete_lines(blob):
    """Decode every newline-terminated line; a torn tail is excluded."""
    parts = blob.split(b"\n")
    # The final element follows the last newline: it is empty when the
    # blob ends with "\n", otherwise a record still being appended --
    # either way not part of the view.  Every other element is a
    # complete stored line and must decode like a normal record.
    return [loads(line + b"\n") for line in parts[:-1]]


def read_log(path, *, quiesce=DEFAULT_QUIESCE):
    """Return one complete, version-consistent view of a migrating log.

    A list of decoded record dicts is returned.  While a migration runs
    the caller receives either the full pre-migration content (records
    decoded exactly as stored) or the full post-migration content
    (every record normalized to the current version); the two field
    shapes never mix inside one view -- including while a writer
    reopens the path by name and appends during the migrator's wrap-up.

    The view is an instant, coherent snapshot: the open descriptor pins
    a single inode for the whole read, so an atomic replacement mid-read
    cannot straddle two files, and only newline-terminated lines are
    included, so a record caught mid-append never appears.  A complete
    but undecodable line raises ValueError, like :func:`loads`.

    The *quiesce* argument is accepted for API symmetry with
    :func:`migrate_log_file`; an instant snapshot never blocks on an
    active appender.
    """
    path = os.fspath(path)
    marker = _commit_marker_path(path)

    # Phase A: no commit marker yet.  Marker publication strictly
    # precedes the rename, so a missing marker means the path still
    # names the pre-migration inode; a descriptor opened here pins it
    # for the whole read even if the rename happens concurrently.
    if not os.path.exists(marker):
        with open(path, "rb") as fh:
            blob = fh.read()
        if not os.path.exists(marker):
            return _complete_lines(blob)
        # The commit landed while we read: serve the post view instead.

    # Phase B: post-commit.  The descriptor pins one inode for the whole
    # snapshot, and normalizing every record makes the view uniform even
    # if that inode transiently holds a migrated prefix plus an
    # old-format tail (convergence rewrites that tail atomically; a
    # path-reopening appender after wrap-up is cleaned by the next
    # idempotent run).
    with open(path, "rb") as fh:
        blob = fh.read()
    return [migrate(rec, CURRENT_VERSION) for rec in _complete_lines(blob)]


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def run_cli(argv):
    parser = argparse.ArgumentParser(
        prog="python3 -m proto_migrate migrate-log",
        description="Migrate an append-only JSONL log to the current "
        "record version: online (writers keep appending), resumable "
        "from durable checkpoints, and atomic.",
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
    if result.post_commit_error:
        # The atomic commit already succeeded: later faults show up as
        # a warning, never as a non-zero exit.
        print(f"warning: {result.post_commit_error}", file=sys.stderr)
    return EXIT_OK
