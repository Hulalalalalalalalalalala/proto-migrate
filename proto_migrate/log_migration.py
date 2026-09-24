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

Group migration
---------------
:func:`migrate_log_group` migrates an explicitly given set of logs as
one all-or-nothing unit: either every member ends at the current
version or no member's original file is modified, and the persistent
state never shows a partially migrated group.  Every member is first
migrated into its own segment files and local checkpoints, ending with
a durable per-member ``prepared`` record; only when ALL members are
prepared does a durable group commit record
(``<first member>.migrate-group/commit``) authorize the unified rename
sequence -- the atomic decision of a cross-file two-phase protocol.
Each finished rename is recorded as a durable ``done`` line.

  * killed before the commit record: no original file has changed; the
    rerun resumes every member from its checkpoints (fully prepared
    members are neither rescanned nor rewritten) and finishes
    byte-identical to one uninterrupted run, and a pre-commit failure
    rolls the whole group back without a trace;
  * killed after the commit record: the rerun completes the remaining
    renames from the record -- it never rolls renames back.

:func:`read_log_group` returns one consistent snapshot of the whole
group: while no member's commit marker exists every member is decoded
as stored; as soon as any marker exists every member is normalized to
the current version, so old and new field shapes never mix inside the
view -- within or between members, even while a writer reopens a path
after wrap-up.  The group summary counts only records newly migrated
during the run itself.
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

from . import CURRENT_VERSION, dumps, loads, migrate

__all__ = [
    "BadRecordError",
    "MigrationResult",
    "GroupMigrationResult",
    "migrate_log_file",
    "migrate_log_group",
    "read_log",
    "read_log_group",
    "run_cli",
    "run_group_cli",
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
# "replace", "dirfsync", "committed", "converge", "cleanup"; group
# migration adds "group-lock", "group-prepare", "group-segments",
# "group-commit", "group-rename", "group-converge" and "group-cleanup".
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


def _audit_line(lineno, raw, *, label=None):
    line = raw[:-1] if raw.endswith(b"\n") else raw
    prefix = b"" if label is None else os.fsencode(label) + b":"
    return (prefix + str(lineno).encode("ascii") + b":"
            + line[:_AUDIT_SNIPPET] + b"\n")


def _emit(audit_stream, on_bad, raw, lineno, *, label=None):
    """Convert one line; return (output bytes or None, was_bad).

    *label* prefixes the skip-audit entry with a member name; the group
    migration passes the member path so one shared stderr stream stays
    attributable.  Single-file callers omit it and get the historical
    ``<lineno>:<bytes>`` format.
    """
    try:
        out = _convert(raw)
    except ValueError as exc:
        if on_bad == "strict":
            raise BadRecordError(lineno, raw, exc) from exc
        audit_stream.write(_audit_line(lineno, raw, label=label))
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
              audit_stream, on_bad, *, label=None):
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
        out, bad = _emit(audit_stream, on_bad, raw, lineno_box[0],
                         label=label)
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
# Group migration: one all-or-nothing pass over an explicit set of logs
# ---------------------------------------------------------------------------
#
# Every member is first migrated into its own segment files and local
# checkpoints (the single-file machinery), finishing with a durable
# per-member ``prepared`` record.  Only when ALL members are prepared
# does the run publish a group commit record in
# ``<first member>.migrate-group/commit`` -- the atomic decision of the
# two-phase protocol.  Phase two then renames each member's assembled
# output over its original, recording every finished rename as a
# durable ``done`` line in the commit record.
#
# A process killed before the commit record is durable has changed no
# original file: the rerun resumes each member from its checkpoints
# (fully prepared members are neither rescanned nor rewritten) or, on a
# pre-commit failure, the whole group rolls back without a trace.  A
# kill after the commit record is finished by the rerun from the record
# itself: remaining renames are completed, never rolled back.


class GroupMigrationResult(NamedTuple):
    results: tuple  # one MigrationResult per member, in manifest order
    records_migrated: int
    records_skipped: int
    records_salvaged: int
    replaced: int  # members whose original was atomically replaced
    post_commit_error: str | None = None


_GROUP_DIR_SUFFIX = ".migrate-group"
_GROUP_COMMIT_NAME = "commit"
_PREPARED_NAME = "prepared"


def _validate_manifest(paths):
    """Check the explicit member manifest shared by both group entries."""
    if isinstance(paths, (str, bytes)) or not isinstance(paths, Sequence):
        raise TypeError("paths must be a sequence of filesystem paths")
    paths = [os.fspath(p) for p in paths]
    if not paths:
        raise ValueError("paths must name at least one log file")
    seen = set()
    for p in paths:
        ap = os.path.abspath(p)
        if ap in seen:
            raise ValueError(f"duplicate path in manifest: {p!r}")
        seen.add(ap)
    return paths


def _require_readable(paths):
    """Raise FileNotFoundError for any missing or unreadable member."""
    for p in paths:
        try:
            with open(p, "rb"):
                pass
        except FileNotFoundError:
            raise
        except OSError as exc:
            raise FileNotFoundError(
                2, f"log file is missing or unreadable: "
                f"{exc.strerror or exc}", p) from exc


def _write_prepared(tmp_dir, inode, mode, offset, migrated, skipped,
                    lineno, final_size):
    """Durably record that a member's assembled output is commit-ready."""
    line = (f"prepared {inode} {mode} {offset} {migrated} {skipped} "
            f"{lineno} {final_size}\n").encode("ascii")
    with open(os.path.join(tmp_dir, _PREPARED_NAME), "wb") as f:
        f.write(line)
        f.flush()
        os.fsync(f.fileno())
    _fsync_dir(tmp_dir)


def _read_prepared(tmp_dir):
    """Return the prepared record as a dict, or None if absent/torn."""
    try:
        with open(os.path.join(tmp_dir, _PREPARED_NAME), "rb") as f:
            data = f.read()
    except (FileNotFoundError, NotADirectoryError):
        return None
    if not data.endswith(b"\n"):
        return None
    try:
        parts = data[:-1].decode("ascii").split(" ")
    except UnicodeDecodeError:
        return None
    if (len(parts) != 8 or parts[0] != "prepared"
            or parts[2] not in ("strict", "skip")):
        return None
    numbers = (parts[1],) + tuple(parts[3:])
    if not all(x.isdigit() for x in numbers):
        return None
    return {
        "inode": int(parts[1]),
        "mode": parts[2],
        "offset": int(parts[3]),
        "migrated": int(parts[4]),
        "skipped": int(parts[5]),
        "lineno": int(parts[6]),
        "final_size": int(parts[7]),
    }


def _read_group_commit(group_dir):
    """Return ``{"members": [...], "done": set()}`` or None.

    The header fixes the member count; a commit record that does not
    parse up to exactly that many member lines was torn before the
    commit decision became durable, so no rename can have started and
    the caller treats it as absent.  A torn ``done`` tail only replays
    renames, which are idempotent.
    """
    try:
        with open(os.path.join(group_dir, _GROUP_COMMIT_NAME), "rb") as f:
            data = f.read()
    except (FileNotFoundError, NotADirectoryError):
        return None
    expected = None
    members = []
    done = set()
    for line in data.splitlines():
        try:
            rec = json.loads(line.decode("utf-8"))
        except (ValueError, UnicodeDecodeError):
            break  # torn tail: ignore everything from here on
        if not isinstance(rec, dict):
            break
        if expected is None:
            if (not isinstance(rec.get("group"), int)
                    or rec["group"] <= 0
                    or rec.get("mode") not in ("strict", "skip")):
                break
            expected = rec["group"]
            continue
        if "member" in rec:
            try:
                idx = rec["member"]
                path = rec["path"]
                src = rec["src"]
                fin = rec["final"]
                if not (isinstance(idx, int) and isinstance(path, str)
                        and isinstance(src, int) and isinstance(fin, int)):
                    raise ValueError
            except (KeyError, ValueError):
                break
            if idx != len(members):
                break
            members.append({"path": path, "src": src, "final": fin})
            continue
        if "done" in rec:
            idx = rec["done"]
            if not isinstance(idx, int) or not 0 <= idx < len(members):
                break
            done.add(idx)
            continue
        break
    if expected is None or len(members) != expected:
        return None
    return {"members": members, "done": done}


def _publish_commit_marker(path, parent):
    """Publish and fsync the commit marker gating consistent readers."""
    marker = _commit_marker_path(path)
    marker_tmp = marker + ".tmp"
    with open(marker_tmp, "wb") as mf:
        mf.write(b"1\n")
        mf.flush()
        os.fsync(mf.fileno())
    os.replace(marker_tmp, marker)
    _fsync_dir(parent)


def _new_group_member(path, abspath):
    return {
        "path": path,
        "abspath": abspath,
        "parent": os.path.dirname(abspath),
        "tmp_dir": path + ".migrate-tmp",
        "src": None,
        "writer": None,
        "cp_fh": None,
        "offset": 0,
        "lineno": 0,
        "src_inode": None,
        "run_migrated": 0,
        "run_skipped": 0,
        "salvaged": 0,
        "dirty": False,
        "replaced": False,
        "final": None,
        "final_inode": None,
        "commit_index": None,
        "warning": None,
    }


def _close_group_member(m):
    writer = m["writer"]
    if writer is not None:
        writer.abort()
        m["writer"] = None
    for key in ("src", "cp_fh"):
        fh = m[key]
        if fh is not None:
            try:
                fh.close()
            except OSError:
                pass
            m[key] = None


def _prepare_group_member(m, on_bad, segment_size, quiesce, audit_stream,
                          follow_window):
    """Scan and drain one member up to (not including) the group commit.

    Fills *m* in place.  Afterwards the member is either clean (no
    rewrite needed, nothing to commit) or commit-ready: its assembled
    output and a durable prepared record wait in its work directory and
    its source descriptor stays open for the post-commit convergence.
    """
    path = m["path"]
    tmp_dir = m["tmp_dir"]
    # A marker.tmp left by a crash between its creation and the atomic
    # rename is stale and would otherwise linger.
    try:
        os.unlink(_commit_marker_path(path) + ".tmp")
    except FileNotFoundError:
        pass
    st = os.stat(path)
    m["src_inode"] = st.st_ino

    prepared = _read_prepared(tmp_dir)
    if prepared is not None:
        final = os.path.join(tmp_dir, "final")
        if (prepared["inode"] == st.st_ino and prepared["mode"] == on_bad
                and os.path.isfile(final)
                and os.path.getsize(final) == prepared["final_size"]):
            # Fully scanned and drained by a previous run: neither
            # rescan nor rewrite, the assembled output is reused as is.
            m["offset"] = prepared["offset"]
            m["lineno"] = prepared["lineno"]
            m["dirty"] = True
            m["final"] = final
            m["src"] = open(path, "rb")
            return m

    state = _prepare_workdir(path, tmp_dir, segment_size, st.st_ino, on_bad)
    writer = m["writer"] = state["writer"]
    cp_fh = m["cp_fh"] = state["cp_fh"]
    offset = state["offset"]
    migrated = state["count"]
    skipped = state["skipped"]
    dirty = state["dirty"]
    m["lineno"] = lineno = migrated + skipped
    src = m["src"] = open(path, "rb")

    # --- SCAN: identical protocol to the single-file migration, but
    # only records converted during this run count towards the summary.
    for raw in _read_lines(src, quiesce, offset=offset,
                           follow=follow_window):
        lineno += 1
        offset += len(raw)
        out, bad = _emit(audit_stream, on_bad, raw, lineno, label=path)
        if bad:
            skipped += 1
            m["run_skipped"] += 1
            dirty = True
            continue
        if raw != out:
            dirty = True
        writer.write(out)
        migrated += 1
        m["run_migrated"] += 1
        if writer.size >= writer.limit:
            name = writer.current_segment
            writer.close()
            _publish_prefix_checkpoint(
                cp_fh, tmp_dir, offset, migrated, skipped, dirty, name,
                os.path.getsize(os.path.join(tmp_dir, name)))

    if writer.has_open_segment:
        name = writer.current_segment
        writer.close()
        _publish_prefix_checkpoint(
            cp_fh, tmp_dir, offset, migrated, skipped, dirty, name,
            os.path.getsize(os.path.join(tmp_dir, name)))
    _crash_point("group-segments")

    m["offset"] = offset
    m["lineno"] = lineno
    m["dirty"] = dirty
    cp_fh.close()
    m["cp_fh"] = None

    if not dirty:
        # Canonical current-version file, nothing skipped: leave every
        # byte untouched; this member needs no commit.
        _close_group_member(m)
        shutil.rmtree(tmp_dir, ignore_errors=True)
        return m

    final = _assemble(tmp_dir, _segment_names(tmp_dir),
                      os.path.join(tmp_dir, "final"))

    # --- DRAIN: migrate everything appended since the scan ended,
    # bounded by the same follow window as the single-file migration.
    with open(final, "ab") as out_fh:
        tail_added = 0
        for raw in _read_lines(src, quiesce, offset=offset,
                               follow=follow_window):
            lineno += 1
            offset += len(raw)
            out, bad = _emit(audit_stream, on_bad, raw, lineno, label=path)
            if bad:
                skipped += 1
                m["run_skipped"] += 1
                continue
            out_fh.write(out)
            migrated += 1
            m["run_migrated"] += 1
            tail_added += 1
        if tail_added:
            out_fh.flush()
            os.fsync(out_fh.fileno())
    m["offset"] = offset
    m["lineno"] = lineno

    _write_prepared(tmp_dir, m["src_inode"], on_bad, offset, migrated,
                    skipped, lineno, os.path.getsize(final))
    m["final"] = final
    return m


def _converge_group_member(m, quiesce, audit_stream, on_bad, warnings):
    """Drain appenders racing a member's commit; faults are warnings."""
    src = m["src"]
    if src is None:
        return
    try:
        salvaged, conv_skipped, lineno, warning = _converge(
            m["path"], m["parent"], m["tmp_dir"], src, m["offset"],
            m["lineno"], quiesce, audit_stream, on_bad, label=m["path"],
        )
    finally:
        try:
            src.close()
        except OSError:
            pass
        m["src"] = None
    m["salvaged"] += salvaged
    m["run_skipped"] += conv_skipped
    m["lineno"] = lineno
    if warning:
        m["warning"] = m["warning"] or warning
        warnings.append(f"{m['path']}: {warning}")


def _recover_group_commit(commit_state, group_dir, on_bad, warnings):
    """Finish a durable group commit interrupted mid-rename.

    Every recorded member whose rename has not landed (its path still
    resolves to the recorded source inode) is renamed now; a member
    whose path already resolves to the recorded output inode only gets
    its ``done`` line rewritten.  Returns the member dicts renamed
    during this run so the caller can converge their racing appenders.
    """
    renamed = []
    commit_fh = open(os.path.join(group_dir, _GROUP_COMMIT_NAME), "ab")
    dirfsync_fault_used = False
    try:
        for idx, rec in enumerate(commit_state["members"]):
            if idx in commit_state["done"]:
                continue
            path = rec["path"]
            tmp_dir = path + ".migrate-tmp"
            prepared = _read_prepared(tmp_dir)
            if prepared is None:
                raise OSError(
                    "group commit recovery: missing prepared state for "
                    f"{path}")
            cur_ino = os.stat(path).st_ino
            if cur_ino == rec["final"]:
                # The rename landed before the interruption; only the
                # done record was lost.
                pass
            elif cur_ino == rec["src"]:
                final = os.path.join(tmp_dir, "final")
                if (not os.path.isfile(final)
                        or os.path.getsize(final) != prepared["final_size"]):
                    raise OSError(
                        "group commit recovery: missing assembled output "
                        f"for {path}")
                parent = os.path.dirname(path)
                src = open(path, "rb")
                _publish_commit_marker(path, parent)
                os.replace(final, path)
                _crash_point("group-rename")
                try:
                    if (not dirfsync_fault_used
                            and os.environ.get(_FAULT_ENV) == "dirfsync"):
                        dirfsync_fault_used = True
                        raise OSError("injected directory fsync fault")
                    _fsync_dir(parent)
                except OSError as exc:
                    warnings.append(
                        f"{path}: directory fsync failed: {exc}")
                renamed.append({
                    "path": path,
                    "parent": parent,
                    "tmp_dir": tmp_dir,
                    "src": src,
                    "offset": prepared["offset"],
                    "lineno": prepared["lineno"],
                    "salvaged": 0,
                    "run_skipped": 0,
                    "warning": None,
                })
            else:
                raise OSError(
                    "group commit recovery: member changed externally "
                    f"while the group commit was pending: {path}")
            commit_fh.seek(0, os.SEEK_END)
            commit_fh.write(json.dumps({"done": idx}).encode("ascii")
                            + b"\n")
            commit_fh.flush()
            os.fsync(commit_fh.fileno())
    finally:
        commit_fh.close()
    return renamed


def _sweep_group_artifacts(member_paths, group_dir, warnings):
    """Remove work directories after a committed group; never fatal."""
    try:
        _crash_point("group-cleanup")
        if os.environ.get(_FAULT_ENV) == "cleanup":
            raise OSError("injected cleanup fault")
        for ap in member_paths:
            shutil.rmtree(ap + ".migrate-tmp", ignore_errors=True)
        shutil.rmtree(group_dir, ignore_errors=True)
    except OSError as exc:
        warnings.append(f"cleanup failed: {exc}")


def _group_result(results, warnings):
    return GroupMigrationResult(
        results=tuple(results),
        records_migrated=sum(r.records_migrated for r in results),
        records_skipped=sum(r.records_skipped for r in results),
        records_salvaged=sum(r.records_salvaged for r in results),
        replaced=sum(1 for r in results if r.replaced),
        post_commit_error="; ".join(warnings) if warnings else None,
    )


def migrate_log_group(paths, *, on_bad="strict",
                      segment_size=DEFAULT_SEGMENT_SIZE,
                      quiesce=DEFAULT_QUIESCE, audit=None):
    """Migrate a whole set of JSONL logs to the current record version.

    The group is all-or-nothing: either every member ends at the
    current version or no member's original file is modified, and no
    partially migrated state is ever left in the persistent files.
    Every member may be appended to by other processes while this runs.

    Each member is first migrated into its own segment files and local
    checkpoints; a failure of any member before the commit phase rolls
    the whole group back to its pre-migration state.  The unified
    rename sequence is a recoverable two-phase protocol: a durable group
    commit record (``<first member>.migrate-group/commit``) is the
    atomic decision, after which a killed run is finished by a rerun
    completing the remaining renames -- never by rolling renames back.
    A rerun continues from the per-member checkpoints: prepared members
    are neither rescanned nor rewritten and the result is byte-for-byte
    identical to one uninterrupted run.

    The summary counts only records newly migrated during this run.
    Returns a :class:`GroupMigrationResult`.  In strict mode raises
    :class:`BadRecordError` at the group's first bad line and leaves
    every member untouched.
    """
    if on_bad not in ("strict", "skip"):
        raise ValueError(f"on_bad must be 'strict' or 'skip', got {on_bad!r}")
    if segment_size <= 0:
        raise ValueError("segment_size must be positive")
    paths = _validate_manifest(paths)
    _require_readable(paths)
    audit_stream = audit if audit is not None else sys.stderr.buffer
    follow_window = max(1.0, quiesce * 20)
    abs_paths = [os.path.abspath(p) for p in paths]
    group_dir = abs_paths[0] + _GROUP_DIR_SUFFIX

    # Probe the commit record only to size the lock set; the
    # authoritative read happens under the locks.
    lock_set = set(abs_paths)
    probe = _read_group_commit(group_dir)
    if probe is not None:
        lock_set.update(m["path"] for m in probe["members"])

    locks = []
    members = []
    warnings = []
    recovery_counts = {}
    committed = False
    try:
        for ap in sorted(lock_set):
            fh = open(ap + ".migrate.lock", "a+b")
            fcntl.flock(fh, fcntl.LOCK_EX)
            locks.append(fh)
        _crash_point("group-lock")

        commit_state = _read_group_commit(group_dir)
        if commit_state is not None:
            # A durable commit decision exists: complete the remaining
            # renames, never roll them back.
            committed = True
            renamed = _recover_group_commit(
                commit_state, group_dir, on_bad, warnings)
            for m in renamed:
                _converge_group_member(m, quiesce, audit_stream, on_bad,
                                       warnings)
                recovery_counts[m["path"]] = (
                    m["salvaged"], m["run_skipped"], m["warning"])
            _sweep_group_artifacts(
                [m["path"] for m in commit_state["members"]], group_dir,
                warnings)
            committed = False  # commit record consumed; artifacts gone
            if set(m["path"] for m in commit_state["members"]) \
                    <= set(abs_paths):
                # The recorded group covers this manifest: members not
                # in the record needed no rename and are not rescanned.
                recorded = {m["path"] for m in commit_state["members"]}
                results = []
                for p, ap in zip(paths, abs_paths):
                    salvaged, skipped, warning = recovery_counts.get(
                        ap, (0, 0, None))
                    results.append(MigrationResult(
                        path=p,
                        records_migrated=0,
                        records_skipped=skipped,
                        records_salvaged=salvaged,
                        replaced=ap in recorded,
                        post_commit_error=warning,
                    ))
                return _group_result(results, warnings)
            # A different overlapping group left the record: its
            # renames are done, so fall through and migrate this
            # manifest as a fresh pass.

        # --- PREPARE: scan and drain every member (resumable) --------
        for p, ap in zip(paths, abs_paths):
            m = _new_group_member(p, ap)
            members.append(m)
            _prepare_group_member(m, on_bad, segment_size, quiesce,
                                  audit_stream, follow_window)
            _crash_point("group-prepare")
        dirty = [m for m in members if m["dirty"]]

        if dirty:
            # --- COMMIT phase 1: the durable group decision ----------
            shutil.rmtree(group_dir, ignore_errors=True)
            os.makedirs(group_dir)
            lines = [json.dumps({"group": len(dirty), "mode": on_bad},
                                separators=(",", ":"))]
            for idx, m in enumerate(dirty):
                m["commit_index"] = idx
                m["final_inode"] = os.stat(m["final"]).st_ino
                lines.append(json.dumps(
                    {"member": idx, "path": m["abspath"],
                     "src": m["src_inode"], "final": m["final_inode"]},
                    separators=(",", ":")))
            commit_fh = open(
                os.path.join(group_dir, _GROUP_COMMIT_NAME), "w+b")
            try:
                commit_fh.write(("\n".join(lines) + "\n").encode("utf-8"))
                commit_fh.flush()
                os.fsync(commit_fh.fileno())
                _fsync_dir(group_dir)
                _fsync_dir(os.path.dirname(group_dir))
                committed = True
                _crash_point("group-commit")

                # --- COMMIT phase 2: the unified rename sequence -----
                dirfsync_fault_used = False
                for m in dirty:
                    _publish_commit_marker(m["path"], m["parent"])
                    os.replace(m["final"], m["path"])
                    _crash_point("group-rename")
                    m["replaced"] = True
                    try:
                        if (not dirfsync_fault_used
                                and os.environ.get(_FAULT_ENV)
                                == "dirfsync"):
                            dirfsync_fault_used = True
                            raise OSError(
                                "injected directory fsync fault")
                        _fsync_dir(m["parent"])
                    except OSError as exc:
                        note = f"directory fsync failed: {exc}"
                        m["warning"] = m["warning"] or note
                        warnings.append(f"{m['path']}: {note}")
                    commit_fh.seek(0, os.SEEK_END)
                    commit_fh.write(
                        json.dumps({"done": m["commit_index"]})
                        .encode("ascii") + b"\n")
                    commit_fh.flush()
                    os.fsync(commit_fh.fileno())
            finally:
                commit_fh.close()

            # --- CONVERGE with racing appenders, per member ----------
            for m in dirty:
                _converge_group_member(m, quiesce, audit_stream, on_bad,
                                       warnings)
            _crash_point("group-converge")

        # --- CLEANUP: never turns a committed group into a failure ---
        try:
            _crash_point("group-cleanup")
            if os.environ.get(_FAULT_ENV) == "cleanup":
                raise OSError("injected cleanup fault")
            for m in members:
                shutil.rmtree(m["tmp_dir"], ignore_errors=True)
            shutil.rmtree(group_dir, ignore_errors=True)
        except OSError as exc:
            warnings.append(f"cleanup failed: {exc}")
    except BaseException:
        for m in members:
            _close_group_member(m)
        if not committed:
            # Pre-commit: roll the whole group back without a trace.
            # No rename has run, so every original file is untouched.
            for m in members:
                shutil.rmtree(m["tmp_dir"], ignore_errors=True)
            shutil.rmtree(group_dir, ignore_errors=True)
        raise
    finally:
        for m in members:
            _close_group_member(m)
        for fh in locks:
            try:
                fcntl.flock(fh, fcntl.LOCK_UN)
            finally:
                fh.close()

    results = []
    for m in members:
        salvaged, skipped, warning = recovery_counts.get(
            m["abspath"], (0, 0, None))
        results.append(MigrationResult(
            path=m["path"],
            records_migrated=m["run_migrated"],
            records_skipped=m["run_skipped"] + skipped,
            records_salvaged=m["salvaged"] + salvaged,
            replaced=m["replaced"],
            post_commit_error=m["warning"] or warning,
        ))
    return _group_result(results, warnings)


# ---------------------------------------------------------------------------
# Public consistent group read entry
# ---------------------------------------------------------------------------


def read_log_group(paths, *, quiesce=DEFAULT_QUIESCE):
    """Return one complete, version-consistent view of a whole log group.

    A list parallel to *paths* is returned, each element that member's
    decoded records.  Old and new field shapes never mix inside the
    view -- neither within a member nor between members: while no
    member's commit marker exists every member is decoded exactly as
    stored, and as soon as any member's marker exists (the group's
    commit sequence has begun or finished) every member is normalized
    to the current version, so a snapshot taken mid-commit or while a
    writer reopens a path after wrap-up is still uniform.

    Each member's snapshot pins one inode for the whole read, so an
    atomic replacement mid-read cannot straddle two files, and only
    newline-terminated lines are included, so a record caught
    mid-append never appears.  A complete but undecodable line raises
    ValueError, like :func:`loads`.  A missing or unreadable member
    raises FileNotFoundError.
    """
    paths = _validate_manifest(paths)
    _require_readable(paths)
    markers = [_commit_marker_path(p) for p in paths]

    def normalized():
        views = []
        for p in paths:
            with open(p, "rb") as fh:
                blob = fh.read()
            views.append([migrate(rec, CURRENT_VERSION)
                          for rec in _complete_lines(blob)])
        return views

    # Phase A: no commit marker on any member.  Marker publication
    # strictly precedes every rename, so the paths still name the
    # pre-migration inodes; re-check after the reads in case the commit
    # sequence started meanwhile.
    if not any(os.path.exists(m) for m in markers):
        blobs = []
        for p in paths:
            with open(p, "rb") as fh:
                blobs.append(fh.read())
        if not any(os.path.exists(m) for m in markers):
            return [_complete_lines(blob) for blob in blobs]

    # Phase B: the commit sequence has begun or finished.  Normalizing
    # every record makes the view uniform no matter how many member
    # renames have landed.
    return normalized()


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


def run_group_cli(argv):
    parser = argparse.ArgumentParser(
        prog="python3 -m proto_migrate migrate-logs",
        description="Migrate a whole set of append-only JSONL logs to the "
        "current record version as one all-or-nothing group: online "
        "(writers keep appending), resumable from durable checkpoints, "
        "and atomic across the whole group.",
    )
    parser.add_argument("paths", nargs="+", metavar="FILE",
                        help="log files to migrate in place as one group")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--strict", action="store_const", const="strict",
                      dest="on_bad", help="abort the whole group on the "
                      "first bad line (default; exit %d, every file "
                      "untouched)" % EXIT_BAD_RECORD)
    mode.add_argument("--skip", action="store_const", const="skip",
                      dest="on_bad", help="skip bad lines, auditing each "
                      "to stderr as '<file>:<lineno>:<first 32 bytes>'")
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

    try:
        result = migrate_log_group(
            args.paths,
            on_bad=args.on_bad,
            segment_size=args.segment_size,
            quiesce=args.quiesce_ms / 1000.0,
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
        "migrated=%d skipped=%d salvaged=%d replaced=%d"
        % (
            result.records_migrated,
            result.records_skipped,
            result.records_salvaged,
            result.replaced,
        )
    )
    if result.post_commit_error:
        # The group commit already succeeded: later faults show up as
        # a warning, never as a non-zero exit.
        print(f"warning: {result.post_commit_error}", file=sys.stderr)
    return EXIT_OK
