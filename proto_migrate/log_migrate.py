"""Crash-safe batch migration of append-only JSONL logs.

The whole log is migrated to :data:`proto_migrate.CURRENT_VERSION` in a
single streaming pass (constant memory regardless of file size).  The
result is first written to *segmented* temporary files, merged into one
staged file, fsynced, and only then atomically renamed over the
original path.  A kill at any point therefore leaves on disk either the
untouched original or the complete migrated file -- never a mixture.

Crash protocol
--------------
Just before the rename a hard link (``.<log>.migrate.old``) to the old
inode and an offset marker (``.<log>.migrate.state``) are created and
fsynced.  Records an appender writes to the old inode after the rename
can therefore still be reached on a rerun through the hard link: the
rerun resumes from the marker offset and appends them to the new file,
checkpointing the marker (atomically) after every fsynced line.  In
strict mode a bad residual line rolls the rename back (the old inode is
renamed into place again, concurrent appends and all) and raises.

Concurrency model (see README for the documented trade-offs):

* A ``fcntl`` exclusive lock on a sidecar file serialises whole-file
  migrators; a second invocation after migration is a pure no-op.
* Records other processes append while the rewrite is in progress are
  not lost.  The scan follows growth past a quiet settle window and
  folds appended lines into the output.  Appenders holding a descriptor
  on the old inode across the rename are drained via the hard link;
  appenders that reopen the path after the rename land directly on the
  new file.
* Replacement is a single atomic ``rename(2)``: at every instant a
  reader opening the path sees either the old file or the new file,
  never a missing or half-written path.
"""

from __future__ import annotations

import fcntl
import glob
import io
import os
import shutil
import time
from dataclasses import dataclass, field

from . import CURRENT_VERSION, dumps, loads, migrate

__all__ = [
    "LOG_STRICT",
    "LOG_SKIP",
    "BadRecordError",
    "MigrationResult",
    "migrate_log",
]

LOG_STRICT = "strict"
LOG_SKIP = "skip"
_MODES = (LOG_STRICT, LOG_SKIP)

#: Exit status used by the CLI when strict mode meets the first bad record.
EXIT_BAD_RECORD = 3

#: Size cap of one segmented temporary part before rotation.
DEFAULT_SEGMENT_SIZE = 8 * 1024 * 1024

#: Drain settle loop: EOF is treated as final only after the source has
#: stayed unchanged for this many probes ...
_SETTLE_ATTEMPTS = 100
#: ... spaced by this interval (default quiet window: 1 second).
_SETTLE_INTERVAL = 0.01

_SNIPPET_BYTES = 32

#: Exit status used when a crash-injection hook fires (tests only).
_CRASH_EXIT = 99


class BadRecordError(ValueError):
    """A log line is corrupt or fails codec validation (strict mode).

    Subclasses :class:`ValueError` as required for rejected payloads
    (NaN/Infinity constants, overflow-to-Infinity literals, bad or
    missing version keys, schema violations, unparseable JSON).
    """

    def __init__(self, lineno, raw, reason):
        self.lineno = lineno
        self.raw = raw
        self.reason = reason
        super().__init__(
            f"bad record at line {lineno}: {reason}: {_snippet(raw)!r}"
        )


@dataclass
class MigrationResult:
    """Outcome of one :func:`migrate_log` call."""

    path: str
    changed: bool
    total_lines: int = 0
    migrated_lines: int = 0
    skipped: int = 0
    audit: list[tuple[int, bytes]] = field(default_factory=list)


def _snippet(raw):
    """First 32 raw bytes of the line, without the line terminator."""
    return raw.rstrip(b"\r\n")[:_SNIPPET_BYTES]


def _encode_line(decoded):
    """Re-encode one decoded record at the current version (bytes + NL)."""
    message = migrate(decoded, CURRENT_VERSION)
    payload = {key: value for key, value in message.items() if key != "v"}
    return dumps(payload)


class _PartWriter:
    """Rotating, individually fsynced segmented temporary files."""

    def __init__(self, directory, segment_size, crash):
        self._dir = directory
        self._segment_size = segment_size
        self._crash = crash
        self.names = []
        self._fh = None
        self._bytes = 0
        self._index = -1
        self._open_part()

    def _open_part(self):
        self._index += 1
        name = os.path.join(self._dir, f"part-{self._index:06d}")
        self.names.append(name)
        self._fh = open(name, "wb")
        self._bytes = 0

    def _rotate(self):
        self._fh.flush()
        os.fsync(self._fh.fileno())
        self._fh.close()
        self._fh = None
        self._crash("part_fsync")
        self._open_part()

    def write(self, data):
        if self._fh is None:
            self._open_part()
        if self._bytes and self._bytes + len(data) > self._segment_size:
            self._rotate()
        self._fh.write(data)
        self._bytes += len(data)

    def close(self):
        if self._fh is not None:
            self._fh.flush()
            os.fsync(self._fh.fileno())
            self._fh.close()
            self._fh = None
            self._crash("part_fsync")


def _fsync_dir(path):
    dir_fd = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)


def _wait_for_growth(fd, last_size, attempts, interval):
    """Return the new size if the file grows within the settle window."""
    for _ in range(attempts):
        time.sleep(interval)
        if os.fstat(fd).st_size != last_size:
            return os.fstat(fd).st_size
    return None


class _Migrator:
    def __init__(
        self,
        path,
        *,
        mode,
        audit_stream,
        segment_size,
        settle_attempts,
        settle_interval,
        crash,
    ):
        self.path = path
        self.directory = os.path.dirname(path) or "."
        base = os.path.basename(path)
        self.base = base
        self.mode = mode
        self.audit_stream = audit_stream
        self.segment_size = segment_size
        self.settle_attempts = settle_attempts
        self.settle_interval = settle_interval
        self.crash = crash

        self.old_link = os.path.join(self.directory, f".{base}.migrate.old")
        self.marker = os.path.join(self.directory, f".{base}.migrate.state")
        self.marker_tmp = self.marker + ".tmp"
        self.work_glob = os.path.join(self.directory, f".{base}.migrate.*")

        self.result = MigrationResult(path=path, changed=False)
        self.lineno = 0

    # ---------- helpers ----------

    def _same_inode(self, a, b):
        sa, sb = os.stat(a), os.stat(b)
        return (sa.st_dev, sa.st_ino) == (sb.st_dev, sb.st_ino)

    def _write_audit(self, number, raw):
        snippet = _snippet(raw)
        self.result.audit.append((number, snippet))
        self.result.skipped += 1
        if self.audit_stream is not None:
            self.audit_stream.write(f"{number}:".encode("ascii"))
            self.audit_stream.write(snippet)
            self.audit_stream.write(b"\n")
            flush = getattr(self.audit_stream, "flush", None)
            if flush is not None:
                flush()

    def _checkpoint(self, offset, lineno):
        """Durably record how far the old inode has been copied."""
        with open(self.marker_tmp, "wb") as fh:
            fh.write(f"{offset} {lineno}\n".encode("ascii"))
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(self.marker_tmp, self.marker)
        _fsync_dir(self.directory)

    def _read_marker(self):
        with open(self.marker, "rb") as fh:
            offset, lineno = fh.read().split()
        return int(offset), int(lineno)

    def _remove_if_exists(self, target):
        try:
            os.unlink(target)
        except FileNotFoundError:
            return
        _fsync_dir(self.directory)

    def _validate(self, line, number):
        """Return the re-encoded line, or None if it is a skipped bad one."""
        try:
            return _encode_line(loads(line))
        except ValueError as exc:
            if self.mode == LOG_STRICT:
                raise BadRecordError(number, line, str(exc)) from exc
            self._write_audit(number, line)
            return None

    # ---------- residual drain (old inode -> new file) ----------

    def _drain_residual(self, old_fd, new_path, offset, number):
        """Copy old-inode bytes from ``offset`` onto the new file.

        Every copied line is fsynced and the marker checkpointed before
        the next one, so a crash here resumes exactly-once.  Follows an
        appender still writing to the old inode until it goes quiet.
        """
        reader = io.open(old_fd, "rb", closefd=False)
        reader.seek(offset)
        new_fh = open(new_path, "ab")
        known_size = os.fstat(old_fd).st_size
        try:
            while True:
                line = reader.readline()
                if line == b"":
                    grown = _wait_for_growth(
                        old_fd,
                        known_size,
                        self.settle_attempts,
                        self.settle_interval,
                    )
                    if grown is None:
                        break
                    known_size = grown
                    continue
                if not line.endswith(b"\n"):
                    grown = _wait_for_growth(
                        old_fd,
                        known_size,
                        self.settle_attempts,
                        self.settle_interval,
                    )
                    if grown is not None:
                        known_size = grown
                        reader.seek(offset)
                        continue
                    # torn tail of a dead appender: judge it as a line
                number += 1
                self.result.total_lines += 1
                encoded = self._validate(line, number)
                if encoded is not None:
                    if encoded != line:
                        self.result.migrated_lines += 1
                    new_fh.write(encoded)
                    new_fh.flush()
                    os.fsync(new_fh.fileno())
                    offset = reader.tell()
                    self._checkpoint(offset, number)
                    self.crash("checkpoint")
                else:
                    offset = reader.tell()
                    self._checkpoint(offset, number)
                known_size = os.fstat(old_fd).st_size
            new_fh.flush()
            os.fsync(new_fh.fileno())
        finally:
            new_fh.close()
            reader.close()
        return offset, number

    def _rollback_rename(self):
        """Put the old inode (hard link) back at the log path."""
        os.replace(self.old_link, self.path)
        _fsync_dir(self.directory)
        self._remove_if_exists(self.marker)

    # ---------- recovery from an interrupted earlier run ----------

    def _recover(self):
        """Finish or undo an interrupted migration.

        Returns a :class:`MigrationResult` if the recovery itself
        completed the migration (caller should stop), else ``None``.
        """
        have_marker = os.path.exists(self.marker)
        have_link = os.path.exists(self.old_link)
        if not have_marker and not have_link:
            return None

        if have_link and (
            not have_marker or self._same_inode(self.path, self.old_link)
        ):
            # Either the main rename never happened (path is still the
            # old inode) or no marker exists: nothing was replaced.
            if have_link:
                self._remove_if_exists(self.old_link)
            self._remove_if_exists(self.marker)
            self._remove_if_exists(self.marker_tmp)
            return None

        if not have_link:
            # Drain had finished and the link was removed; only the
            # marker deletion was left pending.
            self._remove_if_exists(self.marker)
            return None

        # The rename happened: path is the new inode, the hard link
        # still reaches the old inode with late appends. Resume.
        offset, number = self._read_marker()
        old_fd = os.open(self.old_link, os.O_RDONLY)
        try:
            try:
                offset, number = self._drain_residual(
                    old_fd, self.path, offset, number
                )
            except BadRecordError:
                # Strict mode: undo the replacement, preserve appends.
                self._rollback_rename()
                raise
        finally:
            os.close(old_fd)

        self.result.changed = True
        self._remove_if_exists(self.old_link)
        self._remove_if_exists(self.marker)
        self._remove_if_exists(self.marker_tmp)
        return self.result if self.result.total_lines else MigrationResult(
            path=self.path, changed=True
        )

    # ---------- main path ----------

    def run(self):
        recovered = self._recover()
        if recovered is not None:
            return recovered

        work_dir = os.path.join(
            self.directory, f".{self.base}.migrate.{os.getpid()}"
        )
        suffix = 0
        while os.path.exists(work_dir):
            suffix += 1
            work_dir = os.path.join(
                self.directory, f".{self.base}.migrate.{os.getpid()}.{suffix}"
            )
        os.mkdir(work_dir)
        try:
            return self._run(work_dir)
        finally:
            shutil.rmtree(work_dir, ignore_errors=True)

    def _run(self, work_dir):
        fd = os.open(self.path, os.O_RDONLY)
        reader = io.open(fd, "rb", closefd=True)
        start_stat = os.fstat(fd)
        start_id = (start_stat.st_dev, start_stat.st_ino)

        parts = _PartWriter(work_dir, self.segment_size, self.crash)
        all_identical = True
        processed = 0

        def same_inode():
            st = os.fstat(fd)
            return (st.st_dev, st.st_ino) == start_id

        merge_path = os.path.join(work_dir, "migrated.jsonl")

        def consume(line):
            nonlocal all_identical
            encoded = self._validate(line, self.lineno)
            if encoded is None:
                all_identical = False
                return
            if encoded != line:
                all_identical = False
                self.result.migrated_lines += 1
            parts.write(encoded)

        def drain(existing_tail):
            """Scan to a stable EOF, following a concurrent appender."""
            nonlocal processed
            known_size = os.fstat(fd).st_size
            waiting = not existing_tail
            while True:
                line = reader.readline()
                if line == b"":
                    if not waiting:
                        return
                    grown = _wait_for_growth(
                        fd,
                        known_size,
                        self.settle_attempts,
                        self.settle_interval,
                    )
                    if grown is None:
                        return
                    if not same_inode():
                        raise RuntimeError(
                            "log file was replaced during migration"
                        )
                    known_size = grown
                    continue
                if not line.endswith(b"\n"):
                    # Unterminated tail: torn append in flight or a
                    # file genuinely missing its final newline. Wait
                    # before judging so a torn line is never migrated.
                    grown = _wait_for_growth(
                        fd,
                        known_size,
                        self.settle_attempts,
                        self.settle_interval,
                    )
                    if grown is not None:
                        if not same_inode():
                            raise RuntimeError(
                                "log file was replaced during migration"
                            )
                        known_size = grown
                        reader.seek(processed)
                        continue
                    # size stable: genuine newline-less last line
                waiting = True
                self.lineno += 1
                self.result.total_lines += 1
                consume(line)
                self.crash("scan_line")
                processed = reader.tell()
                known_size = os.fstat(fd).st_size

        def stage():
            with open(merge_path, "wb") as out:
                for name in parts.names:
                    with open(name, "rb") as part:
                        shutil.copyfileobj(part, out, length=1024 * 1024)
                self.crash("after_merge")
                out.flush()
                os.fsync(out.fileno())
            self.crash("after_merge_fsync")

        try:
            # ---- single streaming scan, following appenders -------
            drain(existing_tail=True)

            # Close (flush+fsync) parts so the merge reads every byte.
            # If the source grew while staging, drain again: writes
            # reopen a fresh part, and the next merge includes it.
            while True:
                parts.close()
                self.crash("before_merge")
                stage()
                size = os.fstat(fd).st_size
                if not same_inode():
                    raise RuntimeError(
                        "log file was replaced during migration"
                    )
                if size == processed:
                    break
                drain(existing_tail=True)

            if all_identical:
                # Already canonical current-version content: never
                # rewrite a single byte (idempotent rerun).
                self.crash("skip_noop")
                return self.result

            # ---- arm the crash protocol, then replace --------------
            self._checkpoint(processed, self.lineno)
            os.link(self.path, self.old_link)
            _fsync_dir(self.directory)
            self.crash("after_link")

            self.crash("before_rename")
            os.replace(merge_path, self.path)
            self.crash("after_rename")

            # Fold in appends that landed on the old inode after the
            # rename (resumable via marker + hard link).
            try:
                _, self.lineno = self._drain_residual(
                    fd, self.path, processed, self.lineno
                )
            except BadRecordError:
                self._rollback_rename()
                raise
            self.crash("after_residual")

            self._remove_if_exists(self.old_link)
            self._remove_if_exists(self.marker)
            self._remove_if_exists(self.marker_tmp)
            self.crash("after_dir_fsync")
            self.result.changed = True
            return self.result
        finally:
            try:
                reader.close()
            except OSError:
                pass


def migrate_log(
    path,
    *,
    mode=LOG_STRICT,
    audit_stream=None,
    segment_size=DEFAULT_SEGMENT_SIZE,
    settle_attempts=_SETTLE_ATTEMPTS,
    settle_interval=_SETTLE_INTERVAL,
    crash=None,
):
    """Migrate JSONL log ``path`` to the current version atomically.

    ``mode`` is :data:`LOG_STRICT` (default) or :data:`LOG_SKIP`.
    In strict mode the first bad line raises :class:`BadRecordError`
    and the original file is left byte-for-byte intact (a bad line
    discovered after the atomic rename rolls the rename back).  In
    skip mode bad lines are omitted from the result and one
    ``<lineno>:<first 32 raw bytes>`` audit entry per bad line is
    appended to ``audit_stream`` (a binary file-like object) *and*
    returned on the result.

    ``crash`` is an optional ``callable(point)`` used by crash-injection
    tests; if it returns a truthy value the process terminates
    immediately via ``os._exit`` (no cleanup, emulating SIGKILL).

    A file already byte-identical to the canonical current-version
    encoding is left completely untouched (``changed=False``), making
    repeated runs idempotent at byte level.
    """
    if mode not in _MODES:
        raise ValueError(f"mode must be {LOG_STRICT!r} or {LOG_SKIP!r}")

    def no_crash(point):
        return False

    crash = crash or no_crash

    def die(point):
        if crash(point):
            os._exit(_CRASH_EXIT)

    path = os.path.abspath(os.fspath(path))

    # Sidecar advisory lock serialises whole-file migrators.
    lock_fh = open(path + ".migrate.lock", "a+b")
    try:
        fcntl.flock(lock_fh.fileno(), fcntl.LOCK_EX)

        # We hold the only migrator lock: work directories matching the
        # pattern are leftovers from crashed runs and safe to remove.
        # Fixed-name files (marker/link) are handled by _recover().
        for stale in glob.glob(
            os.path.join(
                os.path.dirname(path) or ".",
                f".{os.path.basename(path)}.migrate.*",
            )
        ):
            if os.path.isdir(stale):
                shutil.rmtree(stale, ignore_errors=True)

        migrator = _Migrator(
            path,
            mode=mode,
            audit_stream=audit_stream,
            segment_size=segment_size,
            settle_attempts=settle_attempts,
            settle_interval=settle_interval,
            crash=die,
        )
        return migrator.run()
    finally:
        fcntl.flock(lock_fh.fileno(), fcntl.LOCK_UN)
        lock_fh.close()
