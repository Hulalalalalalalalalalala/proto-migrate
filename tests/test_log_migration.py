import io
import json
import os
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import warnings
from unittest import mock

import proto_migrate
import proto_migrate.log_migration as log_migration
from proto_migrate import CURRENT_VERSION, dumps
from proto_migrate.log_migration import (
    EXIT_BAD_RECORD,
    EXIT_OK,
    EXIT_USAGE,
    BadRecordError,
    migrate_log_file,
)

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def v1(order_id, amount=1.0):
    return (
        b'{"v":1,"order_id":'
        + json.dumps(order_id).encode()
        + b',"amount":'
        + repr(amount).encode()
        + b',"tags":[]}\n'
    )


def v2(order_id, status="paid"):
    return (
        b'{"v":2,"order_id":'
        + json.dumps(order_id).encode()
        + b',"amount":2.5,"tags":["t"],"status":'
        + json.dumps(status).encode()
        + b"}\n"
    )


def v3(order_id, amount=3.0, status="paid", note="", updated_at=0):
    return dumps(
        {
            "order_id": order_id,
            "amount": amount,
            "status": status,
            "note": note,
            "updated_at": updated_at,
        }
    )


def read_bytes(path):
    with open(path, "rb") as f:
        return f.read()


def versions_of(blob):
    return [json.loads(line)["v"] for line in blob.splitlines() if line]


def versions_lenient(blob):
    """Like versions_of but ignores a torn final line from a live append."""
    out = []
    for line in blob.splitlines():
        try:
            out.append(json.loads(line)["v"])
        except (ValueError, KeyError):
            pass
    return out


class TestBasicMigration(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmp.name, "log.jsonl")

    def tearDown(self):
        self.tmp.cleanup()

    def test_mixed_versions_all_become_current(self):
        data = v1("A") + v2("B") + v3("C", note="n")
        with open(self.path, "wb") as f:
            f.write(data)
        result = migrate_log_file(self.path, quiesce=0.01)
        self.assertTrue(result.replaced)
        blob = read_bytes(self.path)
        records = [json.loads(line) for line in blob.splitlines()]
        self.assertEqual([r["v"] for r in records], [3, 3, 3])
        self.assertEqual(records[0], {
            "v": 3, "order_id": "A", "amount": 1.0,
            "status": "new", "note": "", "updated_at": 0,
        })
        self.assertEqual(records[1]["status"], "paid")
        self.assertNotIn("tags", records[1])
        self.assertEqual(records[2]["note"], "n")
        # Every output line is canonical dumps output.
        for r in records:
            self.assertEqual(
                dumps({k: val for k, val in r.items() if k != "v"}),
                (json.dumps(r, separators=(",", ":"), ensure_ascii=True) + "\n")
                .encode(),
            )

    def test_negative_zero_preserved(self):
        with open(self.path, "wb") as f:
            f.write(v3("Z", amount=-0.0))
        migrate_log_file(self.path, quiesce=0.01)
        blob = read_bytes(self.path)
        self.assertIn(b"-0.0", blob)

    def test_last_line_without_newline_still_migrated(self):
        with open(self.path, "wb") as f:
            f.write(v1("A"))
            f.write(v1("B")[:-1])  # no trailing newline
        result = migrate_log_file(self.path, quiesce=0.02)
        self.assertTrue(result.replaced)
        self.assertEqual(versions_of(read_bytes(self.path)), [3, 3])

    def test_segment_rotation(self):
        data = b"".join(v1(f"O{i}") for i in range(60))
        with open(self.path, "wb") as f:
            f.write(data)
        result = migrate_log_file(self.path, segment_size=200, quiesce=0.01)
        self.assertTrue(result.replaced)
        blob = read_bytes(self.path)
        self.assertEqual(len(blob.splitlines()), 60)
        self.assertTrue(all(v == 3 for v in versions_of(blob)))

    def test_no_temp_left_behind(self):
        with open(self.path, "wb") as f:
            f.write(v1("A"))
        migrate_log_file(self.path, quiesce=0.01)
        self.assertFalse(os.path.exists(self.path + ".migrate-tmp"))
        # A stale temp dir from a killed earlier run is swept.
        os.mkdir(self.path + ".migrate-tmp")
        with open(self.path + ".migrate-tmp/junk", "wb") as f:
            f.write(b"x")
        migrate_log_file(self.path, quiesce=0.01)
        self.assertFalse(os.path.exists(self.path + ".migrate-tmp"))


class TestBadRecords(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmp.name, "log.jsonl")

    def tearDown(self):
        self.tmp.cleanup()

    BAD = [
        b"not json at all\n",
        b'{"v":1,"order_id":"A","amount":NaN,"tags":[]}\n',
        b'{"v":1,"order_id":"A","amount":Infinity,"tags":[]}\n',
        b'{"v":1,"order_id":"A","amount":-Infinity,"tags":[]}\n',
        b'{"v":1,"order_id":"A","amount":1e999,"tags":[]}\n',
        b'{"order_id":"A","amount":1,"tags":[]}\n',   # missing v
        b'{"v":"3"}\n',                                # non-integer v
        b'{"v":3.0}\n',                                # float v
        b'{"v":4,"order_id":"A","amount":1}\n',        # unsupported v
        b'{"v":1,"order_id":"A","amount":1}\n',        # missing field
    ]

    def test_strict_rejects_each_kind_and_preserves_file(self):
        for bad in self.BAD:
            with open(self.path, "wb") as f:
                f.write(v1("good-1") + bad + v1("good-2"))
            before = read_bytes(self.path)
            with self.assertRaises(BadRecordError) as ctx:
                migrate_log_file(self.path, quiesce=0.01)
            self.assertIsInstance(ctx.exception, ValueError)
            self.assertEqual(ctx.exception.lineno, 2)
            after = read_bytes(self.path)
            self.assertEqual(before, after)
            self.assertFalse(os.path.exists(self.path + ".migrate-tmp"))

    def test_strict_stops_at_first_bad_line(self):
        with open(self.path, "wb") as f:
            f.write(v1("A") + b"FIRST BAD\n" + b"SECOND BAD\n")
        with self.assertRaises(BadRecordError) as ctx:
            migrate_log_file(self.path, quiesce=0.01)
        self.assertEqual(ctx.exception.lineno, 2)
        self.assertEqual(read_bytes(self.path),
                         v1("A") + b"FIRST BAD\n" + b"SECOND BAD\n")

    def test_skip_audits_and_migrates(self):
        lines = [v1("A"), b"BROKEN\n", v1("C"),
                 b'{"v":1,"order_id":"X","amount":1e400,"tags":[]}\n',
                 b'{"v":9}\n']
        with open(self.path, "wb") as f:
            f.write(b"".join(lines))
        audit = io.BytesIO()
        result = migrate_log_file(self.path, on_bad="skip",
                                  quiesce=0.01, audit=audit)
        self.assertEqual(result.records_skipped, 3)
        self.assertEqual(versions_of(read_bytes(self.path)), [3, 3])
        entries = audit.getvalue().splitlines()
        self.assertEqual(entries[0], b"2:BROKEN")
        self.assertEqual(entries[1],
                         b"4:" + lines[3][:32].rstrip(b"\n"))
        self.assertEqual(entries[2], b"5:" + b'{"v":9}')

    def test_skip_audit_snippet_is_exactly_32_bytes(self):
        with open(self.path, "wb") as f:
            f.write(v1("A") + b"x" * 50 + b"\n")
        audit = io.BytesIO()
        migrate_log_file(self.path, on_bad="skip", quiesce=0.01, audit=audit)
        self.assertEqual(audit.getvalue(), b"2:" + b"x" * 32 + b"\n")

    def test_bad_on_bad_argument(self):
        with open(self.path, "wb") as f:
            f.write(v1("A"))
        with self.assertRaises(ValueError):
            migrate_log_file(self.path, on_bad="explode")


class TestIdempotency(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmp.name, "log.jsonl")

    def tearDown(self):
        self.tmp.cleanup()

    def test_second_run_changes_nothing(self):
        with open(self.path, "wb") as f:
            f.write(v3("A") + v3("B", amount=-0.0))
        first = read_bytes(self.path)
        r1 = migrate_log_file(self.path, quiesce=0.01)
        self.assertFalse(r1.replaced)
        after_first = read_bytes(self.path)
        self.assertEqual(first, after_first)
        r2 = migrate_log_file(self.path, quiesce=0.01)
        self.assertFalse(r2.replaced)
        self.assertEqual(read_bytes(self.path), first)

    def test_second_run_after_real_migration_byte_identical(self):
        with open(self.path, "wb") as f:
            f.write(v1("A") + v2("B"))
        migrate_log_file(self.path, quiesce=0.01)
        once = read_bytes(self.path)
        st = os.stat(self.path)
        time.sleep(0.02)  # any rewrite would bump mtime
        result = migrate_log_file(self.path, quiesce=0.01)
        self.assertFalse(result.replaced)
        self.assertEqual(read_bytes(self.path), once)
        self.assertEqual(os.stat(self.path).st_mtime_ns, st.st_mtime_ns)
        self.assertEqual(os.stat(self.path).st_ino, st.st_ino)


CRASH_RUNNER = r"""
import os, sys
sys.path.insert(0, %r)
os.environ["PROTO_MIGRATE_CRASH_AT"] = sys.argv[2]
from proto_migrate.log_migration import run_cli
flag = "--skip" if sys.argv[3] == "skip" else "--strict"
raise SystemExit(run_cli([sys.argv[1], flag,
                          "--quiesce-ms", "20",
                          "--segment-size", sys.argv[4]]))
""" % REPO_ROOT


class TestCrashInjection(unittest.TestCase):
    CHECKPOINTS = ["lock", "segment", "segments", "assemble", "replace"]

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmp.name, "log.jsonl")
        self.original = b"".join(v1(f"O{i}") for i in range(60))
        with open(self.path, "wb") as f:
            f.write(self.original)

    def tearDown(self):
        self.tmp.cleanup()

    def _crash(self, checkpoint, on_bad="strict"):
        proc = subprocess.run(
            [sys.executable, "-c", CRASH_RUNNER, self.path,
             checkpoint, on_bad, "200"],
            capture_output=True,
        )
        return proc

    def _rerun_clean(self):
        proc = subprocess.run(
            [sys.executable, "-c", CRASH_RUNNER, self.path,
             "none", "strict", str(16 * 1024 * 1024)],
            capture_output=True,
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)

    def test_each_checkpoint_then_rerun(self):
        for checkpoint in self.CHECKPOINTS:
            with self.subTest(checkpoint=checkpoint):
                # Reset the file for every checkpoint.
                with open(self.path, "wb") as f:
                    f.write(self.original)
                proc = self._crash(checkpoint)
                self.assertEqual(proc.returncode, 1, proc.stderr)

                if checkpoint == "replace":
                    # The rename already happened: new file is complete.
                    blob = read_bytes(self.path)
                    self.assertEqual(len(blob.splitlines()), 60)
                    self.assertTrue(all(v == 3 for v in versions_of(blob)))
                    migrated = blob
                else:
                    # Killed before the rename: original byte-for-byte.
                    self.assertEqual(read_bytes(self.path),
                                     self.original)
                    self._rerun_clean()
                    migrated = read_bytes(self.path)
                    self.assertEqual(len(migrated.splitlines()), 60)
                    self.assertTrue(
                        all(v == 3 for v in versions_of(migrated))
                    )

                # Idempotent after recovery: a further run rewrites
                # nothing.
                proc = subprocess.run(
                    [sys.executable, "-c", CRASH_RUNNER, self.path,
                     "none", "strict", str(16 * 1024 * 1024)],
                    capture_output=True,
                )
                self.assertEqual(proc.returncode, 0)
                self.assertIn(b"replaced=no", proc.stdout)
                self.assertEqual(read_bytes(self.path), migrated)

    def test_strict_crash_leaves_no_half_state_and_rerun_still_aborts(self):
        data = v1("A") + b"BAD\n" + v1("C")
        for checkpoint in ("segment", "segments", "assemble"):
            with self.subTest(checkpoint=checkpoint):
                with open(self.path, "wb") as f:
                    f.write(data)
                proc = self._crash(checkpoint)
                # The injected crash fires before the bad line is even
                # reached for some checkpoints; either way the target is
                # untouched.
                self.assertEqual(read_bytes(self.path), data)
                proc = subprocess.run(
                    [sys.executable, "-c", CRASH_RUNNER, self.path,
                     "none", "strict", str(16 * 1024 * 1024)],
                    capture_output=True,
                )
                self.assertEqual(proc.returncode, EXIT_BAD_RECORD)
                self.assertEqual(read_bytes(self.path), data)


class TestConcurrentAppend(unittest.TestCase):
    APPENDED = 25

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmp.name, "log.jsonl")
        self.lock = self.path + ".migrate.lock"
        # Big enough that the migration outlasts the appender, but not
        # so big that 80k lines make the suite slow.
        with open(self.path, "wb") as f:
            for i in range(20_000):
                f.write(v1(f"base-{i}"))

    def tearDown(self):
        self.tmp.cleanup()

    def _appender(self):
        # Start only once the migrator has taken its lock, then append
        # throughout the scan/rewrite, opening the file by path per line.
        for _ in range(1000):
            if os.path.exists(self.lock):
                break
            time.sleep(0.001)
        for i in range(self.APPENDED):
            with open(self.path, "ab") as f:
                f.write(v2(f"late-{i}"))
            time.sleep(0.002)

    def _start_appender(self):
        t = threading.Thread(target=self._appender)
        t.start()
        return t

    def test_appended_records_all_survive_as_current_version(self):
        t = self._start_appender()
        result = migrate_log_file(self.path, quiesce=0.02)
        t.join()
        blob = read_bytes(self.path)
        records = [json.loads(line) for line in blob.splitlines()]
        self.assertEqual(
            len(records), 20_000 + self.APPENDED,
            f"migrated={result}",
        )
        self.assertTrue(all(r["v"] == CURRENT_VERSION for r in records))
        late = {r["order_id"] for r in records
                if r["order_id"].startswith("late-")}
        self.assertEqual(len(late), self.APPENDED)
        for r in records:
            if r["order_id"].startswith("late-"):
                self.assertEqual(r["status"], "paid")
                self.assertNotIn("tags", r)

    def test_concurrent_readers_never_see_half_migration(self):
        stop = threading.Event()
        snapshots = []
        errors = []

        def reader():
            try:
                while not stop.is_set():
                    with open(self.path, "rb") as f:
                        blob = f.read()
                    versions = versions_lenient(blob)
                    if versions:
                        # Compact signature: versions present, count.
                        snapshots.append((frozenset(versions), len(versions)))
                    time.sleep(0.001)
            except OSError as exc:  # pragma: no cover
                errors.append(exc)

        r = threading.Thread(target=reader)
        r.start()
        t = self._start_appender()
        migrate_log_file(self.path, quiesce=0.02)
        t.join()
        stop.set()
        r.join()
        self.assertEqual(errors, [])
        self.assertTrue(snapshots)
        for present, count in snapshots:
            # Forbidden state: migrated v3 records mixed with old
            # versions inside one view of the file.
            if 3 in present:
                self.assertEqual(present, {3},
                                 f"mixed view: {sorted(present)}")
        self.assertIn(snapshots[0][0], ({1}, {1, 2}))
        final_versions = versions_of(read_bytes(self.path))
        self.assertEqual(set(final_versions), {3})
        self.assertEqual(len(final_versions), 20_000 + self.APPENDED)


    def test_old_record_opened_after_rename_gets_migrated(self):
        # Appender that detects the rename (inode change) and then keeps
        # appending old-format records BY PATH, tightly, so the records
        # land on the new inode while convergence is active.  The
        # convergence phase must repair that mixed tail with further
        # atomic replacements; no v1 may remain and none may be lost.
        inode_before = os.stat(self.path).st_ino
        n = 6

        def late_appender():
            for _ in range(2000):
                if os.stat(self.path).st_ino != inode_before:
                    break
                time.sleep(0.001)
            for i in range(n):
                with open(self.path, "ab") as f:
                    f.write(v1(f"post-rename-{i}", amount=7.0))
                time.sleep(0.005)

        t = threading.Thread(target=late_appender)
        t.start()
        result = migrate_log_file(self.path, quiesce=0.02)
        t.join(timeout=5)
        blob = read_bytes(self.path)
        records = [json.loads(line) for line in blob.splitlines()]
        self.assertTrue(all(r["v"] == 3 for r in records),
                        {r["v"] for r in records})
        post = [r for r in records
                if r["order_id"].startswith("post-rename-")]
        self.assertEqual(len(post), n)
        self.assertEqual(
            {r["order_id"] for r in post},
            {f"post-rename-{i}" for i in range(n)},
        )
        for r in post:
            self.assertEqual(r["amount"], 7.0)
            self.assertNotIn("tags", r)
        # Idempotent afterwards.
        again = migrate_log_file(self.path, quiesce=0.02)
        self.assertFalse(again.replaced)

    def test_append_after_completion_is_cleaned_by_rerun(self):
        # A record appended after the migration run has fully finished
        # is the remit of another idempotent run -- the migrator cannot
        # wait forever for future writers.
        migrate_log_file(self.path, quiesce=0.02)
        with open(self.path, "ab") as f:
            f.write(v1("later"))
        blob = read_bytes(self.path)
        self.assertIn(1, versions_of(blob))  # mixed until rerun
        result = migrate_log_file(self.path, quiesce=0.02)
        self.assertTrue(result.replaced)
        records = [json.loads(line)
                   for line in read_bytes(self.path).splitlines()]
        self.assertTrue(all(r["v"] == 3 for r in records))
        self.assertTrue(any(r["order_id"] == "later" for r in records))


class TestCheckpointResume(unittest.TestCase):
    """断点续传: a killed run resumes from its checkpoints instead of
    re-scanning or re-writing the completed prefix, and the result is
    byte-identical to a single uninterrupted run."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmp.name, "log.jsonl")
        self.tmp_dir = self.path + ".migrate-tmp"
        self.ckpt = os.path.join(self.tmp_dir, "checkpoint")
        self.original = b"".join(v1(f"O{i}") for i in range(60))
        with open(self.path, "wb") as f:
            f.write(self.original)

    def tearDown(self):
        self.tmp.cleanup()

    def _crash(self, checkpoint, on_bad="strict"):
        return subprocess.run(
            [sys.executable, "-c", CRASH_RUNNER, self.path,
             checkpoint, on_bad, "200"],
            capture_output=True,
        )

    def _reference(self):
        """What one uninterrupted run produces for self.original."""
        ref = os.path.join(self.tmp.name, "ref.jsonl")
        with open(ref, "wb") as f:
            f.write(self.original)
        migrate_log_file(ref, quiesce=0.01)
        return read_bytes(ref)

    def _ckpt_lines(self):
        with open(self.ckpt, "rb") as f:
            return [json.loads(line) for line in f.read().splitlines()]

    def test_resume_after_segment_crash_skips_completed_prefix(self):
        proc = self._crash("segment")
        self.assertEqual(proc.returncode, 1, proc.stderr)
        # Killed before the rename: original byte-for-byte intact.
        self.assertEqual(read_bytes(self.path), self.original)
        committed = self._ckpt_lines()[-1]
        done = committed["migrated"]
        self.assertGreater(done, 0)
        self.assertLess(done, 60)
        # Resume via the Python entry point with a different segment
        # size: only the remaining records are scanned and written.
        result = migrate_log_file(self.path, quiesce=0.01)
        self.assertTrue(result.replaced)
        self.assertEqual(result.records_migrated, 60 - done)
        self.assertEqual(read_bytes(self.path), self._reference())
        self.assertFalse(os.path.exists(self.tmp_dir))

    def test_resume_after_segments_crash_scans_nothing(self):
        proc = self._crash("segments")
        self.assertEqual(proc.returncode, 1, proc.stderr)
        self.assertGreater(len(self._ckpt_lines()), 1)
        # Every record was already committed to segments; the resumed
        # run goes straight to assemble+replace and migrates zero.
        result = migrate_log_file(self.path, quiesce=0.01)
        self.assertTrue(result.replaced)
        self.assertEqual(result.records_migrated, 0)
        self.assertEqual(read_bytes(self.path), self._reference())

    def test_resume_after_assemble_crash_reuses_final(self):
        proc = self._crash("assemble")
        self.assertEqual(proc.returncode, 1, proc.stderr)
        last = self._ckpt_lines()[-1]
        self.assertIn("final", last)
        result = migrate_log_file(self.path, quiesce=0.01)
        self.assertTrue(result.replaced)
        self.assertEqual(result.records_migrated, 0)
        self.assertEqual(read_bytes(self.path), self._reference())

    def test_torn_checkpoint_tail_rolls_back_to_last_complete_line(self):
        proc = self._crash("segments")
        self.assertEqual(proc.returncode, 1, proc.stderr)
        complete = self._ckpt_lines()
        # Simulate a kill in the middle of a checkpoint write.
        with open(self.ckpt, "ab") as f:
            f.write(b'{"v":1,"ino":12')
        result = migrate_log_file(self.path, quiesce=0.01)
        self.assertTrue(result.replaced)
        self.assertEqual(result.records_migrated,
                         60 - complete[-1]["migrated"])
        self.assertEqual(read_bytes(self.path), self._reference())

    def test_corrupt_checkpoint_line_rolls_back(self):
        proc = self._crash("segments")
        self.assertEqual(proc.returncode, 1, proc.stderr)
        lines = self._ckpt_lines()
        self.assertGreater(len(lines), 2)
        # Keep only the first checkpoint, then a corrupt line: the run
        # must resume from the first checkpoint, not start over.
        with open(self.ckpt, "wb") as f:
            f.write(json.dumps(lines[0]).encode() + b"\n")
            f.write(b'{"v":1,"ino":"not-an-inode"}\n')
        result = migrate_log_file(self.path, quiesce=0.01)
        self.assertTrue(result.replaced)
        self.assertEqual(result.records_migrated, 60 - lines[0]["migrated"])
        self.assertEqual(read_bytes(self.path), self._reference())

    def test_unusable_checkpoint_means_fresh_full_scan(self):
        proc = self._crash("segments")
        self.assertEqual(proc.returncode, 1, proc.stderr)
        with open(self.ckpt, "wb") as f:
            f.write(b"total garbage\n")
        result = migrate_log_file(self.path, quiesce=0.01)
        self.assertTrue(result.replaced)
        self.assertEqual(result.records_migrated, 60)
        self.assertEqual(read_bytes(self.path), self._reference())

    def test_missing_segment_rolls_back_to_earlier_checkpoint(self):
        proc = self._crash("segments")
        self.assertEqual(proc.returncode, 1, proc.stderr)
        lines = self._ckpt_lines()
        self.assertGreater(len(lines), 2)
        # Drop a middle segment: every checkpoint that includes it is
        # invalid, so the run rolls back to the last checkpoint whose
        # segments all survive (here: the one covering only seg-000000).
        os.remove(os.path.join(self.tmp_dir, "seg-000001"))
        surviving = {n for n in os.listdir(self.tmp_dir) if n.startswith("seg-")}
        valid = [ln for ln in lines
                 if all(name in surviving for name, _size
                        in ln.get("segments", []))]
        result = migrate_log_file(self.path, quiesce=0.01)
        self.assertTrue(result.replaced)
        self.assertGreater(valid[-1]["migrated"], 0)
        self.assertEqual(result.records_migrated,
                         60 - valid[-1]["migrated"])
        self.assertEqual(read_bytes(self.path), self._reference())

    def test_source_replaced_invalidates_checkpoint(self):
        proc = self._crash("segments")
        self.assertEqual(proc.returncode, 1, proc.stderr)
        # A different file now lives at the path (new inode): the old
        # checkpoints must not be applied to it.
        fresh = b"".join(v1(f"N{i}") for i in range(5))
        staging = self.path + ".staging"
        with open(staging, "wb") as f:
            f.write(fresh)
        os.replace(staging, self.path)
        result = migrate_log_file(self.path, quiesce=0.01)
        self.assertTrue(result.replaced)
        self.assertEqual(result.records_migrated, 5)
        records = [json.loads(line)
                   for line in read_bytes(self.path).splitlines()]
        self.assertEqual([r["order_id"] for r in records],
                         [f"N{i}" for i in range(5)])
        self.assertTrue(all(r["v"] == 3 for r in records))

    def test_torn_final_truncated_back_to_checkpoint(self):
        proc = self._crash("assemble")
        self.assertEqual(proc.returncode, 1, proc.stderr)
        # Killed while draining the pre-rename tail into "final": the
        # file is longer than the fsynced checkpoint size.  The resume
        # truncates the excess and re-derives it from the source.
        final = os.path.join(self.tmp_dir, "final")
        with open(final, "ab") as f:
            f.write(b"HALF-WRITTEN-TAIL")
        result = migrate_log_file(self.path, quiesce=0.01)
        self.assertTrue(result.replaced)
        self.assertEqual(read_bytes(self.path), self._reference())

    def test_idempotent_rerun_after_resume(self):
        proc = self._crash("segments")
        self.assertEqual(proc.returncode, 1, proc.stderr)
        migrate_log_file(self.path, quiesce=0.01)
        once = read_bytes(self.path)
        st = os.stat(self.path)
        time.sleep(0.02)
        again = migrate_log_file(self.path, quiesce=0.01)
        self.assertFalse(again.replaced)
        self.assertEqual(read_bytes(self.path), once)
        self.assertEqual(os.stat(self.path).st_mtime_ns, st.st_mtime_ns)
        self.assertEqual(os.stat(self.path).st_ino, st.st_ino)


class TestRealKillRecovery(unittest.TestCase):
    """崩溃注入后真杀恢复: SIGKILL a live CLI migration mid-scan, then
    rerun to completion; the recovered result matches a single run."""

    RECORDS = 50_000

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmp.name, "log.jsonl")
        self.ckpt = os.path.join(self.path + ".migrate-tmp", "checkpoint")
        self.original = b"".join(v1(f"K{i}") for i in range(self.RECORDS))
        with open(self.path, "wb") as f:
            f.write(self.original)

    def tearDown(self):
        self.tmp.cleanup()

    def _wait_for_committed_segments(self, proc, minimum):
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            self.assertIsNone(proc.poll(), "migration finished before kill")
            try:
                with open(self.ckpt, "rb") as f:
                    lines = [ln for ln in f.read().splitlines() if ln]
            except OSError:
                lines = []
            if len(lines) >= minimum:
                return lines
            time.sleep(0.005)
        self.fail("checkpoint never reached %d committed lines" % minimum)

    def test_sigkill_mid_scan_then_rerun(self):
        proc = subprocess.Popen(
            [sys.executable, "-m", "proto_migrate", "migrate-log",
             self.path, "--segment-size", "32768", "--quiesce-ms", "20"],
            cwd=REPO_ROOT, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
        lines = self._wait_for_committed_segments(proc, 2)
        done = json.loads(lines[-1])["migrated"]
        self.assertGreater(done, 0)
        self.assertLess(done, self.RECORDS)
        proc.kill()  # SIGKILL: no cleanup, no finally blocks
        proc.wait()
        self.assertNotEqual(proc.returncode, 0)
        # The kill left the target byte-for-byte untouched.
        self.assertEqual(read_bytes(self.path), self.original)

        rerun = subprocess.run(
            [sys.executable, "-m", "proto_migrate", "migrate-log",
             self.path, "--segment-size", "32768", "--quiesce-ms", "20"],
            cwd=REPO_ROOT, capture_output=True, text=True,
        )
        self.assertEqual(rerun.returncode, EXIT_OK, rerun.stderr)
        self.assertIn("replaced=yes", rerun.stdout)
        migrated_now = int(
            rerun.stdout.split("migrated=")[1].split()[0]
        )
        # The resumed run only scanned the unfinished tail.
        self.assertEqual(migrated_now, self.RECORDS - done)

        # Byte-identical to one uninterrupted run.
        ref = os.path.join(self.tmp.name, "ref.jsonl")
        with open(ref, "wb") as f:
            f.write(self.original)
        migrate_log_file(ref, quiesce=0.01)
        self.assertEqual(read_bytes(self.path), read_bytes(ref))
        self.assertFalse(
            os.path.exists(self.path + ".migrate-tmp")
        )

        # Idempotent afterwards.
        third = subprocess.run(
            [sys.executable, "-m", "proto_migrate", "migrate-log",
             self.path],
            cwd=REPO_ROOT, capture_output=True, text=True,
        )
        self.assertEqual(third.returncode, EXIT_OK, third.stderr)
        self.assertIn("replaced=no", third.stdout)


class TestThreePartyConcurrentResume(unittest.TestCase):
    """三方并发 + 断点续传: migrator (CLI subprocess, killed and
    resumed), a live appender, and concurrent readers, all at once."""

    BASE = 20_000
    APPENDED = 30

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmp.name, "log.jsonl")
        self.lock = self.path + ".migrate.lock"
        self.ckpt = os.path.join(self.path + ".migrate-tmp", "checkpoint")
        with open(self.path, "wb") as f:
            for i in range(self.BASE):
                f.write(v1(f"base-{i}"))

    def tearDown(self):
        self.tmp.cleanup()

    def test_kill_and_resume_while_appending_and_reading(self):
        stop = threading.Event()
        snapshots = []
        errors = []

        def reader():
            try:
                while not stop.is_set():
                    with open(self.path, "rb") as f:
                        blob = f.read()
                    versions = versions_lenient(blob)
                    if versions:
                        snapshots.append((frozenset(versions), len(versions)))
                    time.sleep(0.001)
            except OSError as exc:  # pragma: no cover
                errors.append(exc)

        def appender():
            for _ in range(2000):
                if os.path.exists(self.lock):
                    break
                time.sleep(0.001)
            for i in range(self.APPENDED):
                with open(self.path, "ab") as f:
                    f.write(v2(f"late-{i}"))
                time.sleep(0.002)

        r = threading.Thread(target=reader)
        t = threading.Thread(target=appender)
        r.start()
        t.start()

        # First migration attempt: killed for real partway through.
        proc = subprocess.Popen(
            [sys.executable, "-m", "proto_migrate", "migrate-log",
             self.path, "--segment-size", "16384", "--quiesce-ms", "20"],
            cwd=REPO_ROOT, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            self.assertIsNone(proc.poll(), "migration finished before kill")
            if os.path.exists(self.ckpt) and os.path.getsize(self.ckpt) > 0:
                break
            time.sleep(0.005)
        else:
            self.fail("migration never committed a checkpoint")
        proc.kill()
        proc.wait()

        # Resume while the appender and reader are still going.
        rerun = subprocess.run(
            [sys.executable, "-m", "proto_migrate", "migrate-log",
             self.path, "--segment-size", "16384", "--quiesce-ms", "20"],
            cwd=REPO_ROOT, capture_output=True, text=True,
        )
        self.assertEqual(rerun.returncode, EXIT_OK, rerun.stderr)
        t.join()
        stop.set()
        r.join()

        self.assertEqual(errors, [])
        self.assertTrue(snapshots)
        for present, _count in snapshots:
            if 3 in present:
                self.assertEqual(present, {3},
                                 f"mixed view: {sorted(present)}")

        blob = read_bytes(self.path)
        records = [json.loads(line) for line in blob.splitlines()]
        # Zero loss, zero duplication.
        self.assertEqual(len(records), self.BASE + self.APPENDED)
        ids = [rec["order_id"] for rec in records]
        self.assertEqual(len(set(ids)), len(ids))
        self.assertTrue(all(rec["v"] == CURRENT_VERSION for rec in records))
        # Original append order is preserved within each stream.
        base_seq = [i for i in ids if i.startswith("base-")]
        late_seq = [i for i in ids if i.startswith("late-")]
        self.assertEqual(base_seq, [f"base-{i}" for i in range(self.BASE)])
        self.assertEqual(late_seq, [f"late-{i}" for i in range(self.APPENDED)])

        # Idempotent once everything settled.
        again = migrate_log_file(self.path, quiesce=0.02)
        self.assertFalse(again.replaced)
        self.assertEqual(read_bytes(self.path), blob)


class TestPostCommitFailure(unittest.TestCase):
    """错误分类: durability/cleanup failures after the atomic rename
    must not turn a committed migration into a reported failure."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmp.name, "log.jsonl")
        with open(self.path, "wb") as f:
            f.write(v1("A") + v2("B"))

    def tearDown(self):
        self.tmp.cleanup()

    def test_dir_fsync_failure_after_commit_is_warning_not_failure(self):
        parent = os.path.dirname(os.path.abspath(self.path))
        real_fsync_dir = log_migration._fsync_dir

        def flaky_fsync_dir(dirpath):
            if os.path.abspath(dirpath) == parent:
                raise OSError("simulated post-commit fsync failure")
            return real_fsync_dir(dirpath)

        with mock.patch.object(
            log_migration, "_fsync_dir", flaky_fsync_dir
        ):
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                result = migrate_log_file(self.path, quiesce=0.01)
        self.assertTrue(result.replaced)
        self.assertTrue(
            any("fsync" in str(w.message) for w in caught),
            [str(w.message) for w in caught],
        )
        # The migration itself committed: fully migrated output.
        self.assertEqual(versions_of(read_bytes(self.path)), [3, 3])
        # And the next run is a clean idempotent no-op.
        again = migrate_log_file(self.path, quiesce=0.01)
        self.assertFalse(again.replaced)


class TestCommandLine(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmp.name, "log.jsonl")
        with open(self.path, "wb") as f:
            f.write(v1("A") + v2("B"))

    def tearDown(self):
        self.tmp.cleanup()

    def _run(self, *args):
        return subprocess.run(
            [sys.executable, "-m", "proto_migrate", *args],
            capture_output=True, text=True, cwd=REPO_ROOT,
        )

    def test_migrate_log_success(self):
        proc = self._run("migrate-log", self.path)
        self.assertEqual(proc.returncode, EXIT_OK, proc.stderr)
        self.assertIn("replaced=yes", proc.stdout)
        self.assertTrue(all(v == 3 for v in
                            versions_of(read_bytes(self.path))))
        proc2 = self._run("migrate-log", self.path)
        self.assertEqual(proc2.returncode, EXIT_OK)
        self.assertIn("replaced=no", proc2.stdout)

    def test_strict_exit_code(self):
        with open(self.path, "ab") as f:
            f.write(b"broken\n")
        proc = self._run("migrate-log", self.path)
        self.assertEqual(proc.returncode, EXIT_BAD_RECORD)
        self.assertIn("line 3", proc.stderr)

    def test_skip_audit_to_stderr(self):
        with open(self.path, "ab") as f:
            f.write(b"brokenline\n")
        proc = self._run("migrate-log", self.path, "--skip")
        self.assertEqual(proc.returncode, EXIT_OK)
        self.assertIn("3:brokenline", proc.stderr)

    def test_usage_error(self):
        proc = self._run("migrate-log")
        self.assertEqual(proc.returncode, EXIT_USAGE)

    def test_selftest_unchanged(self):
        proc = self._run("--selftest")
        self.assertEqual(proc.returncode, 0)
        self.assertEqual(proc.stdout, "ok\n")

    def test_module_exports_entry_point(self):
        self.assertIs(proto_migrate.migrate_log_file, migrate_log_file)


if __name__ == "__main__":
    unittest.main()
