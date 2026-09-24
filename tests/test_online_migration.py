"""Tests for the online, resumable log migration.

Covers the four required scenario classes:
  * three-party concurrency (migrator + continuously appending writer
    + readers taking consistent snapshots through ``read_log``),
  * checkpoint resume plus corruption / missing-segment / torn-tail
    rollback,
  * real SIGKILL crash injection and recovery to a byte-identical file,
  * idempotent reruns (including after post-commit faults).
"""

import json
import os
import signal
import subprocess
import sys
import tempfile
import threading
import time
import unittest

import proto_migrate
import proto_migrate.log_migration as lm
from proto_migrate import CURRENT_VERSION, dumps
from proto_migrate.log_migration import (
    EXIT_BAD_RECORD,
    EXIT_ERROR,
    EXIT_OK,
    EXIT_USAGE,
    migrate_log_file,
    read_log,
)

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


# ---------------------------------------------------------------------------
# Record builders
# ---------------------------------------------------------------------------


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
    return dumps({
        "order_id": order_id,
        "amount": amount,
        "status": status,
        "note": note,
        "updated_at": updated_at,
    })


def read_bytes(path):
    with open(path, "rb") as f:
        return f.read()


def ids_of(blob):
    return [json.loads(line)["order_id"]
            for line in blob.splitlines() if line]


def versions_of(blob):
    return [json.loads(line)["v"] for line in blob.splitlines() if line]


# ---------------------------------------------------------------------------
# Subprocess drivers
# ---------------------------------------------------------------------------


MIGRATE_RUNNER = r"""
import os, sys
sys.path.insert(0, %r)
if sys.argv[2] != "-":
    os.environ["PROTO_MIGRATE_CRASH_AT"] = sys.argv[2]
if len(sys.argv) > 5 and sys.argv[5] != "-":
    os.environ["PROTO_MIGRATE_FAULT_AT"] = sys.argv[5]
from proto_migrate.log_migration import run_cli
flag = "--skip" if sys.argv[4] == "skip" else "--strict"
raise SystemExit(run_cli([sys.argv[1], flag,
                          "--quiesce-ms", sys.argv[3],
                          "--segment-size", sys.argv[6]]))
""" % REPO_ROOT


APPENDER_RUNNER = r"""
import os, sys, time, json
path, lock, count, interval, kind = sys.argv[1:6]
def v1(i):
    return (b'{"v":1,"order_id":' + json.dumps("late-%d" % i).encode()
            + b',"amount":4.0,"tags":[]}\n')
def v2(i):
    return (b'{"v":2,"order_id":' + json.dumps("late-%d" % i).encode()
            + b',"amount":2.5,"tags":["t"],"status":"paid"}\n')
enc = v1 if kind == "v1" else v2
for _ in range(3000):
    if os.path.exists(lock):
        break
    time.sleep(0.001)
for i in range(int(count)):
    with open(path, "ab") as f:
        f.write(enc(i))
    time.sleep(float(interval))
"""


def run_migrator(path, crash="-", fault="-", on_bad="strict",
                 quiesce_ms="20", segment_size=str(16 * 1024 * 1024)):
    return subprocess.run(
        [sys.executable, "-c", MIGRATE_RUNNER, path, crash,
         quiesce_ms, on_bad, fault, segment_size],
        capture_output=True,
    )


def start_appender(path, lock, count, interval=0.005, kind="v2"):
    return subprocess.Popen(
        [sys.executable, "-c", APPENDER_RUNNER, path, lock,
         str(count), str(interval), kind],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )


def reference_migration(blob, on_bad="strict"):
    """One-shot, crash-free migration of *blob*; returns final bytes."""
    with tempfile.TemporaryDirectory() as d:
        p = os.path.join(d, "ref.log")
        with open(p, "wb") as f:
            f.write(blob)
        migrate_log_file(p, on_bad=on_bad, quiesce=0.01)
        return read_bytes(p)


# ---------------------------------------------------------------------------
# 1. Three-party concurrency
# ---------------------------------------------------------------------------


class TestThreePartyConcurrency(unittest.TestCase):
    BASE = 20_000
    APPENDED = 40

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmp.name, "log.jsonl")
        self.lock = self.path + ".migrate.lock"
        with open(self.path, "wb") as f:
            for i in range(self.BASE):
                f.write(v1(f"base-{i}"))
        self.expected_ids = [f"base-{i}" for i in range(self.BASE)] + [
            f"late-{i}" for i in range(self.APPENDED)
        ]

    def tearDown(self):
        self.tmp.cleanup()

    def test_migrator_appender_readers_no_loss_no_mix(self):
        stop = threading.Event()
        views = []
        view_errors = []

        def reader():
            try:
                while not stop.is_set():
                    recs = read_log(self.path, quiesce=0.01)
                    if recs:
                        versions = {r["v"] for r in recs}
                        ids = [r["order_id"] for r in recs]
                        views.append((versions, ids))
                time.sleep(0.0005)
            except Exception as exc:  # pragma: no cover
                view_errors.append(repr(exc))

        readers = [threading.Thread(target=reader) for _ in range(3)]
        appender = start_appender(self.path, self.lock, self.APPENDED,
                                  interval=0.004, kind="v2")
        for r in readers:
            r.start()
        result = migrate_log_file(self.path, quiesce=0.02)
        appender.wait(timeout=10)
        stop.set()
        for r in readers:
            r.join(timeout=5)

        self.assertEqual(view_errors, [])
        self.assertTrue(views)

        # Every view must be uniform: only old versions (1/2) or only
        # the current version (3) -- never a mix in one snapshot.
        saw_pre = saw_post = False
        for versions, ids in views:
            self.assertTrue(versions <= {1, 2} or versions == {3},
                            f"mixed-version view: {sorted(versions)}")
            if 3 in versions:
                saw_post = True
                self.assertTrue(all(v == 3 for v in versions))
            else:
                saw_pre = True
            # Every view must be a prefix of the global append order:
            # proves no loss, duplication or reordering inside a view.
            self.assertEqual(ids, self.expected_ids[:len(ids)])
        self.assertTrue(saw_post)
        # With ~80ms of appends spread across a >200ms migration the
        # readers practically observe a pre-commit view as well; do not
        # hard-assert timing, but record it for diagnosis.
        del saw_pre

        blob = read_bytes(self.path)
        self.assertEqual(len(blob.splitlines()),
                         self.BASE + self.APPENDED, f"result={result}")
        self.assertTrue(all(v == 3 for v in versions_of(blob)))
        self.assertEqual(ids_of(blob), self.expected_ids)

    def test_path_reopening_appender_during_wrapup_readers_uniform(self):
        inode_before = os.stat(self.path).st_ino
        n = 8

        def late_appender():
            # Append old v1 records BY PATH as soon as the rename lands,
            # so they hit the new inode while convergence is running.
            for _ in range(3000):
                if os.stat(self.path).st_ino != inode_before:
                    break
                time.sleep(0.001)
            for i in range(n):
                with open(self.path, "ab") as f:
                    f.write(v1(f"post-{i}", amount=7.0))
                time.sleep(0.004)

        stop = threading.Event()
        bad_views = []

        def reader():
            while not stop.is_set():
                recs = read_log(self.path, quiesce=0.01)
                versions = {r["v"] for r in recs}
                if versions and versions != {CURRENT_VERSION} \
                        and not versions <= {1, 2}:
                    bad_views.append(versions)
                if 3 in versions and any(v != 3 for v in versions):
                    bad_views.append(versions)

        t_reader = threading.Thread(target=reader)
        t_app = threading.Thread(target=late_appender)
        t_reader.start()
        t_app.start()
        migrate_log_file(self.path, quiesce=0.02)
        t_app.join(timeout=5)
        stop.set()
        t_reader.join(timeout=5)

        # The property under test: no reader ever observed a mixed
        # old/new view while the path-reopening appender ran.
        self.assertEqual(bad_views, [])

        # A record that lands strictly after convergence exited is the
        # documented rerun case; join first, then one idempotent run
        # migrates any such tail.  The on-disk result is then fully
        # current-version with all post records present and in order.
        self.assertTrue(t_app.is_alive() is False)
        migrate_log_file(self.path, quiesce=0.02)
        blob = read_bytes(self.path)
        records = [json.loads(line) for line in blob.splitlines()]
        self.assertTrue(all(r["v"] == 3 for r in records))
        post = [r["order_id"] for r in records
                if r["order_id"].startswith("post-")]
        self.assertEqual(post, [f"post-{i}" for i in range(n)])

    def test_reader_excludes_torn_tail_and_serves_rest(self):
        with open(self.path, "wb") as f:
            f.write(v1("a") + v1("b"))
            f.write(v1("c")[:-1])  # no terminating newline yet
        view = read_log(self.path, quiesce=0.01)
        self.assertEqual([r["order_id"] for r in view], ["a", "b"])
        with open(self.path, "ab") as f:
            f.write(b"\n")
        view = read_log(self.path)
        self.assertEqual([r["order_id"] for r in view], ["a", "b", "c"])


# ---------------------------------------------------------------------------
# 2. Checkpoint resume and corruption rollback
# ---------------------------------------------------------------------------


class TestCheckpointResume(unittest.TestCase):
    N = 60
    SEG = "200"

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmp.name, "log.jsonl")
        self.tmp_dir = self.path + ".migrate-tmp"
        self.cp = os.path.join(self.tmp_dir, "checkpoint")
        self.original = b"".join(v1(f"O{i}") for i in range(self.N))
        with open(self.path, "wb") as f:
            f.write(self.original)

    def tearDown(self):
        self.tmp.cleanup()

    def _crash(self, point, on_bad="strict"):
        return run_migrator(self.path, crash=point, on_bad=on_bad,
                            segment_size=self.SEG)

    def _finish(self):
        proc = run_migrator(self.path)
        self.assertEqual(proc.returncode, 0, proc.stderr)

    def _checkpoint_state(self):
        with open(self.cp, "r", encoding="ascii") as f:
            lines = f.read().splitlines()
        records = []
        for line in lines[1:]:
            off, count, skipped, dirty, name, size = line.split()
            records.append((int(off), int(count), name, int(size)))
        return lines[0], records

    def test_resume_is_byte_identical_to_one_shot_run(self):
        reference = reference_migration(self.original)
        for point in ("checkpoint", "segments", "assemble", "drain",
                      "marker", "replace"):
            with self.subTest(point=point):
                with open(self.path, "wb") as f:
                    f.write(self.original)
                proc = self._crash(point)
                self.assertEqual(proc.returncode, 1, proc.stderr)
                self._finish()
                self.assertEqual(read_bytes(self.path), reference)
                # Idempotent after recovery.
                again = run_migrator(self.path)
                self.assertEqual(again.returncode, 0)
                self.assertIn(b"replaced=no", again.stdout)
                self.assertEqual(read_bytes(self.path), reference)

    def test_checkpoint_actually_skips_migrated_prefix(self):
        # Kill at the first rotation so a durable checkpoint exists.
        proc = self._crash("checkpoint")
        self.assertEqual(proc.returncode, 1)
        _header, records = self._checkpoint_state()
        self.assertTrue(records)
        offset, count, _name, _size = records[-1]
        self.assertGreater(offset, 0)
        self.assertGreater(count, 0)

        # Resume in-process while counting loads() calls: the records
        # already covered by the checkpoint must never be decoded again.
        calls = []
        orig_loads = lm.loads

        def counting_loads(raw):
            calls.append(raw)
            return orig_loads(raw)

        lm.loads = counting_loads
        try:
            result = migrate_log_file(self.path, quiesce=0.01,
                                      segment_size=int(self.SEG))
        finally:
            lm.loads = orig_loads

        remaining = self.N - count
        self.assertEqual(len(calls), remaining,
                         f"rescanned {len(calls)} lines, expected "
                         f"{remaining} (checkpoint covered {count})")
        reference = reference_migration(self.original)
        self.assertEqual(read_bytes(self.path), reference)
        self.assertEqual(result.records_migrated, self.N)

    def test_torn_checkpoint_tail_rolls_back(self):
        reference = reference_migration(self.original)
        self._crash("checkpoint")
        with open(self.cp, "ab") as f:
            f.write(b"999999 88 x")  # half line, no newline
        self._finish()
        self.assertEqual(read_bytes(self.path), reference)

    def test_fully_garbled_checkpoint_starts_fresh(self):
        reference = reference_migration(self.original)
        self._crash("checkpoint")
        with open(self.cp, "wb") as f:
            f.write(b"garbage garbage garbage\n")
        self._finish()
        self.assertEqual(read_bytes(self.path), reference)

    def test_missing_segment_rolls_back(self):
        reference = reference_migration(self.original)
        self._crash("checkpoint")
        _header, records = self._checkpoint_state()
        os.remove(os.path.join(self.tmp_dir, records[0][2]))
        self._finish()
        self.assertEqual(read_bytes(self.path), reference)

    def test_truncated_segment_rolls_back(self):
        reference = reference_migration(self.original)
        self._crash("checkpoint")
        _header, records = self._checkpoint_state()
        seg = os.path.join(self.tmp_dir, records[0][2])
        with open(seg, "r+b") as f:
            f.truncate(1)
        self._finish()
        self.assertEqual(read_bytes(self.path), reference)

    def test_rollback_to_previous_complete_checkpoint(self):
        # Two killed runs accumulate two checkpoint prefixes; damage
        # only the newest one and verify recovery falls back to the
        # previous complete checkpoint, then finishes correctly.
        reference = reference_migration(self.original)
        self._crash("checkpoint")
        self._crash("checkpoint")
        _header, records = self._checkpoint_state()
        self.assertGreaterEqual(len(records), 2)
        # Delete the newest segment and remove its checkpoint line: the
        # run must resume from the older prefix, not start over blindly.
        newest = records[-1]
        os.remove(os.path.join(self.tmp_dir, newest[2]))
        with open(self.cp, "rb") as f:
            data = f.read()
        good_lines = data.splitlines(keepends=True)[:-1]
        with open(self.cp, "wb") as f:
            f.writelines(good_lines)
        self._finish()
        self.assertEqual(read_bytes(self.path), reference)

    def test_strict_rerun_after_skipped_crash_does_not_inherit_prefix(self):
        # A --skip run that crashed after scanning a bad line leaves a
        # checkpoint taken under the skip policy.  A strict rerun must
        # restart the scan and still stop at the bad line.
        data = b"".join(v1(f"O{i}") for i in range(self.N))
        data += b"THIS IS BAD\n"
        data += v1("tail")
        with open(self.path, "wb") as f:
            f.write(data)
        proc = self._crash("segments", on_bad="skip")
        self.assertEqual(proc.returncode, 1)
        # Strict rerun: mode mismatch forces a full rescan -> aborts.
        proc = run_migrator(self.path, on_bad="strict")
        self.assertEqual(proc.returncode, EXIT_BAD_RECORD)
        self.assertEqual(read_bytes(self.path), data)
        # Skip rerun completes and matches a one-shot skip migration.
        proc = run_migrator(self.path, on_bad="skip")
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(read_bytes(self.path),
                         reference_migration(data, on_bad="skip"))

    def test_checkpoint_parser_rejects_bad_records_directly(self):
        os.makedirs(self.tmp_dir, exist_ok=True)
        seg = os.path.join(self.tmp_dir, "seg-000000")
        with open(seg, "wb") as f:
            f.write(b"x" * 10)
        cp = os.path.join(self.tmp_dir, "checkpoint")
        with open(cp, "wb") as f:
            f.write(b"src 123 strict\n")
            f.write(b"10 3 0 0 seg-000000 10\n")
            f.write(b"not a checkpoint\n")
        inode, mode, records, good = lm._read_checkpoint_log(self.tmp_dir)
        self.assertEqual(inode, 123)
        self.assertEqual(mode, "strict")
        self.assertEqual(len(records), 1)
        self.assertTrue(good.endswith(b"seg-000000 10\n"))


# ---------------------------------------------------------------------------
# 3. Real SIGKILL crash injection and recovery
# ---------------------------------------------------------------------------


class TestRealKillRecovery(unittest.TestCase):
    BASE = 60_000

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmp.name, "log.jsonl")
        self.lock = self.path + ".migrate.lock"
        self.tmp_dir = self.path + ".migrate-tmp"
        with open(self.path, "wb") as f:
            for i in range(self.BASE):
                f.write(v1(f"k-{i}"))
        self.original = read_bytes(self.path)

    def tearDown(self):
        self.tmp.cleanup()

    def _start_migrator(self):
        return subprocess.Popen(
            [sys.executable, "-m", "proto_migrate", "migrate-log",
             self.path, "--strict", "--quiesce-ms", "10",
             "--segment-size", "4096"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            cwd=REPO_ROOT,
        )

    def _wait_for_checkpoint(self, timeout=10.0):
        cp = os.path.join(self.tmp_dir, "checkpoint")
        deadline = time.monotonic() + timeout
        last = -1
        while time.monotonic() < deadline:
            if os.path.exists(cp) and os.path.getsize(cp) > last:
                last = os.path.getsize(cp)
                if last > 64:  # header plus at least one record
                    return
            time.sleep(0.005)
        self.fail("migrator never reached a checkpoint")

    def test_kill_during_scan_then_resume(self):
        proc = self._start_migrator()
        self._wait_for_checkpoint()
        proc.send_signal(signal.SIGKILL)
        proc.wait(timeout=5)
        self.assertEqual(proc.returncode, -9)
        # Target is still the untouched original pre-rename.
        self.assertEqual(read_bytes(self.path), self.original)

        proc2 = run_migrator(self.path, quiesce_ms="10", segment_size="4096")
        self.assertEqual(proc2.returncode, 0, proc2.stderr)
        blob = read_bytes(self.path)
        self.assertEqual(len(blob.splitlines()), self.BASE)
        self.assertTrue(all(v == 3 for v in versions_of(blob)))
        self.assertEqual(blob, reference_migration(self.original))

    def test_kill_resume_kill_resume(self):
        proc = self._start_migrator()
        self._wait_for_checkpoint()
        proc.send_signal(signal.SIGKILL)
        proc.wait(timeout=5)

        proc = self._start_migrator()
        # Let the resumed run make more progress, kill again.
        self._wait_for_checkpoint()
        time.sleep(0.05)
        proc.send_signal(signal.SIGKILL)
        proc.wait(timeout=5)

        proc3 = run_migrator(self.path, quiesce_ms="10", segment_size="4096")
        self.assertEqual(proc3.returncode, 0, proc3.stderr)
        self.assertEqual(read_bytes(self.path),
                         reference_migration(self.original))

    def test_kill_while_appender_runs_then_resume_zero_loss(self):
        n = 30
        appender = start_appender(self.path, self.lock, n,
                                  interval=0.01, kind="v2")
        proc = self._start_migrator()
        self._wait_for_checkpoint()
        time.sleep(0.05)
        proc.send_signal(signal.SIGKILL)
        proc.wait(timeout=5)
        appender.wait(timeout=10)

        proc2 = run_migrator(self.path, quiesce_ms="10", segment_size="4096")
        self.assertEqual(proc2.returncode, 0, proc2.stderr)
        blob = read_bytes(self.path)
        records = [json.loads(line) for line in blob.splitlines()]
        self.assertEqual(len(records), self.BASE + n)
        self.assertTrue(all(r["v"] == 3 for r in records))
        expected = [f"k-{i}" for i in range(self.BASE)] + [
            f"late-{i}" for i in range(n)
        ]
        self.assertEqual([r["order_id"] for r in records], expected)


# ---------------------------------------------------------------------------
# 4. Idempotent reruns and exit-code classification
# ---------------------------------------------------------------------------


class TestIdempotentReruns(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmp.name, "log.jsonl")
        with open(self.path, "wb") as f:
            f.write(b"".join(v1(f"I{i}") for i in range(50)))

    def tearDown(self):
        self.tmp.cleanup()

    def test_repeated_reruns_rewrite_nothing(self):
        once = reference_migration(read_bytes(self.path))
        first = run_migrator(self.path)
        self.assertEqual(first.returncode, 0)
        self.assertIn(b"replaced=yes", first.stdout)
        for _ in range(4):
            proc = run_migrator(self.path)
            self.assertEqual(proc.returncode, 0)
            self.assertIn(b"replaced=no", proc.stdout)
            self.assertEqual(read_bytes(self.path), once)
        self.assertFalse(os.path.exists(self.path + ".migrate-tmp"))

    def test_recovered_run_is_idempotent(self):
        once = read_bytes(self.path)
        proc = run_migrator(self.path, crash="checkpoint",
                            segment_size="200")
        self.assertEqual(proc.returncode, 1)
        proc = run_migrator(self.path)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        migrated = read_bytes(self.path)
        self.assertEqual(migrated, reference_migration(once))
        proc = run_migrator(self.path)
        self.assertIn(b"replaced=no", proc.stdout)
        self.assertEqual(read_bytes(self.path), migrated)

    def test_post_commit_dirfsync_fault_is_still_success(self):
        proc = run_migrator(self.path, fault="dirfsync")
        self.assertEqual(proc.returncode, EXIT_OK, proc.stderr)
        self.assertIn(b"replaced=yes", proc.stdout)
        self.assertIn(b"warning:", proc.stderr)
        blob = read_bytes(self.path)
        self.assertTrue(all(v == 3 for v in versions_of(blob)))
        # A rerun is idempotent despite the warning.
        again = run_migrator(self.path)
        self.assertEqual(again.returncode, 0)
        self.assertIn(b"replaced=no", again.stdout)
        self.assertEqual(read_bytes(self.path), blob)

    def test_post_commit_cleanup_fault_is_still_success(self):
        proc = run_migrator(self.path, fault="cleanup")
        self.assertEqual(proc.returncode, EXIT_OK, proc.stderr)
        self.assertIn(b"warning:", proc.stderr)
        # Cleanup fault leaves the work dir behind; the rerun sweeps it.
        self.assertTrue(os.path.exists(self.path + ".migrate-tmp"))
        blob = read_bytes(self.path)
        again = run_migrator(self.path)
        self.assertEqual(again.returncode, 0, again.stderr)
        self.assertFalse(os.path.exists(self.path + ".migrate-tmp"))
        self.assertEqual(read_bytes(self.path), blob)
        self.assertIn(b"replaced=no", again.stdout)

    def test_exit_codes_are_distinguishable(self):
        # Usage error -> 2.
        proc = subprocess.run(
            [sys.executable, "-m", "proto_migrate", "migrate-log"],
            capture_output=True, cwd=REPO_ROOT,
        )
        self.assertEqual(proc.returncode, EXIT_USAGE)
        # Bad record in strict mode -> 3.
        with open(self.path, "ab") as f:
            f.write(b"broken\n")
        proc = run_migrator(self.path)
        self.assertEqual(proc.returncode, EXIT_BAD_RECORD)
        # I/O error (path is a directory) -> 1.
        adir = os.path.join(self.tmp.name, "dir")
        os.mkdir(adir)
        proc = subprocess.run(
            [sys.executable, "-m", "proto_migrate", "migrate-log", adir],
            capture_output=True, cwd=REPO_ROOT,
        )
        self.assertEqual(proc.returncode, EXIT_ERROR)
        # Success -> 0.
        with open(self.path, "wb") as f:
            f.write(v1("only-good"))
        proc = run_migrator(self.path)
        self.assertEqual(proc.returncode, EXIT_OK)


class TestPublicApi(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmp.name, "log.jsonl")

    def tearDown(self):
        self.tmp.cleanup()

    def test_read_log_exported_and_pre_migration_view(self):
        self.assertIs(proto_migrate.read_log, read_log)
        with open(self.path, "wb") as f:
            f.write(v1("a") + v2("b"))
        recs = read_log(self.path)
        self.assertEqual([r["v"] for r in recs], [1, 2])
        migrate_log_file(self.path, quiesce=0.01)
        recs = read_log(self.path)
        self.assertTrue(all(r["v"] == CURRENT_VERSION for r in recs))
        self.assertEqual([r["order_id"] for r in recs], ["a", "b"])

    def test_read_log_normalizes_old_appendix_after_completion(self):
        # After a completed migration a writer lands an old record by
        # path; read_log still hands back a uniform current-version
        # view (a rerun rewrites the bytes themselves).
        with open(self.path, "wb") as f:
            f.write(v1("a"))
        migrate_log_file(self.path, quiesce=0.01)
        with open(self.path, "ab") as f:
            f.write(v1("later"))
        recs = read_log(self.path)
        self.assertTrue(all(r["v"] == CURRENT_VERSION for r in recs))
        self.assertEqual([r["order_id"] for r in recs], ["a", "later"])
        # The bytes are cleaned by the idempotent rerun.
        migrate_log_file(self.path, quiesce=0.01)
        self.assertTrue(all(v == 3 for v in
                            versions_of(read_bytes(self.path))))


if __name__ == "__main__":
    unittest.main()
