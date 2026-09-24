"""Tests for the group-level, all-or-nothing log migration.

Covers:
  * all-or-nothing commit (any pre-commit failure restores every member),
  * strict / skip bad-record handling and the
    ``<file>:<lineno>:<first 32 bytes>`` audit format,
  * member-list validation and every CLI exit code,
  * ``read_log_group`` snapshots under concurrent appenders,
  * real ``os._exit`` crash injection at every group protocol point with
    byte-identical recovery and no rescanning of prepared members,
  * counters that count only newly migrated records, post-border
    fault-as-warning, numeric edge cases and idempotent reruns.
"""

import io
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
from proto_migrate.group_migration import (
    GroupBadRecordError,
    migrate_log_group,
    read_log_group,
    run_group_cli,
)
from proto_migrate.log_migration import (
    EXIT_BAD_RECORD,
    EXIT_ERROR,
    EXIT_OK,
    EXIT_USAGE,
)

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


# ---------------------------------------------------------------------------
# Record builders / helpers
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


def versions_of(blob):
    return [json.loads(line)["v"] for line in blob.splitlines() if line]


def reference_group(originals, on_bad="strict"):
    """Crash-free reference migration of a set of member blobs."""
    with tempfile.TemporaryDirectory() as d:
        paths = []
        for i, blob in enumerate(originals):
            p = os.path.join(d, f"m{i}.log")
            with open(p, "wb") as f:
                f.write(blob)
            paths.append(p)
        migrate_log_group(paths, on_bad=on_bad, quiesce=0.01)
        return [read_bytes(p) for p in paths]


# ---------------------------------------------------------------------------
# Subprocess drivers
# ---------------------------------------------------------------------------


GROUP_RUNNER = r"""
import os, sys
sys.path.insert(0, %r)
if sys.argv[1] != "-":
    os.environ["PROTO_MIGRATE_CRASH_AT"] = sys.argv[1]
if len(sys.argv) > 2 and sys.argv[2] != "-":
    os.environ["PROTO_MIGRATE_FAULT_AT"] = sys.argv[2]
from proto_migrate.group_migration import run_group_cli
rest = [a for a in sys.argv[3:] if not a.startswith("@@")]
raise SystemExit(run_group_cli(rest))
""" % REPO_ROOT


def run_group(paths, crash="-", fault="-", on_bad="strict",
              quiesce_ms="10", segment_size=str(16 * 1024 * 1024)):
    argv = list(paths) + [
        f"--quiesce-ms={quiesce_ms}",
        f"--segment-size={segment_size}",
        "--skip" if on_bad == "skip" else "--strict",
    ]
    return subprocess.run(
        [sys.executable, "-c", GROUP_RUNNER, crash, fault, *argv],
        capture_output=True,
    )


APPENDER_RUNNER = r"""
import os, sys, time, json
path, count, interval, kind = sys.argv[1:5]
def v1(i):
    return (b'{"v":1,"order_id":' + json.dumps("late-%d" % i).encode()
            + b',"amount":4.0,"tags":[]}\n')
def v2(i):
    return (b'{"v":2,"order_id":' + json.dumps("late-%d" % i).encode()
            + b',"amount":2.5,"tags":["t"],"status":"paid"}\n')
enc = v1 if kind == "v1" else v2
for i in range(int(count)):
    with open(path, "ab") as f:
        f.write(enc(i))
    time.sleep(float(interval))
"""


def start_appender(path, count, interval=0.004, kind="v2"):
    return subprocess.Popen(
        [sys.executable, "-c", APPENDER_RUNNER, path,
         str(count), str(interval), kind],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )


# ---------------------------------------------------------------------------
# Basic all-or-nothing behavior
# ---------------------------------------------------------------------------


class TestGroupBasic(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        d = self.tmp.name
        self.p1 = os.path.join(d, "a.log")
        self.p2 = os.path.join(d, "b.log")
        self.p3 = os.path.join(d, "c.log")
        self.paths = [self.p1, self.p2, self.p3]
        with open(self.p1, "wb") as f:
            f.write(v1("a1") + v2("a2") + v1("a3"))
        with open(self.p2, "wb") as f:
            f.write(v2("b1"))
        with open(self.p3, "wb") as f:
            f.write(v1("c1") + v3("c2", note="n"))

    def tearDown(self):
        self.tmp.cleanup()

    def test_whole_group_migrates_together(self):
        result = migrate_log_group(self.paths, quiesce=0.01)
        self.assertTrue(result.replaced)
        self.assertEqual(result.records_migrated, 6)
        self.assertEqual(result.records_skipped, 0)
        per = {os.path.basename(m.path): m for m in result.members}
        self.assertEqual(per["a.log"].records_migrated, 3)
        self.assertEqual(per["b.log"].records_migrated, 1)
        self.assertTrue(per["a.log"].replaced)
        # c.log contains one old record, so it is dirty and renamed.
        self.assertTrue(per["c.log"].replaced)
        for p, n in zip(self.paths, (3, 1, 2)):
            blob = read_bytes(p)
            self.assertEqual(len(blob.splitlines()), n)
            self.assertTrue(all(v == CURRENT_VERSION
                                for v in versions_of(blob)))

    def test_canonical_member_is_not_renamed(self):
        # Replace a.log with fully canonical bytes; only it is clean.
        canonical = v3("clean-1") + v3("clean-2", note="x")
        with open(self.p1, "wb") as f:
            f.write(canonical)
        inode_before = os.stat(self.p1).st_ino
        mtime_before = os.stat(self.p1).st_mtime_ns
        result = migrate_log_group(self.paths, quiesce=0.01)
        per = {os.path.basename(m.path): m for m in result.members}
        self.assertFalse(per["a.log"].replaced)
        self.assertTrue(per["b.log"].replaced)
        self.assertEqual(read_bytes(self.p1), canonical)
        self.assertEqual(os.stat(self.p1).st_ino, inode_before)
        self.assertEqual(os.stat(self.p1).st_mtime_ns, mtime_before)

    def test_strict_bad_record_restores_the_whole_group(self):
        bad = b"BROKEN RECORD HERE\n"
        originals = [read_bytes(p) for p in self.paths]
        # Bad line lives in the second member; the first was already
        # fully prepared before the failure.
        with open(self.p2, "ab") as f:
            f.write(bad)
        with self.assertRaises(GroupBadRecordError) as cm:
            migrate_log_group(self.paths, quiesce=0.01, segment_size=128)
        self.assertEqual(cm.exception.path, self.p2)
        self.assertEqual(cm.exception.lineno, 2)
        self.assertIsInstance(cm.exception, ValueError)
        for p, orig in zip(self.paths, originals):
            self.assertEqual(read_bytes(p), orig + (bad if p == self.p2
                                                    else b""))
        # No backup/staged/work state survives a handled failure.
        leftovers = [n for n in os.listdir(self.tmp.name)
                     if "group" in n or n.endswith(".committed")]
        self.assertEqual(leftovers, [])

    def test_skip_audit_is_file_lineno_snippet(self):
        with open(self.p2, "ab") as f:
            f.write(b"X" * 50 + b"\n")
        buf = io.BytesIO()
        result = migrate_log_group(self.paths, on_bad="skip",
                                   quiesce=0.01, audit=buf)
        self.assertEqual(result.records_skipped, 1)
        self.assertEqual(result.records_migrated, 6)
        audit = buf.getvalue().decode()
        self.assertEqual(len(audit.splitlines()), 1)
        prefix = f"{self.p2}:2:{'X' * 32}"
        self.assertTrue(audit.startswith(prefix), audit)
        self.assertTrue(audit.endswith("\n"))
        # The bad line is absent from the output.
        self.assertEqual(len(read_bytes(self.p2).splitlines()), 1)

    def test_numeric_edge_cases(self):
        p = os.path.join(self.tmp.name, "num.log")
        with open(p, "wb") as f:
            f.write(v1("ok", amount=-0.0))
            f.write(b'{"v":1,"order_id":"nan","amount":NaN,"tags":[]}\n')
            f.write(b'{"v":1,"order_id":"inf","amount":Infinity,'
                    b'"tags":[]}\n')
            f.write(b'{"v":1,"order_id":"big","amount":1e999,'
                    b'"tags":[]}\n')
        buf = io.BytesIO()
        result = migrate_log_group([p], on_bad="skip", quiesce=0.01,
                                   audit=buf)
        self.assertEqual(result.records_skipped, 3)
        self.assertEqual(result.records_migrated, 1)
        self.assertIn(b"-0.0", read_bytes(p))
        self.assertEqual(len(buf.getvalue().splitlines()), 3)

    def test_version_key_problems_are_bad_records(self):
        p = os.path.join(self.tmp.name, "ver.log")
        lines = [
            b'{"order_id":"no-v","amount":1.0,"tags":[]}\n',
            b'{"v":"3","order_id":"str-v","amount":1.0,"tags":[]}\n',
            b'{"v":9,"order_id":"future","amount":1.0,"tags":[]}\n',
        ]
        with open(p, "wb") as f:
            f.writelines(lines)
        buf = io.BytesIO()
        result = migrate_log_group([p], on_bad="skip", quiesce=0.01,
                                   audit=buf)
        self.assertEqual(result.records_skipped, 3)
        self.assertEqual(result.records_migrated, 0)
        # Strict mode on a fresh copy aborts at the first bad line.
        p_strict = os.path.join(self.tmp.name, "ver-strict.log")
        with open(p_strict, "wb") as f:
            f.writelines(lines)
        with self.assertRaises(GroupBadRecordError) as cm:
            migrate_log_group([p_strict], quiesce=0.01)
        self.assertEqual(cm.exception.lineno, 1)


# ---------------------------------------------------------------------------
# Validation and entry-point errors
# ---------------------------------------------------------------------------


class TestGroupValidation(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        d = self.tmp.name
        self.p1 = os.path.join(d, "a.log")
        self.p2 = os.path.join(d, "b.log")
        with open(self.p1, "wb") as f:
            f.write(v1("a"))
        with open(self.p2, "wb") as f:
            f.write(v1("b"))

    def tearDown(self):
        self.tmp.cleanup()

    def test_non_sequence_raises_type_error(self):
        for bad in (self.p1, 42, object()):
            with self.assertRaises(TypeError):
                migrate_log_group(bad)

    def test_empty_or_duplicate_raises_value_error(self):
        with self.assertRaises(ValueError):
            migrate_log_group([])
        with self.assertRaises(ValueError):
            migrate_log_group([self.p1, self.p1])
        # Same file reached through two relative spellings is still a
        # duplicate.
        rel = os.path.relpath(self.p1, start=os.getcwd())
        with self.assertRaises(ValueError):
            migrate_log_group([self.p1, rel])

    def test_missing_member_is_file_not_found(self):
        with self.assertRaises(FileNotFoundError):
            migrate_log_group([self.p1, os.path.join(self.tmp.name, "x")])
        # read_log_group enforces the same contract.
        with self.assertRaises(FileNotFoundError):
            read_log_group([self.p1, os.path.join(self.tmp.name, "x")])

    def test_cli_exit_codes(self):
        # Usage: duplicate members -> 2.
        proc = run_group([self.p1, self.p1])
        self.assertEqual(proc.returncode, EXIT_USAGE, proc.stderr)
        # Missing member -> 1.
        proc = run_group([self.p1, os.path.join(self.tmp.name, "nope")])
        self.assertEqual(proc.returncode, EXIT_ERROR, proc.stderr)
        # Strict bad record -> 3.
        with open(self.p2, "ab") as f:
            f.write(b"bad\n")
        proc = run_group([self.p1, self.p2])
        self.assertEqual(proc.returncode, EXIT_BAD_RECORD, proc.stderr)
        self.assertIn(self.p2.encode(), proc.stderr)
        # Success -> 0.
        proc = run_group([self.p1])
        self.assertEqual(proc.returncode, EXIT_OK, proc.stderr)
        self.assertIn(b"replaced=yes", proc.stdout)

    def test_cli_no_members_is_argparse_usage(self):
        proc = subprocess.run(
            [sys.executable, "-m", "proto_migrate", "migrate-logs"],
            capture_output=True, cwd=REPO_ROOT,
        )
        self.assertEqual(proc.returncode, EXIT_USAGE)

    def test_exports(self):
        self.assertIs(proto_migrate.migrate_log_group, migrate_log_group)
        self.assertIs(proto_migrate.read_log_group, read_log_group)
        self.assertIs(proto_migrate.run_group_cli, run_group_cli)
        self.assertTrue(issubclass(GroupBadRecordError, ValueError))


# ---------------------------------------------------------------------------
# Consistent group snapshots
# ---------------------------------------------------------------------------


class TestGroupSnapshot(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        d = self.tmp.name
        self.paths = [os.path.join(d, n) for n in ("a.log", "b.log")]
        with open(self.paths[0], "wb") as f:
            f.write(v1("a1") + v2("a2"))
        with open(self.paths[1], "wb") as f:
            f.write(v1("b1"))

    def tearDown(self):
        self.tmp.cleanup()

    def test_pre_snapshot_is_raw_post_snapshot_is_uniform(self):
        recs = read_log_group(self.paths)
        self.assertEqual([r["v"] for r in recs], [1, 2, 1])
        migrate_log_group(self.paths, quiesce=0.01)
        recs = read_log_group(self.paths)
        self.assertTrue(all(r["v"] == CURRENT_VERSION for r in recs))
        self.assertEqual([r["order_id"] for r in recs],
                         ["a1", "a2", "b1"])

    def test_torn_tail_excluded_from_snapshot(self):
        with open(self.paths[0], "ab") as f:
            f.write(v1("a3")[:-1])
        recs = read_log_group(self.paths)
        self.assertEqual([r["order_id"] for r in recs],
                         ["a1", "a2", "b1"])

    def test_path_reopening_appender_never_mixes_snapshot(self):
        stop = threading.Event()
        bad = []
        inode_before = os.stat(self.paths[0]).st_ino

        def appender():
            for _ in range(3000):
                if os.stat(self.paths[0]).st_ino != inode_before:
                    break
                time.sleep(0.001)
            i = 0
            while not stop.is_set():
                with open(self.paths[0], "ab") as f:
                    f.write(v1(f"post-{i}"))
                i += 1
                time.sleep(0.002)

        def reader():
            while not stop.is_set():
                recs = read_log_group(self.paths, quiesce=0.01)
                vs = {r["v"] for r in recs}
                if vs and not (vs <= {1, 2} or vs == {3}):
                    bad.append(vs)

        ta = threading.Thread(target=appender)
        tr = threading.Thread(target=reader)
        ta.start()
        tr.start()
        migrate_log_group(self.paths, quiesce=0.02)
        time.sleep(0.05)
        stop.set()
        ta.join(timeout=5)
        tr.join(timeout=5)
        self.assertEqual(bad, [])
        # One rerun rewrites the post-wrap-up old-format tail.
        migrate_log_group(self.paths, quiesce=0.02)
        blob = read_bytes(self.paths[0])
        self.assertTrue(all(v == 3 for v in versions_of(blob)))


# ---------------------------------------------------------------------------
# Crash injection, recovery and resume
# ---------------------------------------------------------------------------


CRASH_POINTS = (
    "group-lock", "group-prepare", "group-stage", "group-staged",
    "group-marker", "group-backups",
)


class TestGroupCrashRecovery(unittest.TestCase):
    SIZES = (120, 200, 60)

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        d = self.tmp.name
        self.paths = [os.path.join(d, f"m{i}.log")
                      for i in range(len(self.SIZES))]
        self.originals = []
        for p, n in zip(self.paths, self.SIZES):
            blob = b"".join(
                (v1 if i % 2 == 0 else v2)(f"{os.path.basename(p)}-{i}")
                for i in range(n)
            )
            with open(p, "wb") as f:
                f.write(blob)
            self.originals.append(blob)
        self.reference = reference_group(self.originals)

    def tearDown(self):
        self.tmp.cleanup()

    def _reset_originals(self):
        for p, blob in zip(self.paths, self.originals):
            with open(p, "wb") as f:
                f.write(blob)

    def _clean_artifacts(self):
        d = self.tmp.name
        for name in os.listdir(d):
            if name.endswith(".log"):
                continue
            target = os.path.join(d, name)
            if os.path.isdir(target):
                import shutil
                shutil.rmtree(target)
            else:
                os.remove(target)

    def test_every_crash_point_recovers_byte_identical(self):
        for point in CRASH_POINTS:
            with self.subTest(point=point):
                self._reset_originals()
                self._clean_artifacts()
                proc = run_group(self.paths, crash=point,
                                 quiesce_ms="5", segment_size="200")
                self.assertEqual(proc.returncode, 1,
                                 (point, proc.stderr))
                proc = run_group(self.paths, quiesce_ms="5",
                                 segment_size="200")
                self.assertEqual(proc.returncode, 0,
                                 (point, proc.stderr))
                for p, ref in zip(self.paths, self.reference):
                    self.assertEqual(read_bytes(p), ref)
                # No protocol debris remains.
                leftovers = [
                    n for n in os.listdir(self.tmp.name)
                    if n.endswith((".migrate-group-backup",
                                   ".migrate-group-staged"))
                    or ".migrate-group-tmp" in n
                    or n == ".migrate-logs-tmp"
                ]
                self.assertEqual(leftovers, [])
                # A third run is idempotent.
                again = run_group(self.paths, quiesce_ms="5")
                self.assertEqual(again.returncode, 0, again.stderr)
                self.assertIn(b"replaced=no", again.stdout)
                for p, ref in zip(self.paths, self.reference):
                    self.assertEqual(read_bytes(p), ref)

    def test_prepared_members_are_not_rescanned_on_resume(self):
        # Killed after the first member staged: every member was
        # prepared, so the resume must not decode a single input line.
        proc = run_group(self.paths, crash="group-stage",
                         quiesce_ms="5", segment_size="200")
        self.assertEqual(proc.returncode, 1, proc.stderr)
        calls = []
        orig_loads = lm.loads

        def counting_loads(raw):
            calls.append(raw)
            return orig_loads(raw)

        lm.loads = counting_loads
        try:
            result = migrate_log_group(self.paths, quiesce=0.005,
                                       segment_size=200)
        finally:
            lm.loads = orig_loads
        self.assertEqual(calls, [])
        self.assertTrue(result.replaced)
        # The finishing invocation itself migrated nothing.
        self.assertEqual(result.records_migrated, 0)
        for p, ref in zip(self.paths, self.reference):
            self.assertEqual(read_bytes(p), ref)

    def test_kill_resume_kill_resume(self):
        for point in ("group-prepare", "group-stage", "group-marker"):
            proc = run_group(self.paths, crash=point, quiesce_ms="5",
                             segment_size="200")
            self.assertEqual(proc.returncode, 1, proc.stderr)
        proc = run_group(self.paths, quiesce_ms="5", segment_size="200")
        self.assertEqual(proc.returncode, 0, proc.stderr)
        for p, ref in zip(self.paths, self.reference):
            self.assertEqual(read_bytes(p), ref)


class TestGroupCrashWithAppenders(unittest.TestCase):
    BASE = 20_000

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        d = self.tmp.name
        self.paths = [os.path.join(d, f"m{i}.log")
                      for i in range(3)]
        for p in self.paths:
            with open(p, "wb") as f:
                for i in range(self.BASE):
                    f.write(v1(f"{os.path.basename(p)}-{i}"))

    def tearDown(self):
        self.tmp.cleanup()

    def test_kill_then_resume_with_live_appenders_zero_loss(self):
        appenders = [
            start_appender(p, 30, interval=0.005,
                           kind="v1" if i % 2 else "v2")
            for i, p in enumerate(self.paths)
        ]
        proc = run_group(self.paths, crash="group-stage",
                         quiesce_ms="10", segment_size="4096")
        self.assertEqual(proc.returncode, 1, proc.stderr)
        proc = run_group(self.paths, quiesce_ms="10",
                         segment_size="4096")
        self.assertEqual(proc.returncode, 0, proc.stderr)
        for a in appenders:
            a.wait(timeout=10)
        # Late appends landing after wrap-up are caught by one final
        # idempotent run.
        deadline = time.time() + 5
        while time.time() < deadline:
            run_group(self.paths, quiesce_ms="10",
                      segment_size="4096")
            if all(all(v == 3 for v in versions_of(read_bytes(p)))
                   for p in self.paths):
                break
            time.sleep(0.05)
        for p in self.paths:
            records = [json.loads(line)
                       for line in read_bytes(p).splitlines()]
            self.assertEqual(len(records), self.BASE + 30)
            self.assertTrue(all(r["v"] == 3 for r in records))
            ids = [r["order_id"] for r in records]
            self.assertEqual(
                ids[:self.BASE],
                [f"{os.path.basename(p)}-{i}" for i in range(self.BASE)],
            )
            self.assertEqual(ids[self.BASE:],
                             [f"late-{i}" for i in range(30)])


# ---------------------------------------------------------------------------
# Live concurrency, post-border faults, idempotency
# ---------------------------------------------------------------------------


class TestGroupConcurrency(unittest.TestCase):
    BASE = 10_000
    APPENDED = 40

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        d = self.tmp.name
        self.paths = [os.path.join(d, f"m{i}.log")
                      for i in range(3)]
        for p in self.paths:
            with open(p, "wb") as f:
                for i in range(self.BASE):
                    f.write(v1(f"{os.path.basename(p)}-{i}"))

    def tearDown(self):
        self.tmp.cleanup()

    def test_appenders_and_readers_no_loss_no_mix(self):
        appenders = [
            start_appender(p, self.APPENDED, interval=0.003,
                           kind="v1" if i % 2 else "v2")
            for i, p in enumerate(self.paths)
        ]
        stop = threading.Event()
        bad_views = []
        views = 0

        def reader():
            nonlocal views
            while not stop.is_set():
                recs = read_log_group(self.paths, quiesce=0.01)
                vs = {r["v"] for r in recs}
                if vs and not (vs <= {1, 2} or vs == {3}):
                    bad_views.append(vs)
                views += 1
                time.sleep(0.0005)

        readers = [threading.Thread(target=reader) for _ in range(3)]
        for t in readers:
            t.start()
        result = migrate_log_group(self.paths, quiesce=0.02)
        for a in appenders:
            a.wait(timeout=10)
        stop.set()
        for t in readers:
            t.join(timeout=5)
        self.assertEqual(bad_views, [])
        self.assertGreater(views, 0)
        total = 0
        for p in self.paths:
            records = [json.loads(line)
                       for line in read_bytes(p).splitlines()]
            total += len(records)
            self.assertTrue(all(r["v"] == 3 for r in records))
        self.assertEqual(total, 3 * (self.BASE + self.APPENDED))
        self.assertEqual(result.records_skipped, 0)


class TestPostBorderFaultsAndIdempotency(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        d = self.tmp.name
        self.paths = [os.path.join(d, f"m{i}.log") for i in range(3)]
        for p in self.paths:
            with open(p, "wb") as f:
                f.write(b"".join(v1(f"{os.path.basename(p)}-{i}")
                                 for i in range(50)))

    def tearDown(self):
        self.tmp.cleanup()

    def test_cleanup_fault_is_warning_still_success(self):
        proc = run_group(self.paths, fault="group-cleanup",
                         quiesce_ms="10", segment_size="200")
        self.assertEqual(proc.returncode, EXIT_OK, proc.stderr)
        self.assertIn(b"replaced=yes", proc.stdout)
        self.assertIn(b"warning:", proc.stderr)
        for p in self.paths:
            self.assertTrue(all(v == 3 for v in versions_of(read_bytes(p))))
        # Rerun finishes (sweeps leftovers), idempotent, exit 0.
        proc = run_group(self.paths, quiesce_ms="10")
        self.assertEqual(proc.returncode, EXIT_OK, proc.stderr)
        self.assertIn(b"replaced=no", proc.stdout)
        leftovers = [
            n for n in os.listdir(self.tmp.name)
            if ".migrate-group-tmp" in n or n == ".migrate-logs-tmp"
        ]
        self.assertEqual(leftovers, [])

    def test_repeated_reruns_rewrite_nothing(self):
        proc = run_group(self.paths, quiesce_ms="10")
        self.assertIn(b"replaced=yes", proc.stdout)
        snaps = [read_bytes(p) for p in self.paths]
        for _ in range(3):
            proc = run_group(self.paths, quiesce_ms="10")
            self.assertEqual(proc.returncode, 0)
            self.assertIn(b"replaced=no", proc.stdout)
            for p, snap in zip(self.paths, snaps):
                self.assertEqual(read_bytes(p), snap)


if __name__ == "__main__":
    unittest.main()
