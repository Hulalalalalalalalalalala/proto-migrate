"""Tests for the streaming snapshot cursor and multi-instance coordination.

Covers:
  * ``read_linked_logs_stream``: batched reads equal the one-shot
    snapshot, batch order is group/member/line, batches never straddle
    a member, the cursor persists between calls, and the pinned
    snapshot shows a single version shape even while a migration and
    path-reopening appenders run;
  * cursor error taxonomy: non-string -> TypeError, corrupt content ->
    ValueError, missing/unreadable pointed member -> FileNotFoundError
    with already returned batches unaffected;
  * multi-instance leases: MigrationLockedError while a lease is held,
    deterministic takeover after the holder disappears, disjoint group
    sets advancing in parallel in one directory;
  * strict mode global first-error ordering across bad lines and bad
    references;
  * post-commit crash recovery converging the backup's appends before
    the backup is removed (zero loss);
  * summary counters: only records this invocation rewrote, consistent
    at member and group level, zero on resume and idempotent reruns.
"""

import io
import json
import os
import subprocess
import sys
import tempfile
import threading
import time
import unittest

import proto_migrate
from proto_migrate import (
    CURRENT_VERSION,
    MigrationLockedError,
    dumps,
    migrate_linked_logs,
    read_linked_logs,
    read_linked_logs_stream,
)
from proto_migrate.group_migration import GroupBadRecordError
from proto_migrate.linked_migration import (
    LinkedBadReferenceError,
    _STREAM_SESSIONS,
    _drop_stream_session,
)
from proto_migrate.log_migration import EXIT_OK

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


def stream_all(groups, batch_records=4096):
    """Drain a stream; return (flat record list, list of batch sizes)."""
    records = []
    sizes = []
    cursor = None
    while True:
        batch, cursor = read_linked_logs_stream(
            groups, cursor, batch_records=batch_records, quiesce=0.01
        )
        records.extend(batch)
        sizes.append(len(batch))
        if cursor is None:
            return records, sizes


LINK = [(1, 0, "order_id", "order_id")]


class TestStreamBasics(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        d = self.tmp.name
        self.orders = os.path.join(d, "orders.log")
        self.details = os.path.join(d, "details.log")
        with open(self.orders, "wb") as f:
            f.write(v1("A1") + v2("A2") + v1("A3"))
        with open(self.details, "wb") as f:
            f.write(v2("A1") + v1("A2"))
        self.groups = [[self.orders], [self.details]]

    def tearDown(self):
        self.tmp.cleanup()

    def test_stream_equals_one_shot_pre_migration(self):
        full = [r for g in read_linked_logs(self.groups) for r in g]
        for batch_records in (1, 2, 3, 100):
            with self.subTest(batch_records=batch_records):
                got, _sizes = stream_all(self.groups, batch_records)
                self.assertEqual(got, full)
        self.assertEqual([r["v"] for r in got], [1, 2, 1, 2, 1])

    def test_stream_equals_one_shot_post_migration(self):
        migrate_linked_logs(self.groups, links=LINK, quiesce=0.01)
        full = [r for g in read_linked_logs(self.groups) for r in g]
        got, _sizes = stream_all(self.groups, 2)
        self.assertEqual(got, full)
        self.assertTrue(all(r["v"] == CURRENT_VERSION for r in got))

    def test_batches_are_member_aligned(self):
        # orders has 3 records, details 2: with room to spare a batch
        # still ends at the member boundary.
        _got, sizes = stream_all(self.groups, 100)
        self.assertEqual(sizes, [3, 2])

    def test_multi_member_groups_and_empty_member(self):
        d = self.tmp.name
        extra = os.path.join(d, "extra.log")
        empty = os.path.join(d, "empty.log")
        with open(extra, "wb") as f:
            f.write(v1("A2"))
        with open(empty, "wb"):
            pass
        groups = [[self.orders, extra], [empty, self.details]]
        full = [r for g in read_linked_logs(groups) for r in g]
        got, _sizes = stream_all(groups, 2)
        self.assertEqual(got, full)
        self.assertEqual([r["order_id"] for r in got],
                         ["A1", "A2", "A3", "A2", "A1", "A2"])

    def test_torn_tail_excluded(self):
        with open(self.details, "ab") as f:
            f.write(v1("A9")[:-1])
        got, _sizes = stream_all(self.groups, 100)
        self.assertEqual([r["order_id"] for r in got],
                         ["A1", "A2", "A3", "A1", "A2"])

    def test_cursor_is_a_persistable_string(self):
        batch1, cursor = read_linked_logs_stream(
            self.groups, None, batch_records=2, quiesce=0.01)
        self.assertIsInstance(cursor, str)
        # "Persist" and resume in a fresh session (simulated restart:
        # the in-process session registry is dropped).
        for token in list(_STREAM_SESSIONS):
            _drop_stream_session(token)
        rest = []
        while cursor is not None:
            batch, cursor = read_linked_logs_stream(
                self.groups, cursor, batch_records=2, quiesce=0.01)
            rest.extend(batch)
        full = [r for g in read_linked_logs(self.groups) for r in g]
        self.assertEqual(batch1 + rest, full)

    def test_session_is_dropped_when_exhausted(self):
        stream_all(self.groups, 1)
        self.assertEqual(dict(_STREAM_SESSIONS), {})

    def test_non_string_cursor_raises_type_error(self):
        for bad in (42, 1.5, b"bytes", ["x"], {"x": 1}, object()):
            with self.subTest(bad=bad):
                with self.assertRaises(TypeError):
                    read_linked_logs_stream(self.groups, bad)

    def test_corrupt_cursor_raises_value_error(self):
        batch, cursor = read_linked_logs_stream(
            self.groups, None, batch_records=1, quiesce=0.01)
        bad_cursors = [
            "", "garbage", "[]", "{}", '{"v":1}',
            '{"v":999,"tok":"x","pol":0,"mi":0,"off":0,"mem":[]}',
            cursor[:-2],          # truncated
            cursor.replace('"mi"', '"mm"'),
        ]
        for bad in bad_cursors:
            with self.subTest(bad=bad):
                with self.assertRaises(ValueError):
                    read_linked_logs_stream(self.groups, bad)

    def test_missing_member_raises_file_not_found(self):
        batch, cursor = read_linked_logs_stream(
            self.groups, None, batch_records=100, quiesce=0.01)
        # First batch covered orders; the cursor points at details.
        self.assertEqual(len(batch), 3)
        self.assertIsNotNone(cursor)
        os.remove(self.details)
        with self.assertRaises(FileNotFoundError):
            read_linked_logs_stream(self.groups, cursor, quiesce=0.01)
        # The batch already returned is unaffected.
        self.assertEqual([r["order_id"] for r in batch],
                         ["A1", "A2", "A3"])

    def test_missing_member_at_start_raises_file_not_found(self):
        missing = os.path.join(self.tmp.name, "nope.log")
        with self.assertRaises(FileNotFoundError):
            read_linked_logs_stream([[self.orders], [missing]])

    def test_stream_spans_migration_with_single_shape(self):
        # A stream opened before the migration keeps serving the pinned
        # pre-migration snapshot even after the commit lands.
        before = [r for g in read_linked_logs(self.groups) for r in g]
        batch1, cursor = read_linked_logs_stream(
            self.groups, None, batch_records=1, quiesce=0.01)
        migrate_linked_logs(self.groups, links=LINK, quiesce=0.01)
        rest = []
        while cursor is not None:
            batch, cursor = read_linked_logs_stream(
                self.groups, cursor, batch_records=1, quiesce=0.01)
            rest.extend(batch)
        self.assertEqual(batch1 + rest, before)
        self.assertTrue(all(r["v"] in (1, 2) for r in batch1 + rest))
        # A fresh stream sees the migrated world.
        got, _sizes = stream_all(self.groups, 2)
        self.assertTrue(all(r["v"] == CURRENT_VERSION for r in got))

    def test_stream_with_live_appender_single_shape(self):
        groups = self.groups
        stop = threading.Event()
        bad = []

        def appender():
            i = 0
            while not stop.is_set():
                with open(self.orders, "ab") as f:
                    f.write(v1(f"L{i}"))
                i += 1
                time.sleep(0.002)

        def reader():
            while not stop.is_set():
                try:
                    got, _sizes = stream_all(groups, 2)
                except FileNotFoundError:
                    continue
                vs = {r["v"] for r in got}
                if vs and not (vs <= {1, 2} or vs == {3}):
                    bad.append(vs)

        ta = threading.Thread(target=appender)
        tr = threading.Thread(target=reader)
        ta.start()
        tr.start()
        migrate_linked_logs(groups, links=LINK, quiesce=0.02)
        time.sleep(0.05)
        stop.set()
        ta.join(timeout=5)
        tr.join(timeout=5)
        self.assertEqual(bad, [])


class TestMultiInstance(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        d = self.tmp.name
        self.orders = os.path.join(d, "orders.log")
        self.details = os.path.join(d, "details.log")
        with open(self.orders, "wb") as f:
            f.write(b"".join(v1(f"O{i}") for i in range(40)))
        with open(self.details, "wb") as f:
            f.write(b"".join(v1(f"O{i}") for i in range(40)))
        self.groups = [[self.orders], [self.details]]

    def tearDown(self):
        self.tmp.cleanup()

    def _leftovers(self):
        return [n for n in os.listdir(self.tmp.name)
                if "migrate-linked" in n]

    def test_active_lease_raises_migration_locked(self):
        import fcntl
        lock = open(self.orders + ".migrate.lock", "a+b")
        fcntl.flock(lock, fcntl.LOCK_EX)
        try:
            with self.assertRaises(MigrationLockedError):
                migrate_linked_logs(self.groups, links=LINK, quiesce=0.01)
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)
            lock.close()
        # Nothing was touched: the files are exactly as before.
        self.assertEqual(len(read_bytes(self.orders).splitlines()), 40)
        # Once the holder is gone the lease is reclaimed.
        result = migrate_linked_logs(self.groups, links=LINK, quiesce=0.01)
        self.assertEqual(result.records_migrated, 80)
        self.assertEqual(self._leftovers(), [])

    def test_disjoint_instances_run_in_parallel(self):
        d = self.tmp.name
        b1 = os.path.join(d, "b1.log")
        b2 = os.path.join(d, "b2.log")
        with open(b1, "wb") as f:
            f.write(b"".join(v1(f"B{i}") for i in range(40)))
        with open(b2, "wb") as f:
            f.write(b"".join(v1(f"B{i}") for i in range(40)))
        other = [[b1], [b2]]
        results = {}
        errors = []

        def run(key, groups):
            try:
                results[key] = migrate_linked_logs(
                    groups, links=LINK, quiesce=0.005)
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        t1 = threading.Thread(target=run, args=("a", self.groups))
        t2 = threading.Thread(target=run, args=("b", other))
        t1.start()
        t2.start()
        t1.join()
        t2.join()
        self.assertEqual(errors, [])
        self.assertEqual(results["a"].records_migrated, 80)
        self.assertEqual(results["b"].records_migrated, 80)
        self.assertEqual(self._leftovers(), [])

    def test_overlapping_instance_fails_fast_then_takes_over(self):
        import fcntl
        d = self.tmp.name
        shared = os.path.join(d, "shared.log")
        with open(shared, "wb") as f:
            f.write(b"".join(v1(f"O{i}") for i in range(20)))
        # Simulate a live foreign instance holding one member's lease.
        import fcntl as _fcntl
        lock = open(shared + ".migrate.lock", "a+b")
        _fcntl.flock(lock, _fcntl.LOCK_EX)
        try:
            with self.assertRaises(MigrationLockedError):
                migrate_linked_logs(
                    [[self.orders], [shared]], links=LINK, quiesce=0.01)
        finally:
            _fcntl.flock(lock, _fcntl.LOCK_UN)
            lock.close()
        result = migrate_linked_logs(
            [[self.orders], [shared]], links=LINK, quiesce=0.01)
        self.assertTrue(result.replaced)
        self.assertEqual(self._leftovers(), [])

    def test_crash_takeover_is_byte_identical_to_serial(self):
        # A crashed instance's state is taken over by the next run
        # without rescanning prepared members.
        runner = (
            "import os,sys;sys.path.insert(0,%r);"
            "os.environ['PROTO_MIGRATE_CRASH_AT']='linked-stage';"
            "from proto_migrate.linked_migration import run_linked_cli;"
            "raise SystemExit(run_linked_cli(sys.argv[1:]))"
        ) % REPO_ROOT
        argv = []
        for g in self.groups:
            argv += ["--group", *g]
        argv += ["--link", "1:0:order_id:order_id", "--skip",
                 "--quiesce-ms=5", "--segment-size=200"]
        proc = subprocess.run([sys.executable, "-c", runner, *argv],
                              capture_output=True)
        self.assertEqual(proc.returncode, 1, proc.stderr)
        result = migrate_linked_logs(
            self.groups, links=LINK, on_bad="skip", quiesce=0.005,
            segment_size=200, audit=io.BytesIO())
        # The finishing invocation migrated nothing itself.
        self.assertEqual(result.records_migrated, 0)
        self.assertEqual(result.references_bad, 0)
        for path in (self.orders, self.details):
            records = [json.loads(line)
                       for line in read_bytes(path).splitlines()]
            self.assertEqual([r["order_id"] for r in records],
                             [f"O{i}" for i in range(40)])
            self.assertTrue(all(r["v"] == 3 for r in records))
        self.assertEqual(self._leftovers(), [])

    def test_cross_groupset_takeover(self):
        # Instance A (groupset {orders, details}) crashes mid-stage;
        # instance B (a different groupset sharing orders) takes the
        # member over from A's durable state; A's own rerun finishes
        # the rest.  The end state equals the serial result.
        runner = (
            "import os,sys;sys.path.insert(0,%r);"
            "os.environ['PROTO_MIGRATE_CRASH_AT']='linked-stage';"
            "from proto_migrate.linked_migration import run_linked_cli;"
            "raise SystemExit(run_linked_cli(sys.argv[1:]))"
        ) % REPO_ROOT
        argv = []
        for g in self.groups:
            argv += ["--group", *g]
        argv += ["--link", "1:0:order_id:order_id", "--skip",
                 "--quiesce-ms=5", "--segment-size=200"]
        proc = subprocess.run([sys.executable, "-c", runner, *argv],
                              capture_output=True)
        self.assertEqual(proc.returncode, 1, proc.stderr)
        result_b = migrate_linked_logs(
            [[self.orders]], links=[], on_bad="skip", quiesce=0.005,
            audit=io.BytesIO())
        # B resumed A's prepared member without migrating anything anew.
        self.assertEqual(result_b.records_migrated, 0)
        result_a = migrate_linked_logs(
            self.groups, links=LINK, on_bad="skip", quiesce=0.005,
            audit=io.BytesIO())
        self.assertEqual(result_a.records_migrated, 0)
        for path in (self.orders, self.details):
            records = [json.loads(line)
                       for line in read_bytes(path).splitlines()]
            self.assertEqual([r["order_id"] for r in records],
                             [f"O{i}" for i in range(40)])
            self.assertTrue(all(r["v"] == 3 for r in records))
        self.assertEqual(self._leftovers(), [])


class TestStrictGlobalOrder(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        d = self.tmp.name
        self.orders = os.path.join(d, "orders.log")
        self.details = os.path.join(d, "details.log")

    def tearDown(self):
        self.tmp.cleanup()

    def test_bad_reference_beats_later_bad_line(self):
        # Group 0 has a dangling reference at line 1; group 1 has a bad
        # line at line 2.  The reference sorts first in global order.
        with open(self.orders, "wb") as f:
            f.write(v1("GHOST"))
        with open(self.details, "wb") as f:
            f.write(v1("A1") + b"BROKEN\n")
        before = (read_bytes(self.orders), read_bytes(self.details))
        with self.assertRaises(LinkedBadReferenceError) as cm:
            migrate_linked_logs(
                [[self.orders], [self.details]],
                links=[(0, 1, "order_id", "order_id")], quiesce=0.01)
        self.assertEqual(cm.exception.path, self.orders)
        self.assertEqual(cm.exception.lineno, 1)
        self.assertEqual(before, (read_bytes(self.orders),
                                  read_bytes(self.details)))
        leftovers = [n for n in os.listdir(self.tmp.name)
                     if "migrate-linked" in n]
        self.assertEqual(leftovers, [])

    def test_bad_line_beats_later_bad_reference(self):
        # Group 0 has a bad line at line 2; group 1 has a dangling
        # reference at line 1.  The bad line sorts first.
        with open(self.orders, "wb") as f:
            f.write(v1("A1") + b"BROKEN\n")
        with open(self.details, "wb") as f:
            f.write(v1("GHOST"))
        with self.assertRaises(GroupBadRecordError) as cm:
            migrate_linked_logs(
                [[self.orders], [self.details]],
                links=LINK, quiesce=0.01)
        self.assertEqual(cm.exception.path, self.orders)
        self.assertEqual(cm.exception.lineno, 2)

    def test_global_order_survives_kill_and_resume(self):
        # A strict run killed after prepare recorded the bad line in
        # the durable line index; the rerun still reports the globally
        # first problem.
        with open(self.orders, "wb") as f:
            f.write(v1("A1") + v1("A2") + b"BROKEN\n")
        with open(self.details, "wb") as f:
            f.write(v1("GHOST"))
        runner = (
            "import os,sys;sys.path.insert(0,%r);"
            "os.environ['PROTO_MIGRATE_CRASH_AT']='linked-prepare';"
            "from proto_migrate.linked_migration import run_linked_cli;"
            "raise SystemExit(run_linked_cli(sys.argv[1:]))"
        ) % REPO_ROOT
        argv = ["--group", self.orders, "--group", self.details,
                "--link", "1:0:order_id:order_id", "--strict",
                "--quiesce-ms=5"]
        proc = subprocess.run([sys.executable, "-c", runner, *argv],
                              capture_output=True)
        self.assertEqual(proc.returncode, 1, proc.stderr)
        with self.assertRaises(GroupBadRecordError) as cm:
            migrate_linked_logs(
                [[self.orders], [self.details]],
                links=LINK, quiesce=0.01)
        self.assertEqual(cm.exception.path, self.orders)
        self.assertEqual(cm.exception.lineno, 3)
        leftovers = [n for n in os.listdir(self.tmp.name)
                     if "migrate-linked" in n]
        self.assertEqual(leftovers, [])


class TestBackupSalvageAfterCommitCrash(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        d = self.tmp.name
        self.orders = os.path.join(d, "orders.log")
        self.details = os.path.join(d, "details.log")
        with open(self.orders, "wb") as f:
            f.write(b"".join(v1(f"O{i}") for i in range(50)))
        with open(self.details, "wb") as f:
            f.write(b"".join(v1(f"O{i}") for i in range(50)))
        self.groups = [[self.orders], [self.details]]

    def tearDown(self):
        self.tmp.cleanup()

    def _crash_at_marker(self):
        runner = (
            "import os,sys;sys.path.insert(0,%r);"
            "os.environ['PROTO_MIGRATE_CRASH_AT']='linked-marker';"
            "from proto_migrate.linked_migration import run_linked_cli;"
            "raise SystemExit(run_linked_cli(sys.argv[1:]))"
        ) % REPO_ROOT
        argv = []
        for g in self.groups:
            argv += ["--group", *g]
        argv += ["--link", "1:0:order_id:order_id", "--skip",
                 "--quiesce-ms=5"]
        proc = subprocess.run([sys.executable, "-c", runner, *argv],
                              capture_output=True)
        self.assertEqual(proc.returncode, 1, proc.stderr)

    def test_appends_on_the_backup_inode_are_not_lost(self):
        # Writers holding the pre-rename inode keep appending after the
        # crash; the recovery run converges those records off the
        # backup before removing it.
        fd_o = open(self.orders, "ab")
        fd_d = open(self.details, "ab")
        try:
            self._crash_at_marker()
            for i in range(50, 60):
                fd_o.write(v1(f"O{i}"))
                fd_d.write(v1(f"O{i}"))
            fd_o.flush()
            fd_d.flush()
        finally:
            fd_o.close()
            fd_d.close()
        result = migrate_linked_logs(
            self.groups, links=LINK, on_bad="skip", quiesce=0.01,
            audit=io.BytesIO())
        self.assertEqual(result.records_migrated, 20)
        for path in (self.orders, self.details):
            records = [json.loads(line)
                       for line in read_bytes(path).splitlines()]
            self.assertEqual([r["order_id"] for r in records],
                             [f"O{i}" for i in range(60)])
            self.assertTrue(all(r["v"] == 3 for r in records))
        leftovers = [n for n in os.listdir(self.tmp.name)
                     if "migrate-linked" in n]
        self.assertEqual(leftovers, [])


class TestCounterConsistency(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        d = self.tmp.name
        self.orders = os.path.join(d, "orders.log")
        self.details = os.path.join(d, "details.log")

    def tearDown(self):
        self.tmp.cleanup()

    def test_clean_member_reports_zero_and_totals_agree(self):
        with open(self.orders, "wb") as f:
            f.write(v3("A1") + v3("A2"))
        with open(self.details, "wb") as f:
            f.write(v1("A1") + v1("A2"))
        result = migrate_linked_logs(
            [[self.orders], [self.details]], links=LINK, quiesce=0.01)
        per = {os.path.basename(m.path): m for m in result.members}
        self.assertEqual(per["orders.log"].records_migrated, 0)
        self.assertEqual(per["details.log"].records_migrated, 2)
        self.assertEqual(
            result.records_migrated,
            sum(m.records_migrated for m in result.members))
        self.assertEqual(result.records_migrated, 2)

    def test_idempotent_rerun_reports_zero(self):
        with open(self.orders, "wb") as f:
            f.write(v1("A1"))
        with open(self.details, "wb") as f:
            f.write(v1("A1"))
        migrate_linked_logs(
            [[self.orders], [self.details]], links=LINK, quiesce=0.01)
        result = migrate_linked_logs(
            [[self.orders], [self.details]], links=LINK, quiesce=0.01)
        self.assertEqual(result.records_migrated, 0)
        self.assertEqual(result.records_skipped, 0)
        self.assertFalse(result.replaced)
        self.assertTrue(all(m.records_migrated == 0
                            for m in result.members))


if __name__ == "__main__":
    unittest.main()
