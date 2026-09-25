"""Tests for the cross-group reference-integrity migration.

Covers:
  * all-or-nothing linked migration with valid references,
  * the four bad-reference classes (dangling, target skipped, cyclic,
    illegal target version) plus cascading drops,
  * strict / skip handling, the ``<file>:<lineno>:<first 32 bytes>``
    audit format and the separate bad-record / bad-reference counters,
  * group-list and link-declaration validation and every CLI exit code,
  * ``read_linked_logs`` snapshots under concurrent appenders,
  * real ``os._exit`` crash injection at every linked protocol point
    with byte-identical recovery and no rescanning of prepared members,
  * post-border fault-as-warning, counters that count only newly
    migrated records / newly resolved references, and idempotent reruns.
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
import proto_migrate.log_migration as lm
from proto_migrate import CURRENT_VERSION, dumps
from proto_migrate.group_migration import GroupBadRecordError
from proto_migrate.linked_migration import (
    LinkedBadReferenceError,
    migrate_linked_logs,
    read_linked_logs,
    run_linked_cli,
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


def order_ids(blob):
    return [json.loads(line)["order_id"] for line in blob.splitlines()
            if line]


LINK = [(1, 0, "order_id", "order_id")]


def reference_linked(groups, links=LINK, on_bad="skip"):
    """Crash-free reference migration of a set of groups of blobs."""
    with tempfile.TemporaryDirectory() as d:
        paths = []
        for gi, group in enumerate(groups):
            member_paths = []
            for mi, blob in enumerate(group):
                p = os.path.join(d, f"g{gi}m{mi}.log")
                with open(p, "wb") as f:
                    f.write(blob)
                member_paths.append(p)
            paths.append(member_paths)
        migrate_linked_logs(paths, links=links, on_bad=on_bad,
                            quiesce=0.01, audit=io.BytesIO())
        return [[read_bytes(p) for p in group] for group in paths]


# ---------------------------------------------------------------------------
# Subprocess drivers
# ---------------------------------------------------------------------------


LINKED_RUNNER = r"""
import os, sys
sys.path.insert(0, %r)
if sys.argv[1] != "-":
    os.environ["PROTO_MIGRATE_CRASH_AT"] = sys.argv[1]
if len(sys.argv) > 2 and sys.argv[2] != "-":
    os.environ["PROTO_MIGRATE_FAULT_AT"] = sys.argv[2]
from proto_migrate.linked_migration import run_linked_cli
rest = [a for a in sys.argv[3:] if not a.startswith("@@")]
raise SystemExit(run_linked_cli(rest))
""" % REPO_ROOT


def run_linked(groups, links=("--link", "1:0:order_id:order_id"),
               crash="-", fault="-", on_bad="skip",
               quiesce_ms="10", segment_size=str(16 * 1024 * 1024)):
    argv = []
    for group in groups:
        argv.append("--group")
        argv.extend(group)
    argv.extend(links)
    argv += [
        f"--quiesce-ms={quiesce_ms}",
        f"--segment-size={segment_size}",
        "--skip" if on_bad == "skip" else "--strict",
    ]
    return subprocess.run(
        [sys.executable, "-c", LINKED_RUNNER, crash, fault, *argv],
        capture_output=True,
    )


APPENDER_RUNNER = r"""
import os, sys, time, json
path, count, interval, prefix = sys.argv[1:5]
for i in range(int(count)):
    line = (b'{"v":1,"order_id":' + json.dumps(prefix + "%d" % i).encode()
            + b',"amount":4.0,"tags":[]}\n')
    with open(path, "ab") as f:
        f.write(line)
    time.sleep(float(interval))
"""


def start_appender(path, count, interval=0.004, prefix="late-"):
    return subprocess.Popen(
        [sys.executable, "-c", APPENDER_RUNNER, path,
         str(count), str(interval), prefix],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )


# ---------------------------------------------------------------------------
# Basic linked migration
# ---------------------------------------------------------------------------


class TestLinkedBasic(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        d = self.tmp.name
        self.orders = os.path.join(d, "orders.log")
        self.details = os.path.join(d, "details.log")
        with open(self.orders, "wb") as f:
            f.write(v1("A1") + v2("A2") + v1("A3"))
        with open(self.details, "wb") as f:
            f.write(v1("A1") + v1("A2") + v2("A3"))

    def tearDown(self):
        self.tmp.cleanup()

    def test_valid_links_migrate_together(self):
        result = migrate_linked_logs(
            [[self.orders], [self.details]], links=LINK, quiesce=0.01)
        self.assertTrue(result.replaced)
        self.assertEqual(result.records_migrated, 6)
        self.assertEqual(result.records_skipped, 0)
        self.assertEqual(result.references_bad, 0)
        self.assertEqual(len(result.groups), 2)
        self.assertEqual(len(result.members), 2)
        for path, ids in ((self.orders, ["A1", "A2", "A3"]),
                          (self.details, ["A1", "A2", "A3"])):
            blob = read_bytes(path)
            self.assertEqual(order_ids(blob), ids)
            self.assertTrue(all(v == CURRENT_VERSION
                                for v in versions_of(blob)))

    def test_canonical_groups_are_left_untouched(self):
        with open(self.orders, "wb") as f:
            f.write(v3("A1") + v3("A2"))
        with open(self.details, "wb") as f:
            f.write(v3("A1") + v3("A2"))
        before = (read_bytes(self.orders), read_bytes(self.details))
        inodes = (os.stat(self.orders).st_ino,
                  os.stat(self.details).st_ino)
        result = migrate_linked_logs(
            [[self.orders], [self.details]], links=LINK, quiesce=0.01)
        self.assertFalse(result.replaced)
        self.assertEqual(result.records_migrated, 0)
        self.assertEqual(result.references_bad, 0)
        self.assertEqual(before, (read_bytes(self.orders),
                                  read_bytes(self.details)))
        self.assertEqual(inodes, (os.stat(self.orders).st_ino,
                                  os.stat(self.details).st_ino))

    def test_multiple_members_per_group(self):
        extra = os.path.join(self.tmp.name, "extra.log")
        with open(extra, "wb") as f:
            f.write(v1("A1"))
        result = migrate_linked_logs(
            [[self.orders], [self.details, extra]], links=LINK,
            quiesce=0.01)
        self.assertTrue(result.replaced)
        self.assertEqual(result.records_migrated, 7)
        self.assertEqual(order_ids(read_bytes(extra)), ["A1"])

    def test_numeric_edge_cases_unchanged(self):
        with open(self.orders, "wb") as f:
            f.write(v1("A1", amount=-0.0))
        with open(self.details, "wb") as f:
            f.write(v1("A1", amount=-0.0))
            f.write(b'{"v":1,"order_id":"nan","amount":NaN,"tags":[]}\n')
            f.write(b'{"v":1,"order_id":"big","amount":1e999,"tags":[]}\n')
        buf = io.BytesIO()
        result = migrate_linked_logs(
            [[self.orders], [self.details]], links=LINK, on_bad="skip",
            quiesce=0.01, audit=buf)
        self.assertEqual(result.records_skipped, 2)
        self.assertEqual(result.references_bad, 0)
        self.assertIn(b"-0.0", read_bytes(self.orders))
        self.assertIn(b"-0.0", read_bytes(self.details))


# ---------------------------------------------------------------------------
# Bad references: the four classes, strict/skip, audit, counters
# ---------------------------------------------------------------------------


class TestBadReferences(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        d = self.tmp.name
        self.orders = os.path.join(d, "orders.log")
        self.details = os.path.join(d, "details.log")

    def tearDown(self):
        self.tmp.cleanup()

    def _write(self, orders_blob, details_blob):
        with open(self.orders, "wb") as f:
            f.write(orders_blob)
        with open(self.details, "wb") as f:
            f.write(details_blob)

    def test_dangling_skip_drops_and_audits(self):
        self._write(v1("A1"), v1("A1") + v1("GHOST"))
        buf = io.BytesIO()
        result = migrate_linked_logs(
            [[self.orders], [self.details]], links=LINK, on_bad="skip",
            quiesce=0.01, audit=buf)
        self.assertEqual(result.references_bad, 1)
        self.assertEqual(result.records_skipped, 0)
        self.assertEqual(result.records_migrated, 2)
        self.assertEqual(order_ids(read_bytes(self.details)), ["A1"])
        audit = buf.getvalue()
        self.assertEqual(len(audit.splitlines()), 1)
        prefix = f"{self.details}:2:".encode() + v1("GHOST")[:32]
        self.assertTrue(audit.startswith(prefix), audit)
        self.assertTrue(audit.endswith(b"\n"))

    def test_dangling_strict_aborts_before_any_rename(self):
        self._write(v1("A1"), v1("A1") + v1("GHOST"))
        before = (read_bytes(self.orders), read_bytes(self.details))
        with self.assertRaises(LinkedBadReferenceError) as cm:
            migrate_linked_logs(
                [[self.orders], [self.details]], links=LINK, quiesce=0.01)
        self.assertIsInstance(cm.exception, ValueError)
        self.assertEqual(cm.exception.path, self.details)
        self.assertEqual(cm.exception.lineno, 2)
        self.assertEqual(before, (read_bytes(self.orders),
                                  read_bytes(self.details)))
        # No work state survives a handled failure.
        leftovers = [n for n in os.listdir(self.tmp.name)
                     if "migrate-linked" in n and n != ".migrate-linked.lock"]
        self.assertEqual(leftovers, [])

    def test_target_skipped_as_bad_record(self):
        self._write(
            v1("A1") + b'{"v":1,"order_id":"BAD","amount":NaN,"tags":[]}\n',
            v1("A1") + v1("BAD"),
        )
        buf = io.BytesIO()
        result = migrate_linked_logs(
            [[self.orders], [self.details]], links=LINK, on_bad="skip",
            quiesce=0.01, audit=buf)
        # The NaN line is a bad record; the record referencing it is a
        # bad reference -- counted separately.
        self.assertEqual(result.records_skipped, 1)
        self.assertEqual(result.references_bad, 1)
        self.assertEqual(order_ids(read_bytes(self.orders)), ["A1"])
        self.assertEqual(order_ids(read_bytes(self.details)), ["A1"])
        # One audit line for the skipped record, one for the dropped
        # referrer.
        self.assertEqual(len(buf.getvalue().splitlines()), 2)

    def test_target_with_illegal_version_key(self):
        self._write(
            v1("A1") + b'{"v":9,"order_id":"FUT","amount":1.0,"tags":[]}\n',
            v1("A1") + v1("FUT"),
        )
        buf = io.BytesIO()
        result = migrate_linked_logs(
            [[self.orders], [self.details]], links=LINK, on_bad="skip",
            quiesce=0.01, audit=buf)
        self.assertEqual(result.records_skipped, 1)
        self.assertEqual(result.references_bad, 1)
        self.assertEqual(order_ids(read_bytes(self.details)), ["A1"])

    def test_cyclic_reference(self):
        g0 = os.path.join(self.tmp.name, "g0.log")
        g1 = os.path.join(self.tmp.name, "g1.log")
        with open(g0, "wb") as f:
            f.write(v1("X"))
        with open(g1, "wb") as f:
            f.write(v1("X"))
        links = [(0, 1, "order_id", "order_id"),
                 (1, 0, "order_id", "order_id")]
        buf = io.BytesIO()
        result = migrate_linked_logs(
            [[g0], [g1]], links=links, on_bad="skip", quiesce=0.01,
            audit=buf)
        self.assertEqual(result.references_bad, 2)
        self.assertEqual(read_bytes(g0), b"")
        self.assertEqual(read_bytes(g1), b"")
        # Strict mode reports the cycle as the first bad reference.
        with open(g0, "wb") as f:
            f.write(v1("X"))
        with open(g1, "wb") as f:
            f.write(v1("X"))
        with self.assertRaises(LinkedBadReferenceError):
            migrate_linked_logs([[g0], [g1]], links=links, quiesce=0.01)

    def test_cascade_through_dropped_target(self):
        g0 = os.path.join(self.tmp.name, "c0.log")
        g1 = os.path.join(self.tmp.name, "c1.log")
        g2 = os.path.join(self.tmp.name, "c2.log")
        with open(g0, "wb") as f:
            f.write(v1("ROOT"))
        with open(g1, "wb") as f:
            f.write(v1("ROOT") + v1("MID"))
        with open(g2, "wb") as f:
            f.write(v1("MID") + v1("GHOST"))
        links = [(1, 0, "order_id", "order_id"),
                 (2, 1, "order_id", "order_id")]
        result = migrate_linked_logs(
            [[g0], [g1], [g2]], links=links, on_bad="skip", quiesce=0.01,
            audit=io.BytesIO())
        # g1:MID dangles, g2:MID cascades, g2:GHOST dangles.
        self.assertEqual(result.references_bad, 3)
        self.assertEqual(order_ids(read_bytes(g1)), ["ROOT"])
        self.assertEqual(read_bytes(g2), b"")

    def test_strict_first_bad_line_beats_bad_reference(self):
        # The bad line sorts before the bad reference in global order
        # (group 0 is prepared first), so it is the reported failure.
        self._write(v1("A1") + b"BROKEN\n", v1("GHOST"))
        before = (read_bytes(self.orders), read_bytes(self.details))
        with self.assertRaises(GroupBadRecordError) as cm:
            migrate_linked_logs(
                [[self.orders], [self.details]], links=LINK, quiesce=0.01)
        self.assertEqual(cm.exception.path, self.orders)
        self.assertEqual(cm.exception.lineno, 2)
        # Every member is left exactly as it was; no work state remains.
        self.assertEqual(before, (read_bytes(self.orders),
                                  read_bytes(self.details)))
        leftovers = [n for n in os.listdir(self.tmp.name)
                     if "migrate-linked" in n and n != ".migrate-linked.lock"]
        self.assertEqual(leftovers, [])

    def test_strict_first_bad_reference_is_global_first(self):
        d = self.tmp.name
        g0 = os.path.join(d, "f0.log")
        g1 = os.path.join(d, "f1.log")
        with open(g0, "wb") as f:
            f.write(v1("A1"))
        with open(self.orders, "wb") as f:
            f.write(v1("MISS1"))
        with open(g1, "wb") as f:
            f.write(v1("A1"))
        with open(self.details, "wb") as f:
            f.write(v1("MISS2"))
        # Groups: [orders(ghost ref), details(ghost ref)] both dangling;
        # the first group order is the reported one.
        with self.assertRaises(LinkedBadReferenceError) as cm:
            migrate_linked_logs(
                [[self.orders, g0], [self.details, g1]],
                links=[(0, 1, "order_id", "order_id")], quiesce=0.01)
        self.assertEqual(cm.exception.path, self.orders)
        self.assertEqual(cm.exception.lineno, 1)


# ---------------------------------------------------------------------------
# Validation and entry-point errors
# ---------------------------------------------------------------------------


class TestLinkedValidation(unittest.TestCase):
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
                migrate_linked_logs(bad)
        # A group that is itself not a sequence of members.
        with self.assertRaises(TypeError):
            migrate_linked_logs([self.p1])

    def test_empty_or_duplicate_raises_value_error(self):
        with self.assertRaises(ValueError):
            migrate_linked_logs([])
        with self.assertRaises(ValueError):
            migrate_linked_logs([[]])
        with self.assertRaises(ValueError):
            migrate_linked_logs([[self.p1], [self.p1]])
        with self.assertRaises(ValueError):
            migrate_linked_logs([[self.p1, self.p1]])

    def test_bad_link_declarations(self):
        with self.assertRaises(TypeError):
            migrate_linked_logs([[self.p1]], links="0:0:order_id")
        with self.assertRaises(ValueError):
            migrate_linked_logs([[self.p1]], links=[(0, 0, "order_id")])
        with self.assertRaises(ValueError):
            migrate_linked_logs(
                [[self.p1]], links=[(0, 1, "order_id", "order_id")])
        with self.assertRaises(ValueError):
            migrate_linked_logs(
                [[self.p1]], links=[(0, 0, "", "order_id")])
        with self.assertRaises(ValueError):
            migrate_linked_logs(
                [[self.p1]],
                links=[(0, 0, "order_id", "order_id"),
                       (0, 0, "order_id", "order_id")])

    def test_missing_member_is_file_not_found(self):
        missing = os.path.join(self.tmp.name, "x")
        with self.assertRaises(FileNotFoundError):
            migrate_linked_logs([[self.p1], [missing]])
        with self.assertRaises(FileNotFoundError):
            read_linked_logs([[self.p1], [missing]])

    def test_cli_exit_codes(self):
        # Usage: duplicate members -> 2.
        proc = run_linked([[self.p1], [self.p1]])
        self.assertEqual(proc.returncode, EXIT_USAGE, proc.stderr)
        # Missing member -> 1.
        proc = run_linked([[self.p1], [os.path.join(self.tmp.name, "x")]])
        self.assertEqual(proc.returncode, EXIT_ERROR, proc.stderr)
        # Strict bad reference -> 3.
        proc = run_linked([[self.p1], [self.p2]], on_bad="strict")
        self.assertEqual(proc.returncode, EXIT_BAD_RECORD, proc.stderr)
        self.assertIn(self.p2.encode(), proc.stderr)
        # Success -> 0.
        with open(self.p2, "wb") as f:
            f.write(v1("a"))
        proc = run_linked([[self.p1], [self.p2]], on_bad="skip")
        self.assertEqual(proc.returncode, EXIT_OK, proc.stderr)
        self.assertIn(b"replaced=yes", proc.stdout)
        self.assertIn(b"refs_bad=0", proc.stdout)

    def test_cli_malformed_link_is_usage(self):
        proc = run_linked([[self.p1]], links=("--link", "bogus"))
        self.assertEqual(proc.returncode, EXIT_USAGE, proc.stderr)

    def test_exports(self):
        self.assertIs(proto_migrate.migrate_linked_logs, migrate_linked_logs)
        self.assertIs(proto_migrate.read_linked_logs, read_linked_logs)
        self.assertIs(proto_migrate.run_linked_cli, run_linked_cli)
        self.assertIs(proto_migrate.LinkedBadReferenceError,
                      LinkedBadReferenceError)
        self.assertTrue(issubclass(LinkedBadReferenceError, ValueError))


# ---------------------------------------------------------------------------
# Consistent cross-group snapshots
# ---------------------------------------------------------------------------


class TestLinkedSnapshot(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        d = self.tmp.name
        self.orders = os.path.join(d, "orders.log")
        self.details = os.path.join(d, "details.log")
        with open(self.orders, "wb") as f:
            f.write(v1("A1") + v2("A2"))
        with open(self.details, "wb") as f:
            f.write(v1("A1"))

    def tearDown(self):
        self.tmp.cleanup()

    def test_pre_snapshot_raw_post_snapshot_uniform(self):
        groups = [[self.orders], [self.details]]
        recs = read_linked_logs(groups)
        self.assertEqual(len(recs), 2)
        self.assertEqual([r["v"] for r in recs[0]], [1, 2])
        self.assertEqual([r["v"] for r in recs[1]], [1])
        migrate_linked_logs(groups, links=LINK, quiesce=0.01)
        recs = read_linked_logs(groups)
        self.assertTrue(all(r["v"] == CURRENT_VERSION
                            for group in recs for r in group))
        self.assertEqual([r["order_id"] for r in recs[0]], ["A1", "A2"])
        self.assertEqual([r["order_id"] for r in recs[1]], ["A1"])

    def test_torn_tail_excluded(self):
        with open(self.details, "ab") as f:
            f.write(v1("A2")[:-1])
        recs = read_linked_logs([[self.orders], [self.details]])
        self.assertEqual([r["order_id"] for r in recs[1]], ["A1"])

    def test_path_reopening_appender_never_mixes_snapshot(self):
        groups = [[self.orders], [self.details]]
        stop = threading.Event()
        bad = []
        inode_before = os.stat(self.orders).st_ino

        def appender():
            for _ in range(3000):
                if os.stat(self.orders).st_ino != inode_before:
                    break
                time.sleep(0.001)
            i = 0
            while not stop.is_set():
                with open(self.orders, "ab") as f:
                    f.write(v1("A2"))
                i += 1
                time.sleep(0.002)

        def reader():
            while not stop.is_set():
                recs = read_linked_logs(groups, quiesce=0.01)
                vs = {r["v"] for group in recs for r in group}
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
        # One rerun rewrites the post-wrap-up old-format tail.
        migrate_linked_logs(groups, links=LINK, quiesce=0.02)
        self.assertTrue(all(v == 3
                            for v in versions_of(read_bytes(self.orders))))


# ---------------------------------------------------------------------------
# Crash injection, recovery and resume
# ---------------------------------------------------------------------------


CRASH_POINTS = (
    "linked-lock", "linked-prepare", "linked-refs", "linked-filter",
    "linked-stage", "linked-staged", "linked-marker", "linked-backups",
    "linked-cleanup",
)


class TestLinkedCrashRecovery(unittest.TestCase):
    SIZES = (120, 90)

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        d = self.tmp.name
        self.orders = os.path.join(d, "orders.log")
        self.details = os.path.join(d, "details.log")
        self.order_blob = b"".join(
            (v1 if i % 2 else v2)(f"O{i}") for i in range(self.SIZES[0]))
        self.detail_blob = b"".join(
            v1(f"O{i}") for i in range(self.SIZES[1])) + v1("GHOST")
        self.groups = [[self.orders], [self.details]]
        self.reference = reference_linked(
            [[self.order_blob], [self.detail_blob]])

    def tearDown(self):
        self.tmp.cleanup()

    def _reset(self):
        with open(self.orders, "wb") as f:
            f.write(self.order_blob)
        with open(self.details, "wb") as f:
            f.write(self.detail_blob)

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
                self._reset()
                self._clean_artifacts()
                proc = run_linked(self.groups, crash=point,
                                  quiesce_ms="5", segment_size="200")
                self.assertEqual(proc.returncode, 1,
                                 (point, proc.stderr))
                proc = run_linked(self.groups, quiesce_ms="5",
                                  segment_size="200")
                self.assertEqual(proc.returncode, 0,
                                 (point, proc.stderr))
                self.assertEqual(read_bytes(self.orders),
                                 self.reference[0][0])
                self.assertEqual(read_bytes(self.details),
                                 self.reference[1][0])
                leftovers = [
                    n for n in os.listdir(self.tmp.name)
                    if "migrate-linked" in n
                    and n != ".migrate-linked.lock"
                ]
                self.assertEqual(leftovers, [])
                # A third run is idempotent.
                again = run_linked(self.groups, quiesce_ms="5")
                self.assertEqual(again.returncode, 0, again.stderr)
                self.assertIn(b"replaced=no", again.stdout)
                self.assertIn(b"refs_bad=0", again.stdout)

    def test_prepared_members_are_not_rescanned_on_resume(self):
        self._reset()
        proc = run_linked(self.groups, crash="linked-stage",
                          quiesce_ms="5", segment_size="200")
        self.assertEqual(proc.returncode, 1, proc.stderr)
        calls = []
        orig_loads = lm.loads

        def counting_loads(raw):
            calls.append(raw)
            return orig_loads(raw)

        lm.loads = counting_loads
        try:
            result = migrate_linked_logs(
                self.groups, links=LINK, on_bad="skip", quiesce=0.005,
                segment_size=200, audit=io.BytesIO())
        finally:
            lm.loads = orig_loads
        self.assertEqual(calls, [])
        self.assertTrue(result.replaced)
        # The finishing invocation itself migrated nothing and resolved
        # no new references.
        self.assertEqual(result.records_migrated, 0)
        self.assertEqual(result.references_bad, 0)
        self.assertEqual(read_bytes(self.orders), self.reference[0][0])
        self.assertEqual(read_bytes(self.details), self.reference[1][0])

    def test_kill_resume_kill_resume(self):
        self._reset()
        for point in ("linked-prepare", "linked-stage", "linked-marker"):
            proc = run_linked(self.groups, crash=point, quiesce_ms="5",
                              segment_size="200")
            self.assertEqual(proc.returncode, 1, (point, proc.stderr))
        proc = run_linked(self.groups, quiesce_ms="5", segment_size="200")
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(read_bytes(self.orders), self.reference[0][0])
        self.assertEqual(read_bytes(self.details), self.reference[1][0])


class TestLinkedCrashWithAppenders(unittest.TestCase):
    BASE = 4000

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        d = self.tmp.name
        self.orders = os.path.join(d, "orders.log")
        self.details = os.path.join(d, "details.log")
        with open(self.orders, "wb") as f:
            for i in range(self.BASE):
                f.write(v1(f"O{i}"))
        with open(self.details, "wb") as f:
            for i in range(self.BASE):
                f.write(v1(f"O{i}"))
        self.groups = [[self.orders], [self.details]]

    def tearDown(self):
        self.tmp.cleanup()

    def test_kill_then_resume_with_live_appenders_zero_loss(self):
        # Orders gain brand-new records; details gain records
        # referencing orders that already exist, so no reference can
        # dangle regardless of append/prepare interleaving.
        appenders = [
            start_appender(self.orders, 20, prefix="OL"),
            start_appender(self.details, 20, prefix="O"),
        ]
        proc = run_linked(self.groups, crash="linked-stage",
                          quiesce_ms="10", segment_size="4096")
        self.assertEqual(proc.returncode, 1, proc.stderr)
        proc = run_linked(self.groups, quiesce_ms="10",
                          segment_size="4096")
        self.assertEqual(proc.returncode, 0, proc.stderr)
        for a in appenders:
            a.wait(timeout=10)
        deadline = time.time() + 5
        while time.time() < deadline:
            run_linked(self.groups, quiesce_ms="10", segment_size="4096")
            if all(all(v == 3 for v in versions_of(read_bytes(p)))
                   for p in (self.orders, self.details)):
                break
            time.sleep(0.05)
        for p, prefix in ((self.orders, "OL"), (self.details, "O")):
            records = [json.loads(line)
                       for line in read_bytes(p).splitlines()]
            self.assertTrue(all(r["v"] == 3 for r in records))
            ids = [r["order_id"] for r in records]
            self.assertEqual(ids[:self.BASE],
                             [f"O{i}" for i in range(self.BASE)])
            self.assertEqual(ids[self.BASE:],
                             [f"{prefix}{i}" for i in range(20)])


# ---------------------------------------------------------------------------
# Live concurrency, post-border faults, idempotency
# ---------------------------------------------------------------------------


class TestLinkedConcurrency(unittest.TestCase):
    BASE = 3000
    APPENDED = 20

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        d = self.tmp.name
        self.orders = os.path.join(d, "orders.log")
        self.details = os.path.join(d, "details.log")
        with open(self.orders, "wb") as f:
            for i in range(self.BASE):
                f.write(v1(f"O{i}"))
        with open(self.details, "wb") as f:
            for i in range(self.BASE):
                f.write(v1(f"O{i}"))
        self.groups = [[self.orders], [self.details]]

    def tearDown(self):
        self.tmp.cleanup()

    def test_appenders_and_readers_no_loss_no_mix(self):
        # Orders gain brand-new records; details gain records
        # referencing orders that already exist (refs can never dangle
        # no matter how appends interleave with the prepare).
        appenders = [
            start_appender(self.orders, self.APPENDED, prefix="OL"),
            start_appender(self.details, self.APPENDED, prefix="O"),
        ]
        stop = threading.Event()
        bad_views = []
        views = 0

        def reader():
            nonlocal views
            while not stop.is_set():
                recs = read_linked_logs(self.groups, quiesce=0.01)
                vs = {r["v"] for group in recs for r in group}
                if vs and not (vs <= {1, 2} or vs == {3}):
                    bad_views.append(vs)
                views += 1
                time.sleep(0.0005)

        readers = [threading.Thread(target=reader) for _ in range(2)]
        for t in readers:
            t.start()
        result = migrate_linked_logs(self.groups, links=LINK, quiesce=0.02)
        for a in appenders:
            a.wait(timeout=10)
        stop.set()
        for t in readers:
            t.join(timeout=5)
        self.assertEqual(bad_views, [])
        self.assertGreater(views, 0)
        self.assertEqual(result.records_skipped, 0)
        self.assertEqual(result.references_bad, 0)
        for p, tail_prefix in ((self.orders, "OL"), (self.details, "O")):
            records = [json.loads(line)
                       for line in read_bytes(p).splitlines()]
            self.assertEqual(len(records), self.BASE + self.APPENDED)
            self.assertTrue(all(r["v"] == 3 for r in records))
            ids = [r["order_id"] for r in records]
            self.assertEqual(ids[:self.BASE],
                             [f"O{i}" for i in range(self.BASE)])
            self.assertEqual(ids[self.BASE:],
                             [f"{tail_prefix}{i}"
                              for i in range(self.APPENDED)])


class TestLinkedPostBorderFaultsAndIdempotency(unittest.TestCase):
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

    def test_cleanup_fault_is_warning_still_success(self):
        proc = run_linked(self.groups, fault="linked-cleanup",
                          quiesce_ms="10", segment_size="200")
        self.assertEqual(proc.returncode, EXIT_OK, proc.stderr)
        self.assertIn(b"replaced=yes", proc.stdout)
        self.assertIn(b"warning:", proc.stderr)
        for p in (self.orders, self.details):
            self.assertTrue(all(v == 3
                                for v in versions_of(read_bytes(p))))
        # Rerun finishes (sweeps leftovers), idempotent, exit 0.
        proc = run_linked(self.groups, quiesce_ms="10")
        self.assertEqual(proc.returncode, EXIT_OK, proc.stderr)
        self.assertIn(b"replaced=no", proc.stdout)
        leftovers = [
            n for n in os.listdir(self.tmp.name)
            if "migrate-linked" in n and n != ".migrate-linked.lock"
        ]
        self.assertEqual(leftovers, [])

    def test_repeated_reruns_rewrite_nothing(self):
        proc = run_linked(self.groups, quiesce_ms="10")
        self.assertIn(b"replaced=yes", proc.stdout)
        snaps = (read_bytes(self.orders), read_bytes(self.details))
        for _ in range(3):
            proc = run_linked(self.groups, quiesce_ms="10")
            self.assertEqual(proc.returncode, 0)
            self.assertIn(b"replaced=no", proc.stdout)
            self.assertIn(b"refs_bad=0", proc.stdout)
            self.assertEqual(snaps, (read_bytes(self.orders),
                                     read_bytes(self.details)))


if __name__ == "__main__":
    unittest.main()
