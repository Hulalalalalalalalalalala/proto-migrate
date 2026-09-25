"""Tests for the cross-group, reference-aware linked migration.

Covers:
  * externally declared (source-group, target-group) references with
    earliest member-order/line-order same-order-id targeting,
  * the four bad-reference kinds (missing, target-skipped, illegal
    target version key, chain looping back to its source) and the
    global first-bad-line-or-ref ordering in strict mode,
  * skip mode dropping + transitive closure, the
    ``<file>:<lineno>:<first 32 bytes>`` audit format, and bad-line
    vs bad-reference counters kept apart,
  * all-or-nothing rollback, group/ref-list validation and CLI codes,
  * ``read_linked_logs`` snapshots (torn tails, path-reopening
    appenders never mixing field shapes),
  * real ``os._exit`` crash injection at every linked protocol point
    with byte-identical recovery, no rescanning of prepared members and
    idempotent zero-counter reruns,
  * numeric edge cases and post-border fault-as-warning.
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
import proto_migrate.linked_migration as lim
from proto_migrate import CURRENT_VERSION
from proto_migrate.linked_migration import (
    LinkedBadReferenceError,
    migrate_linked_groups,
    read_linked_logs,
    run_linked_cli,
)
from proto_migrate.group_migration import GroupBadRecordError
from proto_migrate.log_migration import (
    EXIT_BAD_RECORD,
    EXIT_ERROR,
    EXIT_OK,
    EXIT_USAGE,
)

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


# ---------------------------------------------------------------------------
# Builders
# ---------------------------------------------------------------------------


def v1(order_id, amount=1.0):
    return (
        b'{"v":1,"order_id":' + json.dumps(order_id).encode()
        + b',"amount":' + repr(amount).encode() + b',"tags":[]}\n'
    )


def v2(order_id, status="paid"):
    return (
        b'{"v":2,"order_id":' + json.dumps(order_id).encode()
        + b',"amount":2.5,"tags":["t"],"status":'
        + json.dumps(status).encode() + b"}\n"
    )


def v3(order_id, amount=3.0, status="paid", note="", updated_at=0):
    from proto_migrate import dumps
    return dumps({
        "order_id": order_id, "amount": amount, "status": status,
        "note": note, "updated_at": updated_at,
    })


def vbad(order_id):
    return (b'{"v":"9","order_id":' + json.dumps(order_id).encode()
            + b',"amount":1}\n')


def read_bytes(path):
    with open(path, "rb") as f:
        return f.read()


def versions_of(blob):
    return [json.loads(line)["v"] for line in blob.splitlines() if line]


def ids_of(blob):
    return [json.loads(line)["order_id"]
            for line in blob.splitlines() if line]


def ref_linked(originals, refs, on_bad="strict"):
    """Crash-free reference migration of nested group blobs."""
    with tempfile.TemporaryDirectory() as d:
        out = {}
        groups = []
        flat = [blob for group in originals for blob in group]
        paths = []
        for i, blob in enumerate(flat):
            p = os.path.join(d, f"m{i}.log")
            with open(p, "wb") as f:
                f.write(blob)
            paths.append(p)
        k = 0
        for blobs in originals:
            groups.append(paths[k:k + len(blobs)])
            k += len(blobs)
        migrate_linked_groups(groups, refs, on_bad=on_bad, quiesce=0.01)
        for i in range(len(flat)):
            p = os.path.join(d, f"m{i}.log")
            out[os.path.basename(p)] = read_bytes(p)
        return out


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
raise SystemExit(run_linked_cli(sys.argv[3:]))
""" % REPO_ROOT


def run_linked(groups, refs=(), crash="-", fault="-", on_bad="strict",
               quiesce_ms="10", segment_size=str(16 * 1024 * 1024)):
    argv = []
    for group in groups:
        argv.append("--group")
        argv.extend(group)
    for src, dst in refs:
        argv += ["--ref", f"{src}:{dst}"]
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
path, count, interval = sys.argv[1:4]
def enc(i):
    return (b'{"v":1,"order_id":' + json.dumps("late-%d" % i).encode()
            + b',"amount":4.0,"tags":[]}\n')
for i in range(int(count)):
    with open(path, "ab") as f:
        f.write(enc(i))
    time.sleep(float(interval))
"""


def start_appender(path, count, interval=0.004):
    return subprocess.Popen(
        [sys.executable, "-c", APPENDER_RUNNER, path,
         str(count), str(interval)],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )


# ---------------------------------------------------------------------------
# Basic behavior
# ---------------------------------------------------------------------------


class TestLinkedBasic(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        d = self.tmp.name
        self.orders = os.path.join(d, "orders.log")
        self.details = os.path.join(d, "details.log")
        with open(self.orders, "wb") as f:
            f.write(v1("o1") + v2("o2"))
        with open(self.details, "wb") as f:
            f.write(v1("o1") + v2("o2"))
        self.groups = [[self.orders], [self.details]]
        self.refs = [(1, 0)]

    def tearDown(self):
        self.tmp.cleanup()

    def test_cross_group_references_migrate_together(self):
        result = migrate_linked_groups(self.groups, self.refs,
                                       quiesce=0.01)
        self.assertTrue(result.replaced)
        self.assertEqual(result.records_migrated, 4)
        self.assertEqual(result.records_skipped, 0)
        self.assertEqual(result.references_bad, 0)
        blob = read_bytes(self.orders) + read_bytes(self.details)
        self.assertTrue(all(v == CURRENT_VERSION for v in versions_of(blob)))
        # Plain codec shape: references are external, no extra field.
        self.assertEqual(ids_of(read_bytes(self.details)), ["o1", "o2"])
        self.assertEqual(result.groups,
                         tuple(tuple(g) for g in self.groups))
        self.assertEqual(result.refs, tuple(self.refs))

    def test_idempotent_rerun_changes_nothing(self):
        r1 = migrate_linked_groups(self.groups, self.refs, quiesce=0.01)
        snap = read_bytes(self.orders) + read_bytes(self.details)
        r2 = migrate_linked_groups(self.groups, self.refs, quiesce=0.01)
        self.assertFalse(r2.replaced)
        self.assertEqual(r2.records_migrated, 0)
        self.assertEqual(r2.records_skipped, 0)
        self.assertEqual(r2.references_bad, 0)
        self.assertEqual(read_bytes(self.orders)
                         + read_bytes(self.details), snap)
        self.assertTrue(r1.replaced)

    def test_no_declarations_just_migrates_all_groups(self):
        result = migrate_linked_groups(self.groups, [], quiesce=0.01)
        self.assertTrue(result.replaced)
        self.assertEqual(result.records_migrated, 4)
        self.assertEqual(result.references_bad, 0)

    def test_strict_rolls_every_group_back(self):
        originals = (read_bytes(self.orders), read_bytes(self.details))
        with open(self.details, "ab") as f:
            f.write(v1("ghost"))
        with self.assertRaises(LinkedBadReferenceError) as cm:
            migrate_linked_groups(self.groups, self.refs, quiesce=0.01,
                                  segment_size=128)
        self.assertEqual(cm.exception.path, self.details)
        self.assertEqual(cm.exception.lineno, 3)
        self.assertEqual(cm.exception.kind, "missing")
        self.assertEqual(cm.exception.target, "ghost")
        self.assertIsInstance(cm.exception, ValueError)
        self.assertEqual(read_bytes(self.orders), originals[0])
        self.assertEqual(read_bytes(self.details),
                         originals[1] + v1("ghost"))
        leftovers = [n for n in os.listdir(self.tmp.name)
                     if "migrate" in n and not n.endswith(".lock")]
        self.assertEqual(leftovers, [])


class TestTargetResolution(unittest.TestCase):
    """Earliest same-order-id record across members, then lines."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        d = self.tmp.name
        self.a = os.path.join(d, "a.log")
        self.b1 = os.path.join(d, "b1.log")
        self.b2 = os.path.join(d, "b2.log")

    def tearDown(self):
        self.tmp.cleanup()

    def test_target_searched_across_members_in_order(self):
        # x only lives in the second member of the target group.
        with open(self.a, "wb") as f:
            f.write(v1("x"))
        with open(self.b1, "wb") as f:
            f.write(v1("other"))
        with open(self.b2, "wb") as f:
            f.write(v1("x"))
        result = migrate_linked_groups([[self.a], [self.b1, self.b2]],
                                       [(0, 1)], quiesce=0.01)
        self.assertEqual(result.references_bad, 0)
        self.assertTrue(result.replaced)

    def test_earliest_bad_target_is_not_skipped_over_to_later_one(self):
        # The earliest matching target line is bad; a later good x in
        # another member must not become the target.
        with open(self.a, "wb") as f:
            f.write(v1("x"))
        with open(self.b1, "wb") as f:
            f.write(b'{"v":1,"order_id":"x","tags":[]}\n')  # bad line
        with open(self.b2, "wb") as f:
            f.write(v1("x"))  # good, but later
        with self.assertRaises(LinkedBadReferenceError) as cm:
            migrate_linked_groups([[self.a], [self.b1, self.b2]],
                                  [(0, 1)], quiesce=0.01)
        self.assertEqual(cm.exception.kind, "target-skipped")
        # In skip mode the source record is dropped as well.
        with open(self.a, "wb") as f:
            f.write(v1("x"))
        result = migrate_linked_groups(
            [[self.a], [self.b1, self.b2]], [(0, 1)],
            on_bad="skip", quiesce=0.01)
        self.assertEqual(os.path.getsize(self.a), 0)
        self.assertEqual(result.references_bad, 1)
        self.assertEqual(result.records_skipped, 1)  # the bad target


class TestBadReferenceKinds(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        d = self.tmp.name
        self.a = os.path.join(d, "a.log")
        self.b = os.path.join(d, "b.log")

    def tearDown(self):
        self.tmp.cleanup()

    def _groups(self):
        return [[self.a], [self.b]]

    def test_missing_target(self):
        with open(self.a, "wb") as f:
            f.write(v1("a"))
        with open(self.b, "wb") as f:
            f.write(v1("b"))
        with self.assertRaises(LinkedBadReferenceError) as cm:
            migrate_linked_groups(self._groups(), [(0, 1)], quiesce=0.01)
        self.assertEqual(cm.exception.kind, "missing")
        self.assertEqual(cm.exception.target, "a")

    def test_target_skipped(self):
        with open(self.a, "wb") as f:
            f.write(v1("x"))
        with open(self.b, "wb") as f:
            f.write(b'{"v":1,"order_id":"x","tags":[]}\n')
        with self.assertRaises(LinkedBadReferenceError) as cm:
            migrate_linked_groups(self._groups(), [(0, 1)], quiesce=0.01)
        self.assertEqual(cm.exception.kind, "target-skipped")

    def test_illegal_target_version_key(self):
        with open(self.a, "wb") as f:
            f.write(v1("x"))
        with open(self.b, "wb") as f:
            f.write(vbad("x"))
        with self.assertRaises(LinkedBadReferenceError) as cm:
            migrate_linked_groups(self._groups(), [(0, 1)], quiesce=0.01)
        self.assertEqual(cm.exception.kind, "illegal-target-version")

    def test_cycle_pair_self_loop_and_chain_back_to_source(self):
        # Two groups, mutual declarations: x -> x -> x.
        with open(self.a, "wb") as f:
            f.write(v1("x"))
        with open(self.b, "wb") as f:
            f.write(v1("x"))
        with self.assertRaises(LinkedBadReferenceError) as cm:
            migrate_linked_groups(self._groups(), [(0, 1), (1, 0)],
                                  quiesce=0.01)
        self.assertEqual(cm.exception.kind, "cycle")
        # A self declaration: the earliest same-id line is the record
        # itself, so it is a length-one self loop -- even a single line.
        with open(self.a, "wb") as f:
            f.write(v1("solo"))
        with self.assertRaises(LinkedBadReferenceError) as cm:
            migrate_linked_groups([[self.a]], [(0, 0)], quiesce=0.01)
        self.assertEqual(cm.exception.kind, "cycle")
        self.assertEqual(cm.exception.lineno, 1)
        # Two lines of one id: line 1 self-loops; line 2 reaches back
        # to line 1 whose chain returns to it.
        with open(self.a, "wb") as f:
            f.write(v1("twice") + v1("twice"))
        with self.assertRaises(LinkedBadReferenceError) as cm:
            migrate_linked_groups([[self.a]], [(0, 0)], quiesce=0.01)
        self.assertEqual(cm.exception.kind, "cycle")
        self.assertEqual(cm.exception.lineno, 1)

    def test_edge_level_cycle_other_declaration_stays_valid(self):
        # The first record loops via one declaration but its other
        # declaration resolves cleanly; only the looping edge is bad.
        c = os.path.join(self.tmp.name, "c.log")
        with open(self.a, "wb") as f:
            f.write(v1("x") + v1("ok"))
        with open(self.b, "wb") as f:
            f.write(v1("x"))
        with open(c, "wb") as f:
            f.write(v1("x") + v1("ok"))
        with self.assertRaises(LinkedBadReferenceError) as cm:
            migrate_linked_groups([[self.a], [self.b], [c]],
                                  [(0, 1), (1, 0)], quiesce=0.01)
        self.assertEqual(cm.exception.kind, "cycle")
        self.assertEqual(cm.exception.lineno, 1)

    def test_first_offense_bad_line_when_earlier(self):
        with open(self.a, "wb") as f:
            f.write(vbad("x"))
        with open(self.b, "wb") as f:
            f.write(v1("ghost"))
        with self.assertRaises(GroupBadRecordError) as cm:
            migrate_linked_groups(self._groups(), [(1, 0)], quiesce=0.01)
        self.assertEqual(cm.exception.path, self.a)
        self.assertEqual(cm.exception.lineno, 1)

    def test_first_offense_bad_ref_masks_later_bad_line(self):
        with open(self.a, "wb") as f:
            f.write(v1("x"))
        with open(self.b, "wb") as f:
            f.write(vbad("x"))
        with self.assertRaises(LinkedBadReferenceError) as cm:
            migrate_linked_groups(self._groups(), [(0, 1)], quiesce=0.01)
        self.assertEqual(cm.exception.kind, "illegal-target-version")


class TestSkipMode(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        d = self.tmp.name
        self.a = os.path.join(d, "a.log")
        self.b = os.path.join(d, "b.log")
        self.c = os.path.join(d, "c.log")

    def tearDown(self):
        self.tmp.cleanup()

    def test_drops_record_and_audits_file_lineno_snippet(self):
        with open(self.a, "wb") as f:
            f.write(v1("ghost") + v1("keep"))
        with open(self.b, "wb") as f:
            f.write(v1("keep"))
        buf = io.BytesIO()
        result = migrate_linked_groups([[self.a], [self.b]], [(0, 1)],
                                       on_bad="skip", quiesce=0.01,
                                       audit=buf)
        self.assertEqual(result.references_bad, 1)
        self.assertEqual(result.records_skipped, 0)
        self.assertEqual(ids_of(read_bytes(self.a)), ["keep"])
        audit = buf.getvalue().decode()
        self.assertEqual(len(audit.splitlines()), 1)
        self.assertTrue(audit.startswith(
            f"{self.a}:1:" + '{"v":1,"order_id":"ghost"'))

    def test_bad_line_and_bad_ref_counted_apart(self):
        with open(self.a, "wb") as f:
            f.write(v1("x"))
        with open(self.b, "wb") as f:
            f.write(b'{"v":1,"order_id":"x","tags":[]}\n' + v1("ok"))
        buf = io.BytesIO()
        result = migrate_linked_groups([[self.a], [self.b]], [(0, 1)],
                                       on_bad="skip", quiesce=0.01,
                                       audit=buf)
        self.assertEqual(result.records_skipped, 1)   # bad target line
        self.assertEqual(result.references_bad, 1)   # a -> x bad edge
        self.assertEqual(result.records_migrated, 1)  # only "ok"
        self.assertEqual(os.path.getsize(self.a), 0)
        lines = buf.getvalue().decode().splitlines()
        self.assertEqual(len(lines), 2)
        self.assertIn(f"{self.b}:1:", lines[0])
        self.assertIn(f"{self.a}:1:", lines[1])

    def test_transitive_dropping_across_groups(self):
        # a -> b, c -> a : b has no "g", so a's g record drops and then
        # the c record pointing at it drops transitively; the unrelated
        # ids exist in every group they point at and survive.
        with open(self.a, "wb") as f:
            f.write(v1("g") + v1("unrelated"))
        with open(self.b, "wb") as f:
            f.write(v1("unrelated"))
        with open(self.c, "wb") as f:
            f.write(v1("g") + v1("unrelated"))
        result = migrate_linked_groups(
            [[self.a], [self.b], [self.c]], [(0, 1), (2, 0)],
            on_bad="skip", quiesce=0.01)
        self.assertEqual(ids_of(read_bytes(self.a)), ["unrelated"])
        self.assertEqual(ids_of(read_bytes(self.c)), ["unrelated"])
        self.assertEqual(result.references_bad, 2)

    def test_numeric_edge_cases(self):
        with open(self.a, "wb") as f:
            f.write(b'{"v":1,"order_id":"ok","amount":-0.0,'
                    b'"tags":[]}\n')
            f.write(b'{"v":1,"order_id":"big","amount":1e999,'
                    b'"tags":[]}\n')
        result = migrate_linked_groups([[self.a]], [], on_bad="skip",
                                       quiesce=0.01)
        self.assertEqual(result.records_skipped, 1)
        self.assertEqual(result.references_bad, 0)
        self.assertEqual(result.records_migrated, 1)
        self.assertIn(b"-0.0", read_bytes(self.a))


# ---------------------------------------------------------------------------
# Validation / CLI
# ---------------------------------------------------------------------------


class TestLinkedValidation(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        d = self.tmp.name
        self.a = os.path.join(d, "a.log")
        self.b = os.path.join(d, "b.log")
        for p in (self.a, self.b):
            with open(p, "wb") as f:
                f.write(v1(os.path.basename(p)))

    def tearDown(self):
        self.tmp.cleanup()

    def test_groups_non_sequence_type_error(self):
        for bad in (self.a, 42, object(), "x"):
            with self.assertRaises(TypeError):
                migrate_linked_groups(bad, [])

    def test_refs_non_sequence_type_error(self):
        for bad in (42, "0:1", object()):
            with self.assertRaises(TypeError):
                migrate_linked_groups([[self.a], [self.b]], bad)

    def test_single_ref_pair_is_value_error_not_type_error(self):
        # A bare pair is a sequence, just not a sequence of pairs --
        # exactly like a flat member list for groups.
        with self.assertRaises(ValueError):
            migrate_linked_groups([[self.a], [self.b]], (0, 1))

    def test_empty_and_duplicate_value_error(self):
        with self.assertRaises(ValueError):
            migrate_linked_groups([], [])
        with self.assertRaises(ValueError):
            migrate_linked_groups([[]], [])
        with self.assertRaises(ValueError):
            migrate_linked_groups([[self.a], [self.a]], [])
        with self.assertRaises(ValueError):
            migrate_linked_groups([self.a, self.b], [])  # flat list

    def test_bad_reference_declarations(self):
        with self.assertRaises(ValueError):
            migrate_linked_groups([[self.a], [self.b]], [(0,)])
        with self.assertRaises(ValueError):
            migrate_linked_groups([[self.a], [self.b]], [(0, 9)])
        with self.assertRaises(ValueError):
            migrate_linked_groups([[self.a], [self.b]], [("0", 1)])
        with self.assertRaises(ValueError):
            migrate_linked_groups([[self.a], [self.b]],
                                  [(0, 1), (0, 1)])

    def test_missing_member_file_not_found(self):
        with self.assertRaises(FileNotFoundError):
            migrate_linked_groups(
                [[self.a], [os.path.join(self.tmp.name, "x")]], [])
        with self.assertRaises(FileNotFoundError):
            read_linked_logs(
                [[self.a], [os.path.join(self.tmp.name, "x")]])

    def test_cli_exit_codes(self):
        # Usage: duplicate members -> 2.
        proc = run_linked([[self.a, self.a]])
        self.assertEqual(proc.returncode, EXIT_USAGE, proc.stderr)
        # Usage: out-of-range --ref -> 2.
        proc = run_linked([[self.a], [self.b]], [(0, 9)])
        self.assertEqual(proc.returncode, EXIT_USAGE, proc.stderr)
        # Usage: malformed --ref is rejected by the entry point.
        proc = subprocess.run(
            [sys.executable, "-c", LINKED_RUNNER, "-", "-",
             "--group", self.a, "--ref", "x:y"],
            capture_output=True)
        self.assertEqual(proc.returncode, EXIT_USAGE)
        # Missing member -> 1.
        proc = run_linked([[self.a],
                           [os.path.join(self.tmp.name, "nope")]])
        self.assertEqual(proc.returncode, EXIT_ERROR, proc.stderr)
        # Strict bad reference -> 3.
        bad = os.path.join(self.tmp.name, "bad.log")
        with open(bad, "wb") as f:
            f.write(v1("ghost"))
        proc = run_linked([[self.a], [bad]], [(1, 0)])
        self.assertEqual(proc.returncode, EXIT_BAD_RECORD, proc.stderr)
        self.assertIn(b"bad reference", proc.stderr)
        # Success -> 0.
        proc = run_linked([[self.a, self.b]])
        self.assertEqual(proc.returncode, EXIT_OK, proc.stderr)
        self.assertIn(b"replaced=yes", proc.stdout)
        self.assertIn(b"groups=1", proc.stdout)

    def test_cli_no_groups_is_usage(self):
        proc = subprocess.run(
            [sys.executable, "-m", "proto_migrate",
             "migrate-linked-logs"],
            capture_output=True, cwd=REPO_ROOT)
        self.assertEqual(proc.returncode, EXIT_USAGE)

    def test_exports(self):
        self.assertIs(proto_migrate.migrate_linked_groups,
                      migrate_linked_groups)
        self.assertIs(proto_migrate.read_linked_logs, read_linked_logs)
        self.assertIs(proto_migrate.run_linked_cli, run_linked_cli)
        self.assertTrue(issubclass(LinkedBadReferenceError, ValueError))


# ---------------------------------------------------------------------------
# Snapshots
# ---------------------------------------------------------------------------


class TestLinkedSnapshot(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        d = self.tmp.name
        self.orders = os.path.join(d, "orders.log")
        self.details = os.path.join(d, "details.log")
        with open(self.orders, "wb") as f:
            f.write(v1("o1") + v2("o2"))
        with open(self.details, "wb") as f:
            f.write(v1("o1"))

    def tearDown(self):
        self.tmp.cleanup()

    def test_pre_raw_post_uniform(self):
        groups = [[self.orders], [self.details]]
        recs = read_linked_logs(groups)
        self.assertEqual([r["v"] for r in recs], [1, 2, 1])
        migrate_linked_groups(groups, [(1, 0)], quiesce=0.01)
        recs = read_linked_logs(groups)
        self.assertTrue(all(r["v"] == CURRENT_VERSION for r in recs))
        self.assertEqual([r["order_id"] for r in recs],
                         ["o1", "o2", "o1"])
        for r in recs:
            self.assertNotIn("ref_order_id", r)

    def test_torn_tail_excluded(self):
        with open(self.details, "ab") as f:
            f.write(v1("o2")[:-1])
        recs = read_linked_logs([[self.orders], [self.details]])
        self.assertEqual([r["order_id"] for r in recs],
                         ["o1", "o2", "o1"])

    def test_path_reopening_appender_never_mixes(self):
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
                    f.write(v1(f"post-{i}"))
                i += 1
                time.sleep(0.002)

        def reader():
            while not stop.is_set():
                recs = read_linked_logs(groups, quiesce=0.01)
                vs = {r["v"] for r in recs}
                if vs and not (vs <= {1, 2} or vs == {3}):
                    bad.append(vs)

        ta = threading.Thread(target=appender)
        tr = threading.Thread(target=reader)
        ta.start()
        tr.start()
        migrate_linked_groups(groups, [], quiesce=0.02)
        time.sleep(0.05)
        stop.set()
        ta.join(timeout=5)
        tr.join(timeout=5)
        self.assertEqual(bad, [])
        migrate_linked_groups(groups, [], quiesce=0.02)
        self.assertTrue(all(v == 3
                            for v in versions_of(read_bytes(self.orders))))


# ---------------------------------------------------------------------------
# Crash recovery
# ---------------------------------------------------------------------------


CRASH_POINTS = (
    "linked-lock", "linked-prepare", "linked-verify",
    "linked-assembled", "linked-stage", "linked-staged",
    "linked-marker", "linked-backups",
)


class TestLinkedCrashRecovery(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        d = self.tmp.name
        # group 0: orders; group 1: two detail members referencing them
        self.paths = [os.path.join(d, f"m{i}.log")
                      for i in range(3)]
        self.groups = [[self.paths[0]],
                       [self.paths[1], self.paths[2]]]
        self.refs = [(1, 0)]
        n_orders = 120
        with open(self.paths[0], "wb") as f:
            f.write(b"".join(v1(f"o{i}") for i in range(n_orders)))
        for p in self.paths[1:]:
            with open(p, "wb") as f:
                f.write(b"".join(
                    v1(f"o{i % n_orders}")
                    for i in range(60)))
        self.originals = [read_bytes(p) for p in self.paths]
        self.reference = ref_linked(
            [[self.originals[0]],
             [self.originals[1], self.originals[2]]],
            self.refs)

    def tearDown(self):
        self.tmp.cleanup()

    def _reset(self):
        for p, blob in zip(self.paths, self.originals):
            with open(p, "wb") as f:
                f.write(blob)

    def _clean_artifacts(self):
        for name in os.listdir(self.tmp.name):
            if name.endswith(".log"):
                continue
            target = os.path.join(self.tmp.name, name)
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
                proc = run_linked(self.groups, self.refs, crash=point,
                                  quiesce_ms="5", segment_size="256")
                self.assertEqual(proc.returncode, 1,
                                 (point, proc.stderr))
                proc = run_linked(self.groups, self.refs,
                                  quiesce_ms="5", segment_size="256")
                self.assertEqual(proc.returncode, 0,
                                 (point, proc.stderr))
                for p in self.paths:
                    self.assertEqual(
                        read_bytes(p), self.reference[os.path.basename(p)])
                leftovers = [
                    n for n in os.listdir(self.tmp.name)
                    if n.endswith((".migrate-linked-backup",
                                   ".migrate-linked-staged"))
                    or ".migrate-linked-tmp" in n
                ]
                self.assertEqual(leftovers, [])
                again = run_linked(self.groups, self.refs,
                                   quiesce_ms="5")
                self.assertEqual(again.returncode, 0, again.stderr)
                self.assertIn(b"replaced=no", again.stdout)
                self.assertIn(b"migrated=0", again.stdout)

    def test_prepared_members_not_rescanned_on_resume(self):
        proc = run_linked(self.groups, self.refs, crash="linked-stage",
                          quiesce_ms="5", segment_size="256")
        self.assertEqual(proc.returncode, 1, proc.stderr)
        calls = []
        orig_loads = lim.loads

        def counting_loads(raw):
            calls.append(raw)
            return orig_loads(raw)

        lim.loads = counting_loads
        try:
            result = migrate_linked_groups(
                self.groups, self.refs, quiesce=0.005,
                segment_size=256)
        finally:
            lim.loads = orig_loads
        self.assertEqual(calls, [])
        self.assertTrue(result.replaced)
        self.assertEqual(result.records_migrated, 0)
        for p in self.paths:
            self.assertEqual(
                read_bytes(p), self.reference[os.path.basename(p)])

    def test_kill_resume_kill_resume(self):
        for point in ("linked-prepare", "linked-stage",
                      "linked-marker"):
            proc = run_linked(self.groups, self.refs, crash=point,
                              quiesce_ms="5", segment_size="256")
            self.assertEqual(proc.returncode, 1, proc.stderr)
        proc = run_linked(self.groups, self.refs, quiesce_ms="5",
                          segment_size="256")
        self.assertEqual(proc.returncode, 0, proc.stderr)
        for p in self.paths:
            self.assertEqual(
                read_bytes(p), self.reference[os.path.basename(p)])


class TestLinkedSkipCrashRecovery(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        d = self.tmp.name
        self.a = os.path.join(d, "a.log")
        self.b = os.path.join(d, "b.log")
        self.groups = [[self.a], [self.b]]
        self.refs = [(0, 1)]

    def tearDown(self):
        self.tmp.cleanup()

    def test_skip_crash_recovery_empty_a_intact_b(self):
        def reset():
            with open(self.a, "wb") as f:
                f.write(b"".join(v1(f"ghost{i}") for i in range(100)))
            with open(self.b, "wb") as f:
                f.write(b"".join(v1(f"b{i}") for i in range(100)))
            for name in os.listdir(self.tmp.name):
                if name.endswith(".log"):
                    continue
                t = os.path.join(self.tmp.name, name)
                if os.path.isdir(t):
                    import shutil
                    shutil.rmtree(t)
                else:
                    os.remove(t)

        for point in ("linked-prepare", "linked-stage",
                      "linked-marker"):
            with self.subTest(point=point):
                reset()
                run_linked(self.groups, self.refs, crash=point,
                           on_bad="skip", quiesce_ms="5",
                           segment_size="256")
                proc = run_linked(self.groups, self.refs,
                                  on_bad="skip", quiesce_ms="5",
                                  segment_size="256")
                self.assertEqual(proc.returncode, 0, proc.stderr)
                self.assertEqual(os.path.getsize(self.a), 0)
                self.assertEqual(len(read_bytes(self.b).splitlines()),
                                 100)


class TestLinkedCrashWithAppenders(unittest.TestCase):
    BASE = 8000

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        d = self.tmp.name
        self.paths = [os.path.join(d, f"m{i}.log") for i in range(2)]
        for p in self.paths:
            with open(p, "wb") as f:
                for i in range(self.BASE):
                    f.write(v1(f"{os.path.basename(p)}-{i}"))
        self.groups = [[self.paths[0]], [self.paths[1]]]

    def tearDown(self):
        self.tmp.cleanup()

    def test_kill_then_resume_with_live_appenders_zero_loss(self):
        # No reference declarations: late-N ids have no target by
        # design; this exercises the streaming/resume machinery only.
        appenders = [start_appender(p, 30, interval=0.005)
                     for p in self.paths]
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
            run_linked(self.groups, quiesce_ms="10",
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
            base = os.path.basename(p)
            self.assertEqual(
                ids[:self.BASE],
                [f"{base}-{i}" for i in range(self.BASE)])
            self.assertEqual(ids[self.BASE:],
                             [f"late-{i}" for i in range(30)])


# ---------------------------------------------------------------------------
# Post-border faults and idempotency
# ---------------------------------------------------------------------------


class TestLinkedPostBorderFaults(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        d = self.tmp.name
        self.a = os.path.join(d, "a.log")
        self.b = os.path.join(d, "b.log")
        with open(self.a, "wb") as f:
            f.write(b"".join(v1(f"o{i}") for i in range(40)))
        with open(self.b, "wb") as f:
            f.write(b"".join(v1(f"o{i}") for i in range(40)))
        self.groups = [[self.a], [self.b]]
        self.refs = [(1, 0)]

    def tearDown(self):
        self.tmp.cleanup()

    def test_cleanup_fault_is_warning_still_success(self):
        proc = run_linked(self.groups, self.refs,
                          fault="linked-cleanup",
                          quiesce_ms="10", segment_size="256")
        self.assertEqual(proc.returncode, EXIT_OK, proc.stderr)
        self.assertIn(b"replaced=yes", proc.stdout)
        self.assertIn(b"warning:", proc.stderr)
        for p in (self.a, self.b):
            self.assertTrue(all(v == 3
                                for v in versions_of(read_bytes(p))))
        proc = run_linked(self.groups, self.refs, quiesce_ms="10")
        self.assertEqual(proc.returncode, EXIT_OK, proc.stderr)
        self.assertIn(b"replaced=no", proc.stdout)
        leftovers = [
            n for n in os.listdir(self.tmp.name)
            if ".migrate-linked-tmp" in n or n == ".migrate-linked-tmp"
        ]
        self.assertEqual(leftovers, [])

    def test_selftest_still_passes(self):
        proc = subprocess.run(
            [sys.executable, "-m", "proto_migrate", "--selftest"],
            capture_output=True, cwd=REPO_ROOT)
        self.assertEqual(proc.returncode, 0, proc.stderr)


if __name__ == "__main__":
    unittest.main()
