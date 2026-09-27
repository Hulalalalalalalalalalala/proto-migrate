"""Tests for incremental (delta) linked migration.

Covers:
  * first delta without a ledger deterministically falls back to a full
    migration and labels the reason; later deltas process only the
    appended suffix;
  * the four per-member reconciliation counters (lines added, records
    rewritten, records skipped, records dropped for bad references) and
    their equality with the summary of a full migration of the same
    appended input, and byte-for-byte equality of the final files;
  * ledger missing / corrupt / source-inode-changed / configuration
    changed fallback, each deterministic with a reason;
  * real ``os._exit`` crash injection at every delta protocol point
    with byte-identical recovery, summary replay (the recovery reports
    the uninterrupted summary) and an idle rerun that counts zero;
  * a continuously appending writer: zero loss, zero duplication,
    original order;
  * non-blocking leases (overlap mutually exclusive, disjoint groups
    parallel, ``MigrationLockedError``) and takeover;
  * strict-mode global first-error ordering (group/member/line; a bad
    line compared against a bad reference), the TypeError/ValueError/
    FileNotFoundError taxonomy and the CLI exit codes.
"""

import io
import json
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import unittest

import proto_migrate
from proto_migrate import CURRENT_VERSION, dumps
from proto_migrate import migrate_linked_delta, migrate_linked_logs
from proto_migrate.linked_migration import (
    LinkedBadReferenceError,
    MigrationLockedError,
)
from proto_migrate.group_migration import GroupBadRecordError
from proto_migrate.log_migration import (
    EXIT_BAD_RECORD,
    EXIT_ERROR,
    EXIT_OK,
    EXIT_USAGE,
    migrate_log_file,
)

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

LINK = [(1, 0, "order_id", "order_id")]


# ---------------------------------------------------------------------------
# Fixtures / drivers
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
    return [json.loads(line)["order_id"] for line in blob.splitlines()
            if line]


def versions_of(blob):
    return [json.loads(line)["v"] for line in blob.splitlines() if line]


DELTA_RUNNER = r"""
import os, sys
sys.path.insert(0, %r)
if sys.argv[1] != "-":
    os.environ["PROTO_MIGRATE_CRASH_AT"] = sys.argv[1]
if len(sys.argv) > 2 and sys.argv[2] != "-":
    os.environ["PROTO_MIGRATE_FAULT_AT"] = sys.argv[2]
from proto_migrate.delta_migration import run_delta_cli
raise SystemExit(run_delta_cli(sys.argv[3:]))
""" % REPO_ROOT


def delta_argv(groups, links=("--link", "1:0:order_id:order_id"),
               on_bad="skip", segment_size=str(16 * 1024 * 1024),
               quiesce_ms="10"):
    argv = []
    for group in groups:
        argv.append("--group")
        argv.extend(group)
    if links:
        argv.extend(links if isinstance(links, tuple) else
                    [a for spec in links for a in ("--link", spec)])
    argv += [
        f"--quiesce-ms={quiesce_ms}",
        f"--segment-size={segment_size}",
        "--skip" if on_bad == "skip" else "--strict",
    ]
    return argv


def run_delta_cli_subprocess(groups, crash="-", fault="-", **kw):
    return subprocess.run(
        [sys.executable, "-c", DELTA_RUNNER, crash, fault,
         *delta_argv(groups, **kw)],
        capture_output=True,
    )


APPENDER_RUNNER = r"""
import os, sys, time, json
path, count, interval, prefix, refprefix = sys.argv[1:6]
for i in range(int(count)):
    key = (refprefix + "%d" % i) if refprefix != "-" else (prefix + "%d" % i)
    line = (b'{"v":1,"order_id":' + json.dumps(key).encode()
            + b',"amount":4.0,"tags":[]}\n')
    with open(path, "ab") as f:
        f.write(line)
    time.sleep(float(interval))
"""


def start_appender(path, count, interval, prefix, refprefix="-"):
    return subprocess.Popen(
        [sys.executable, "-c", APPENDER_RUNNER, path, str(count),
         str(interval), prefix, refprefix],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )


def make_pair(d, n_orders=3, n_details=2, orders_builder=v1,
              details_blob=None):
    orders = os.path.join(d, "orders.log")
    details = os.path.join(d, "details.log")
    with open(orders, "wb") as f:
        f.write(b"".join(orders_builder(f"A{i}") for i in range(n_orders)))
    if details_blob is None:
        details_blob = b"".join(v1(f"A{i}") for i in range(n_details))
    with open(details, "wb") as f:
        f.write(details_blob)
    return [[orders], [details]]


# ---------------------------------------------------------------------------
# Basic incremental semantics and counters
# ---------------------------------------------------------------------------


class TestDeltaBasic(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        d = self.tmp.name
        self.orders = os.path.join(d, "orders.log")
        self.details = os.path.join(d, "details.log")
        with open(self.orders, "wb") as f:
            f.write(v1("A1") + v2("A2") + v1("A3"))
        with open(self.details, "wb") as f:
            f.write(v1("A1") + v1("A2"))
        self.groups = [[self.orders], [self.details]]

    def tearDown(self):
        self.tmp.cleanup()

    def test_first_run_without_ledger_falls_back_to_full(self):
        result = migrate_linked_delta(
            self.groups, links=LINK, on_bad="skip", quiesce=0.005,
            audit=io.BytesIO())
        self.assertTrue(result.fallback)
        self.assertIn("ledger missing", result.fallback_reason)
        self.assertTrue(result.replaced)
        self.assertEqual(result.records_added, 5)
        self.assertEqual(result.records_rewritten, 5)
        per = {m.path: m for m in result.members}
        self.assertEqual(per[self.orders].lines_added, 3)
        self.assertEqual(per[self.orders].records_rewritten, 3)
        self.assertEqual(per[self.details].lines_added, 2)
        self.assertTrue(all(v == CURRENT_VERSION
                            for b in (read_bytes(self.orders),
                                      read_bytes(self.details))
                            for v in versions_of(b)))

    def test_incremental_only_processes_appended_rows(self):
        migrate_linked_delta(
            self.groups, links=LINK, on_bad="skip", quiesce=0.005,
            audit=io.BytesIO())
        with open(self.orders, "ab") as f:
            f.write(v1("A4"))
        with open(self.details, "ab") as f:
            f.write(v1("A3") + v1("A4") + v1("GHOST"))
        result = migrate_linked_delta(
            self.groups, links=LINK, on_bad="skip", quiesce=0.005,
            audit=io.BytesIO())
        self.assertFalse(result.fallback)
        per = {m.path: m for m in result.members}
        self.assertEqual(per[self.orders].lines_added, 1)
        self.assertEqual(per[self.orders].records_rewritten, 1)
        self.assertEqual(per[self.details].lines_added, 3)
        self.assertEqual(per[self.details].records_rewritten, 2)
        self.assertEqual(per[self.details].records_dropped, 1)
        self.assertEqual(result.records_dropped, 1)
        self.assertEqual(result.references_bad, 1)
        self.assertEqual(ids_of(read_bytes(self.details)),
                         ["A1", "A2", "A3", "A4"])

    def test_idle_rerun_counts_zero_and_touches_nothing(self):
        migrate_linked_delta(
            self.groups, links=LINK, on_bad="skip", quiesce=0.005,
            audit=io.BytesIO())
        before = (read_bytes(self.orders), read_bytes(self.details))
        inos = (os.stat(self.orders).st_ino, os.stat(self.details).st_ino)
        result = migrate_linked_delta(
            self.groups, links=LINK, on_bad="skip", quiesce=0.005,
            audit=io.BytesIO())
        self.assertEqual(result.records_added, 0)
        self.assertEqual(result.records_rewritten, 0)
        self.assertEqual(result.records_skipped, 0)
        self.assertEqual(result.records_dropped, 0)
        self.assertFalse(result.replaced)
        self.assertEqual((read_bytes(self.orders),
                          read_bytes(self.details)), before)
        self.assertEqual((os.stat(self.orders).st_ino,
                          os.stat(self.details).st_ino), inos)

    def test_canonical_appended_rows_added_but_not_rewritten(self):
        groups = make_pair(self.tmp.name)
        with open(groups[0][0], "wb") as f:
            f.write(v3("A1"))
        with open(groups[1][0], "wb") as f:
            f.write(v3("A1"))
        migrate_linked_delta(groups, links=LINK, on_bad="skip",
                             quiesce=0.005, audit=io.BytesIO())
        with open(groups[0][0], "ab") as f:
            f.write(v3("A2") + v1("A3"))
        with open(groups[1][0], "ab") as f:
            f.write(v3("A2") + v1("A3"))
        result = migrate_linked_delta(groups, links=LINK, on_bad="skip",
                                      quiesce=0.005, audit=io.BytesIO())
        per = {m.path: m for m in result.members}
        self.assertEqual(per[groups[0][0]].lines_added, 2)
        self.assertEqual(per[groups[0][0]].records_rewritten, 1)

    def test_references_into_reconciled_prefix_resolve(self):
        groups = make_pair(self.tmp.name)
        with open(groups[0][0], "wb") as f:
            f.write(v3("A1") + v3("A2"))
        with open(groups[1][0], "wb") as f:
            f.write(v3("A1") + v3("A2"))
        migrate_linked_delta(groups, links=LINK, on_bad="skip",
                             quiesce=0.005, audit=io.BytesIO())
        # New detail rows reference OLD prefix keys A1/A2 (not rescanned).
        with open(groups[1][0], "ab") as f:
            f.write(v1("A1") + v1("A2"))
        result = migrate_linked_delta(groups, links=LINK, on_bad="skip",
                                      quiesce=0.005, audit=io.BytesIO())
        self.assertEqual(result.references_bad, 0)
        self.assertEqual(result.records_dropped, 0)

    def test_negative_zero_preserved(self):
        groups = make_pair(self.tmp.name)
        with open(groups[0][0], "wb") as f:
            f.write(v1("A1", amount=-0.0))
        with open(groups[1][0], "wb") as f:
            f.write(v1("A1", amount=-0.0))
        migrate_linked_delta(groups, links=LINK, on_bad="skip",
                             quiesce=0.005, audit=io.BytesIO())
        self.assertIn(b"-0.0", read_bytes(groups[0][0]))
        self.assertIn(b"-0.0", read_bytes(groups[1][0]))


# ---------------------------------------------------------------------------
# Equivalence with a full migration: counters and final bytes
# ---------------------------------------------------------------------------


class TestDeltaEquivalentToFull(unittest.TestCase):
    def _full_reference(self, orders_blob, details_blob):
        d = tempfile.mkdtemp()
        try:
            o = os.path.join(d, "orders.log")
            dt = os.path.join(d, "details.log")
            with open(o, "wb") as f:
                f.write(orders_blob)
            with open(dt, "wb") as f:
                f.write(details_blob)
            audit = io.BytesIO()
            migrate_linked_logs([[o], [dt]], links=LINK, on_bad="skip",
                                quiesce=0.005, audit=audit)
            return read_bytes(o), read_bytes(dt), audit.getvalue()
        finally:
            shutil.rmtree(d)

    def test_second_delta_byte_identical_to_full_migration(self):
        d = tempfile.TemporaryDirectory()
        try:
            base_o = v3("A1") + v3("A2")
            base_d = v3("A1") + v3("A2")
            o = os.path.join(d.name, "orders.log")
            dt = os.path.join(d.name, "details.log")
            with open(o, "wb") as f:
                f.write(base_o)
            with open(dt, "wb") as f:
                f.write(base_d)
            groups = [[o], [dt]]
            migrate_linked_delta(groups, links=LINK, on_bad="skip",
                                 quiesce=0.005, audit=io.BytesIO())
            # Appended segment: a valid ref, an old-format row, a ghost.
            add_o = v1("A3") + v2("A4")
            add_d = v1("A3") + v1("A4") + v1("GHOST")
            with open(o, "ab") as f:
                f.write(add_o)
            with open(dt, "ab") as f:
                f.write(add_d)
            audit = io.BytesIO()
            result = migrate_linked_delta(groups, links=LINK,
                                          on_bad="skip", quiesce=0.005,
                                          audit=audit)
            got = (read_bytes(o), read_bytes(dt))
            ref_o, ref_d, _ref_audit = self._full_reference(
                base_o + add_o, base_d + add_d)
            self.assertEqual(got, (ref_o, ref_d))
            # Four counters describe exactly the appended segment:
            # orders A3(v1),A4(v2) and details A3(v1),A4(v1) are the
            # four surviving old rows (GHOST is dropped, not rewritten).
            self.assertEqual(result.records_added, 5)
            self.assertEqual(result.records_rewritten, 4)
            self.assertEqual(result.records_dropped, 1)
            self.assertEqual(result.references_bad, 1)
            # The dropped GHOST (details line 5) is audited with its
            # global source line and the 32-byte raw snippet.
            audit_text = audit.getvalue().decode()
            self.assertIn(f"{dt}:5:", audit_text)
            self.assertIn('{"v":1,"order_id":"GHOST"', audit_text)
        finally:
            d.cleanup()


# ---------------------------------------------------------------------------
# Deterministic ledger fallback
# ---------------------------------------------------------------------------


class TestDeltaLedgerFallback(unittest.TestCase):
    def _settled(self, d):
        groups = make_pair(d)
        with open(groups[0][0], "wb") as f:
            f.write(v3("A1") + v3("A2"))
        with open(groups[1][0], "wb") as f:
            f.write(v3("A1") + v3("A2"))
        migrate_linked_delta(groups, links=LINK, on_bad="skip",
                             quiesce=0.005, audit=io.BytesIO())
        with open(groups[0][0], "ab") as f:
            f.write(v1("A3"))
        with open(groups[1][0], "ab") as f:
            f.write(v1("A3"))
        return groups

    def test_missing_ledger_full_fallback_with_reason(self):
        d = tempfile.TemporaryDirectory()
        try:
            groups = self._settled(d.name)
            os.remove(groups[0][0] + ".migrate-delta-ledger")
            result = migrate_linked_delta(groups, links=LINK,
                                          on_bad="skip", quiesce=0.005,
                                          audit=io.BytesIO())
            self.assertTrue(result.fallback)
            self.assertIn("ledger missing", result.fallback_reason)
            self.assertEqual(result.records_added, 6)
            self.assertTrue(all(v == CURRENT_VERSION
                                for g in groups
                                for v in versions_of(read_bytes(g[0]))))
        finally:
            d.cleanup()

    def test_corrupt_ledger_full_fallback_with_reason(self):
        d = tempfile.TemporaryDirectory()
        try:
            groups = self._settled(d.name)
            with open(groups[1][0] + ".migrate-delta-ledger", "wb") as f:
                f.write(b"{not valid json")
            result = migrate_linked_delta(groups, links=LINK,
                                          on_bad="skip", quiesce=0.005,
                                          audit=io.BytesIO())
            self.assertTrue(result.fallback)
            self.assertIn("ledger corrupt", result.fallback_reason)
            self.assertEqual(result.records_added, 6)
        finally:
            d.cleanup()

    def test_source_inode_change_full_fallback_with_reason(self):
        d = tempfile.TemporaryDirectory()
        try:
            groups = make_pair(d.name)
            with open(groups[0][0], "wb") as f:
                f.write(v3("A1") + v3("A2"))
            with open(groups[1][0], "wb") as f:
                f.write(v3("A1") + v3("A2"))
            migrate_linked_delta(groups, links=LINK, on_bad="skip",
                                 quiesce=0.005, audit=io.BytesIO())
            with open(groups[0][0], "ab") as f:
                f.write(v1("A3"))
            with open(groups[1][0], "ab") as f:
                f.write(v1("A3"))
            # A full linked migration renames to a new inode; the delta
            # ledger still pins the pre-rename inode.
            migrate_linked_logs(groups, links=LINK, on_bad="skip",
                                quiesce=0.005, audit=io.BytesIO())
            with open(groups[0][0], "ab") as f:
                f.write(v1("A4"))
            with open(groups[1][0], "ab") as f:
                f.write(v1("A4"))
            result = migrate_linked_delta(groups, links=LINK,
                                          on_bad="skip", quiesce=0.005,
                                          audit=io.BytesIO())
            self.assertTrue(result.fallback)
            self.assertIn("source inode changed",
                          result.fallback_reason)
            self.assertEqual(ids_of(read_bytes(groups[0][0])),
                             ["A1", "A2", "A3", "A4"])
            # The rebuilt ledger is now usable: next append incremental.
            with open(groups[0][0], "ab") as f:
                f.write(v1("A5"))
            with open(groups[1][0], "ab") as f:
                f.write(v1("A5"))
            again = migrate_linked_delta(groups, links=LINK,
                                         on_bad="skip", quiesce=0.005,
                                         audit=io.BytesIO())
            self.assertFalse(again.fallback)
            self.assertEqual(again.records_added, 2)
        finally:
            d.cleanup()

    def test_configuration_change_full_fallback_with_reason(self):
        d = tempfile.TemporaryDirectory()
        try:
            groups = self._settled(d.name)
            result = migrate_linked_delta(groups, links=[],
                                          on_bad="skip", quiesce=0.005,
                                          audit=io.BytesIO())
            self.assertTrue(result.fallback)
            self.assertIn("configuration changed",
                          result.fallback_reason)
        finally:
            d.cleanup()


# ---------------------------------------------------------------------------
# Strict mode and the global first-error ordering
# ---------------------------------------------------------------------------


class TestDeltaStrict(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        d = self.tmp.name
        self.orders = os.path.join(d, "orders.log")
        self.details = os.path.join(d, "details.log")
        with open(self.orders, "wb") as f:
            f.write(v3("A1"))
        with open(self.details, "wb") as f:
            f.write(v3("A1"))
        self.groups = [[self.orders], [self.details]]
        migrate_linked_delta(self.groups, links=LINK, on_bad="skip",
                             quiesce=0.005, audit=io.BytesIO())

    def tearDown(self):
        self.tmp.cleanup()

    def test_bad_reference_raises_value_error_and_touches_nothing(self):
        with open(self.details, "ab") as f:
            f.write(v1("GHOST"))
        snap = (read_bytes(self.orders), read_bytes(self.details))
        with self.assertRaises(LinkedBadReferenceError) as cm:
            migrate_linked_delta(self.groups, links=LINK, quiesce=0.005)
        self.assertIsInstance(cm.exception, ValueError)
        self.assertEqual(cm.exception.path, self.details)
        self.assertEqual(cm.exception.lineno, 2)
        self.assertEqual((read_bytes(self.orders),
                          read_bytes(self.details)), snap)
        leftovers = [n for n in os.listdir(self.tmp.name)
                     if "delta-tmp" in n]
        self.assertEqual(leftovers, [])

    def test_bad_line_beats_later_bad_reference(self):
        with open(self.orders, "ab") as f:
            f.write(b"BROKEN\n")
        with open(self.details, "ab") as f:
            f.write(v1("NOPE"))
        with self.assertRaises(GroupBadRecordError) as cm:
            migrate_linked_delta(self.groups, links=LINK, quiesce=0.005)
        self.assertEqual(cm.exception.path, self.orders)
        self.assertEqual(cm.exception.lineno, 2)

    def test_illegal_amount_is_a_bad_record(self):
        with open(self.orders, "ab") as f:
            f.write(b'{"v":1,"order_id":"B","amount":NaN,"tags":[]}\n')
        with open(self.details, "ab") as f:
            f.write(v1("B"))
        with self.assertRaises(GroupBadRecordError):
            migrate_linked_delta(self.groups, links=LINK, quiesce=0.005)


# ---------------------------------------------------------------------------
# Validation taxonomy and CLI exit codes
# ---------------------------------------------------------------------------


class TestDeltaValidationAndCli(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        d = self.tmp.name
        self.p1 = os.path.join(d, "a.log")
        self.p2 = os.path.join(d, "b.log")
        with open(self.p1, "wb") as f:
            f.write(v1("a"))
        with open(self.p2, "wb") as f:
            f.write(v1("a"))

    def tearDown(self):
        self.tmp.cleanup()

    def test_type_error_for_non_sequence(self):
        for bad in (self.p1, 42, object()):
            with self.assertRaises(TypeError):
                migrate_linked_delta(bad)
        with self.assertRaises(TypeError):
            migrate_linked_delta([self.p1])

    def test_value_error_for_empty_or_duplicate(self):
        with self.assertRaises(ValueError):
            migrate_linked_delta([])
        with self.assertRaises(ValueError):
            migrate_linked_delta([[]])
        with self.assertRaises(ValueError):
            migrate_linked_delta([[self.p1], [self.p1]])
        with self.assertRaises(ValueError):
            migrate_linked_delta([[self.p1, self.p1]])

    def test_missing_member_is_file_not_found(self):
        missing = os.path.join(self.tmp.name, "x")
        with self.assertRaises(FileNotFoundError):
            migrate_linked_delta([[self.p1], [missing]])

    def test_cli_exit_codes(self):
        # Usage: duplicate -> 2.
        proc = run_delta_cli_subprocess([[self.p1], [self.p1]])
        self.assertEqual(proc.returncode, EXIT_USAGE, proc.stderr)
        # Missing member -> 1.
        proc = run_delta_cli_subprocess(
            [[self.p1], [os.path.join(self.tmp.name, "x")]])
        self.assertEqual(proc.returncode, EXIT_ERROR, proc.stderr)
        # Strict bad reference -> 3 (p2 references a missing key).
        with open(self.p2, "wb") as f:
            f.write(v1("ghost"))
        proc = run_delta_cli_subprocess([[self.p1], [self.p2]],
                                        on_bad="strict")
        self.assertEqual(proc.returncode, EXIT_BAD_RECORD, proc.stderr)
        # Skip success -> 0 with fallback label.
        with open(self.p2, "wb") as f:
            f.write(v1("a"))
        proc = run_delta_cli_subprocess([[self.p1], [self.p2]])
        self.assertEqual(proc.returncode, EXIT_OK, proc.stderr)
        self.assertIn(b"added=2", proc.stdout)
        self.assertIn(b"fallback=yes", proc.stdout)

    def test_exports(self):
        self.assertIs(proto_migrate.migrate_linked_delta,
                      migrate_linked_delta)


# ---------------------------------------------------------------------------
# Crash injection, durable summary replay and idempotent recovery
# ---------------------------------------------------------------------------


CRASH_POINTS = (
    "delta-lock", "delta-prepare", "delta-refs", "delta-filter",
    "delta-stage", "delta-staged", "delta-marker", "delta-backups",
    "delta-cleanup", "delta-ledger",
)


class TestDeltaCrashRecovery(unittest.TestCase):
    BASE = 60
    EXTRA_O = 10
    EXTRA_D = 5

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        d = self.tmp.name
        self.orders = os.path.join(d, "orders.log")
        self.details = os.path.join(d, "details.log")
        with open(self.orders, "wb") as f:
            f.write(b"".join(v1(f"O{i}") for i in range(self.BASE)))
        with open(self.details, "wb") as f:
            f.write(b"".join(v1(f"O{i}") for i in range(self.BASE)))
        self.groups = [[self.orders], [self.details]]
        # Establish the ledger with a clean first delta.
        proc = run_delta_cli_subprocess(self.groups, segment_size="200",
                                        quiesce_ms="5")
        self.assertEqual(proc.returncode, 0, proc.stderr)
        with open(self.orders, "ab") as f:
            f.write(b"".join(v1(f"N{i}") for i in range(self.EXTRA_O)))
        with open(self.details, "ab") as f:
            # Reference existing base keys: never dangle.
            f.write(b"".join(v1(f"O{i}") for i in range(self.EXTRA_D)))

    def tearDown(self):
        self.tmp.cleanup()

    def _reset_to_appended(self):
        """Rewrite base + appended rows and clear all delta state."""
        for name in list(os.listdir(self.tmp.name)):
            if name in ("orders.log", "details.log"):
                continue
            target = os.path.join(self.tmp.name, name)
            if os.path.isdir(target):
                shutil.rmtree(target, ignore_errors=True)
            else:
                os.remove(target)
        with open(self.orders, "wb") as f:
            f.write(b"".join(v1(f"O{i}") for i in range(self.BASE)))
        with open(self.details, "wb") as f:
            f.write(b"".join(v1(f"O{i}") for i in range(self.BASE)))
        proc = run_delta_cli_subprocess(self.groups, segment_size="200",
                                        quiesce_ms="5")
        assert proc.returncode == 0, proc.stderr
        with open(self.orders, "ab") as f:
            f.write(b"".join(v1(f"N{i}") for i in range(self.EXTRA_O)))
        with open(self.details, "ab") as f:
            f.write(b"".join(v1(f"O{i}") for i in range(self.EXTRA_D)))

    def _recover(self):
        return run_delta_cli_subprocess(self.groups, segment_size="200",
                                        quiesce_ms="5")

    def _assert_final(self):
        o = [json.loads(x) for x in read_bytes(self.orders).splitlines()]
        d = [json.loads(x) for x in read_bytes(self.details).splitlines()]
        self.assertEqual(len(o), self.BASE + self.EXTRA_O)
        self.assertEqual(len(d), self.BASE + self.EXTRA_D)
        self.assertTrue(all(r["v"] == CURRENT_VERSION for r in o + d))
        self.assertEqual([r["order_id"] for r in o][self.BASE:],
                         [f"N{i}" for i in range(self.EXTRA_O)])
        self.assertEqual([r["order_id"] for r in d][self.BASE:],
                         [f"O{i}" for i in range(self.EXTRA_D)])

    def test_every_crash_point_recovers_byte_identical(self):
        for point in CRASH_POINTS:
            with self.subTest(point=point):
                self._reset_to_appended()
                proc = run_delta_cli_subprocess(
                    self.groups, crash=point, segment_size="200",
                    quiesce_ms="5")
                self.assertEqual(proc.returncode, 1,
                                 (point, proc.stderr))
                proc = self._recover()
                self.assertEqual(proc.returncode, 0,
                                 (point, proc.stderr))
                self._assert_final()
                leftovers = [
                    n for n in os.listdir(self.tmp.name)
                    if "delta-tmp" in n]
                self.assertEqual(leftovers, [])
                # A third, idle run reports zero and rewrites nothing.
                idle = self._recover()
                self.assertIn(b"added=0", idle.stdout)
                self.assertIn(b"rewritten=0", idle.stdout)

    def test_recovery_summary_matches_uninterrupted(self):
        self._reset_to_appended()
        ref = self._recover()
        self.assertEqual(ref.returncode, 0)
        ref_first = ref.stdout.splitlines()[0]
        for point in ("delta-prepare", "delta-stage", "delta-marker",
                      "delta-ledger"):
            with self.subTest(point=point):
                self._reset_to_appended()
                proc = run_delta_cli_subprocess(
                    self.groups, crash=point, segment_size="200",
                    quiesce_ms="5")
                self.assertEqual(proc.returncode, 1)
                rec = self._recover()
                self.assertEqual(
                    rec.stdout.splitlines()[0], ref_first,
                    (point, rec.stdout, ref_first))


# ---------------------------------------------------------------------------
# A continuously appending writer
# ---------------------------------------------------------------------------


class TestDeltaWithLiveAppender(unittest.TestCase):
    BASE = 400
    EXTRA = 40

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        d = self.tmp.name
        self.orders = os.path.join(d, "orders.log")
        self.details = os.path.join(d, "details.log")
        with open(self.orders, "wb") as f:
            f.write(b"".join(v1(f"B{i}") for i in range(self.BASE)))
            # Pre-existing keys the detail appender will reference, so a
            # detail reference can never dangle however appends interleave.
            f.write(b"".join(v1(f"T{i}") for i in range(self.EXTRA)))
        with open(self.details, "wb") as f:
            f.write(b"".join(v1(f"B{i}") for i in range(self.BASE)))
        self.groups = [[self.orders], [self.details]]
        run_delta_cli_subprocess(self.groups, segment_size="4096")

    def tearDown(self):
        self.tmp.cleanup()

    def test_appender_zero_loss_zero_dup_in_order(self):
        appenders = [
            start_appender(self.orders, self.EXTRA, 0.002, "OL"),
            # Details reference the pre-existing Tk keys so they can
            # never dangle regardless of append/prepare interleaving.
            start_appender(self.details, self.EXTRA, 0.002, "DL",
                           refprefix="T"),
        ]
        proc = run_delta_cli_subprocess(self.groups, segment_size="4096")
        self.assertEqual(proc.returncode, 0, proc.stderr)
        for app in appenders:
            app.wait(timeout=20)
        for _ in range(20):
            run_delta_cli_subprocess(self.groups, segment_size="4096")
            time.sleep(0.02)
        orders = [json.loads(x)
                  for x in read_bytes(self.orders).splitlines()]
        details = [json.loads(x)
                   for x in read_bytes(self.details).splitlines()]
        self.assertEqual(len(orders), self.BASE + 2 * self.EXTRA)
        self.assertEqual(len(details), self.BASE + self.EXTRA)
        self.assertTrue(all(r["v"] == CURRENT_VERSION
                            for r in orders + details))
        oids = [r["order_id"] for r in orders]
        dids = [r["order_id"] for r in details]
        self.assertEqual(len(oids), len(set(oids)))
        self.assertEqual(len(dids), len(set(dids)))
        self.assertEqual(oids[:self.BASE],
                         [f"B{i}" for i in range(self.BASE)])
        self.assertEqual(oids[self.BASE:self.BASE + self.EXTRA],
                         [f"T{i}" for i in range(self.EXTRA)])
        self.assertEqual(oids[self.BASE + self.EXTRA:],
                         [f"OL{i}" for i in range(self.EXTRA)])
        self.assertEqual(dids[:self.BASE],
                         [f"B{i}" for i in range(self.BASE)])
        self.assertEqual(dids[self.BASE:],
                         [f"T{i}" for i in range(self.EXTRA)])


# ---------------------------------------------------------------------------
# Multi-instance leases
# ---------------------------------------------------------------------------


class TestDeltaLeases(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        d = self.tmp.name
        self.paths = []
        for i in range(4):
            p = os.path.join(d, f"f{i}.log")
            with open(p, "wb") as f:
                f.write(v3("K"))
            self.paths.append(p)
        for g in ([[self.paths[0]], [self.paths[1]]],
                  [[self.paths[2]], [self.paths[3]]],
                  [[self.paths[1]], [self.paths[2]]]):
            migrate_linked_delta(g, links=[], on_bad="skip",
                                 quiesce=0.005, audit=io.BytesIO())
        for p in self.paths:
            with open(p, "ab") as f:
                f.write(b"".join(v1(f"X{i}") for i in range(300)))

    def tearDown(self):
        self.tmp.cleanup()

    def test_overlap_is_exclusive_disjoint_runs_parallel(self):
        outcomes = []

        def run(groups, tag):
            try:
                migrate_linked_delta(groups, links=[], on_bad="skip",
                                     quiesce=0.02, segment_size=4096,
                                     audit=io.BytesIO())
                outcomes.append((tag, "ok"))
            except MigrationLockedError:
                outcomes.append((tag, "locked"))

        g_overlap = [[self.paths[1]], [self.paths[2]]]
        t1 = threading.Thread(
            target=run, args=([[self.paths[0]], [self.paths[1]]], "a"))
        t2 = threading.Thread(target=run, args=(g_overlap, "overlap"))
        t1.start()
        time.sleep(0.05)
        t2.start()
        t1.join()
        t2.join()
        self.assertIn(("overlap", "locked"), outcomes)

        outcomes.clear()
        t1 = threading.Thread(
            target=run, args=([[self.paths[0]], [self.paths[1]]], "a"))
        t2 = threading.Thread(
            target=run,
            args=([[self.paths[2]], [self.paths[3]]], "b"))
        t1.start()
        t2.start()
        t1.join()
        t2.join()
        self.assertEqual(sorted(outcomes), [("a", "ok"), ("b", "ok")])

    def test_held_lease_raises_locked(self):
        import fcntl
        fh = open(self.paths[0] + ".migrate.lock", "a+b")
        fcntl.flock(fh, fcntl.LOCK_EX)
        try:
            with self.assertRaises(MigrationLockedError):
                migrate_linked_delta(
                    [[self.paths[0]]], links=[], on_bad="skip",
                    quiesce=0.005, audit=io.BytesIO())
        finally:
            fcntl.flock(fh, fcntl.LOCK_UN)
            fh.close()


# ---------------------------------------------------------------------------
# The single-file idempotent-counter fix
# ---------------------------------------------------------------------------


class TestSingleFileIdempotentCounterFix(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmp.name, "x.log")

    def tearDown(self):
        self.tmp.cleanup()

    def test_canonical_file_reports_zero(self):
        with open(self.path, "wb") as f:
            f.write(v3("A") + v3("B"))
        result = migrate_log_file(self.path, quiesce=0.005)
        self.assertFalse(result.replaced)
        self.assertEqual(result.records_migrated, 0)
        self.assertEqual(result.records_skipped, 0)

    def test_second_run_after_real_migration_reports_zero(self):
        with open(self.path, "wb") as f:
            f.write(v1("A") + v1("B") + v1("C"))
        first = migrate_log_file(self.path, quiesce=0.005)
        self.assertEqual(first.records_migrated, 3)
        second = migrate_log_file(self.path, quiesce=0.005)
        self.assertFalse(second.replaced)
        self.assertEqual(second.records_migrated, 0)
        self.assertEqual(second.records_skipped, 0)


if __name__ == "__main__":
    unittest.main()
