"""Tests for the read-only rehearsal and the online compaction.

Covers:
  * ``rehearse_linked_logs`` -- per-member rewrite counts, predicted
    bytes, globally ordered bad-record/bad-reference locations, strict
    first-error attribution and the exact skip-mode discard list all
    matching the real migration; strict read-only behaviour (no file is
    created or modified, no lease is taken, a live migrator is
    undisturbed, no ``MigrationLockedError``); pinned locations that do
    not drift under a live appender; validation/CLI parity;
  * ``compact_linked_logs`` -- deterministic compact form (one merged
    segment, one checkpoint record, no line index), idempotency,
    byte-identical migration afterwards with zero rescans and unchanged
    counters, crash recovery at every crash point, strict-mode
    first-error preservation, ``MigrationLockedError`` on a held lease;
  * streaming-session descriptor reclaim after a cursor is abandoned;
  * codec contract additions: ``1e999`` overflow classification,
    ``-0.0`` preservation and the per-entry exit codes.
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
import proto_migrate.log_migration as lm
from proto_migrate import (
    CURRENT_VERSION,
    MigrationLockedError,
    compact_linked_logs,
    dumps,
    migrate_linked_logs,
    read_linked_logs_stream,
    rehearse_linked_logs,
)
from proto_migrate.group_migration import GroupBadRecordError
from proto_migrate.linked_migration import (
    LinkedBadReferenceError,
    _STREAM_SESSIONS,
    close_linked_logs_stream,
)
from proto_migrate.log_migration import EXIT_BAD_RECORD, EXIT_ERROR, EXIT_OK
from proto_migrate.rehearsal import run_rehearsal_cli
from proto_migrate.compaction import run_compact_cli

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

LINK = [(1, 0, "order_id", "order_id")]


def v1(order_id, amount=1.0):
    return (
        b'{"v":1,"order_id":'
        + json.dumps(order_id).encode()
        + b',"amount":'
        + repr(amount).encode()
        + b',"tags":[]}\n'
    )


def v3(order_id, amount=3.0):
    return dumps({
        "order_id": order_id,
        "amount": amount,
        "status": "paid",
        "note": "",
        "updated_at": 0,
    })


def read_bytes(path):
    with open(path, "rb") as f:
        return f.read()


def write_pair(d, orders_blob, details_blob, names=("orders.log",
                                                     "details.log")):
    orders = os.path.join(d, names[0])
    details = os.path.join(d, names[1])
    with open(orders, "wb") as f:
        f.write(orders_blob)
    with open(details, "wb") as f:
        f.write(details_blob)
    return orders, details


def cli(argv, runner):
    return subprocess.run([sys.executable, "-c", runner, *argv],
                          capture_output=True)


REHEARSAL_RUNNER = r"""
import sys
sys.path.insert(0, %r)
from proto_migrate.rehearsal import run_rehearsal_cli
raise SystemExit(run_rehearsal_cli(sys.argv[1:]))
""" % REPO_ROOT

COMPACT_RUNNER = r"""
import os, sys
sys.path.insert(0, %r)
if sys.argv[1] != "-":
    os.environ["PROTO_MIGRATE_CRASH_AT"] = sys.argv[1]
from proto_migrate.compaction import run_compact_cli
rest = [a for a in sys.argv[2:] if not a.startswith("@@")]
raise SystemExit(run_compact_cli(rest))
""" % REPO_ROOT


def linked_argv(orders, details, on_bad, extra=()):
    return [
        "--group", orders, "--group", details,
        "--link", "1:0:order_id:order_id",
        "--skip" if on_bad == "skip" else "--strict",
        "--quiesce-ms=5", *extra,
    ]


# ---------------------------------------------------------------------------
# Rehearsal: agreement with the real migration
# ---------------------------------------------------------------------------


class TestRehearsalAgreement(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        d = self.tmp.name
        self.orders, self.details = write_pair(
            d, v1("A1") + v1("A2") + v1("X"),
            v1("A1") + v1("GHOST") + v1("A2") + b"BROKEN\n" + v1("X"))
        self.groups = [[self.orders], [self.details]]

    def tearDown(self):
        self.tmp.cleanup()

    def _real(self, on_bad):
        d2 = tempfile.mkdtemp()
        o = os.path.join(d2, "orders.log")
        x = os.path.join(d2, "details.log")
        shutil.copy(self.orders, o)
        shutil.copy(self.details, x)
        buf = io.BytesIO()
        result = migrate_linked_logs(
            [[o], [x]], links=LINK, on_bad=on_bad, quiesce=0.005,
            audit=buf)
        # Normalize the differing scratch directories out of the audit
        # entries (the audit names the member path).
        audit = buf.getvalue().replace(d2.encode(), b"DIR")
        return result, audit, read_bytes(o), read_bytes(x)

    def test_skip_report_matches_real_run(self):
        report = rehearse_linked_logs(self.groups, links=LINK,
                                      on_bad="skip", quiesce=0.005)
        result, audit, real_o, real_d = self._real("skip")
        by_name = {os.path.basename(m.path): m for m in report.members}
        self.assertEqual(by_name["orders.log"].records_migrated, 3)
        self.assertEqual(by_name["orders.log"].records_skipped, 0)
        self.assertEqual(by_name["details.log"].records_migrated, 3)
        self.assertEqual(by_name["details.log"].records_skipped, 1)
        self.assertEqual(by_name["details.log"].records_dropped, 1)
        self.assertEqual(result.records_migrated,
                         sum(m.records_migrated for m in report.members))
        self.assertEqual(result.records_skipped,
                         sum(m.records_skipped for m in report.members))
        self.assertEqual(result.references_bad, report.references_bad)
        self.assertEqual(report.references_bad, 1)
        # Exact audit bytes (modulo the scratch directory naming the
        # member path), in the real run's emission order.
        rehearsal_audit = b"".join(
            e.audit for e in report.discarded
        ).replace(self.tmp.name.encode(), b"DIR")
        self.assertEqual(rehearsal_audit, audit)
        # Predicted bytes equal the real migrated files.
        self.assertEqual(by_name["orders.log"].predicted_bytes, real_o)
        self.assertEqual(by_name["details.log"].predicted_bytes, real_d)

    def test_locations_are_group_member_line_ordered(self):
        report = rehearse_linked_logs(self.groups, links=LINK,
                                      on_bad="skip", quiesce=0.005)
        self.assertEqual(
            [(os.path.basename(e.path), e.lineno, e.kind)
             for e in report.bad_records],
            [("details.log", 4, "bad_record")])
        self.assertEqual(
            [(os.path.basename(e.path), e.lineno)
             for e in report.bad_references],
            [("details.log", 2)])
        kinds = [(os.path.basename(e.path), e.lineno, e.kind)
                 for e in report.discarded]
        # Bad records are audited first, then dropped records.
        self.assertEqual(kinds,
                         [("details.log", 4, "bad_record"),
                          ("details.log", 2, "dropped")])

    def test_strict_first_error_attribution_matches_real_run(self):
        report = rehearse_linked_logs(self.groups, links=LINK,
                                      on_bad="strict", quiesce=0.005)
        # The dangling reference (line 2) precedes the bad line (4).
        self.assertEqual(report.first_error_kind, "bad_reference")
        self.assertIsInstance(report.first_error, LinkedBadReferenceError)
        try:
            migrate_linked_logs(self.groups, links=LINK, quiesce=0.005)
        except LinkedBadReferenceError as exc:
            self.assertEqual(type(exc), type(report.first_error))
            self.assertEqual(str(exc), str(report.first_error))
            self.assertEqual(exc.path, self.details)
            self.assertEqual(exc.lineno, 2)
        else:
            self.fail("real run did not raise")

    def test_strict_bad_line_wins_when_it_sorts_first(self):
        d = self.tmp.name
        o = os.path.join(d, "o1.log")
        x = os.path.join(d, "x1.log")
        with open(o, "wb") as f:
            f.write(v1("A1") + b"BROKEN\n")
        with open(x, "wb") as f:
            f.write(v1("GHOST"))
        report = rehearse_linked_logs([[o], [x]], links=LINK,
                                      quiesce=0.005)
        self.assertEqual(report.first_error_kind, "bad_record")
        self.assertIsInstance(report.first_error, GroupBadRecordError)
        with self.assertRaises(GroupBadRecordError) as cm:
            migrate_linked_logs([[o], [x]], links=LINK, quiesce=0.005)
        self.assertEqual(str(cm.exception), str(report.first_error))
        self.assertEqual(cm.exception.path, o)
        self.assertEqual(cm.exception.lineno, 2)

    def test_four_bad_reference_classes_located(self):
        d = self.tmp.name
        g0 = os.path.join(d, "c0.log")
        g1 = os.path.join(d, "c1.log")
        g2 = os.path.join(d, "c2.log")
        with open(g0, "wb") as f:
            f.write(v1("ROOT"))
        with open(g1, "wb") as f:
            f.write(v1("ROOT") + v1("MID"))
        with open(g2, "wb") as f:
            f.write(v1("MID") + v1("GHOST"))
        links = [(1, 0, "order_id", "order_id"),
                 (2, 1, "order_id", "order_id")]
        report = rehearse_linked_logs([[g0], [g1], [g2]], links=links,
                                      on_bad="skip", quiesce=0.005)
        self.assertEqual(report.references_bad, 3)
        self.assertEqual(
            [(os.path.basename(e.path), e.lineno)
             for e in report.bad_references],
            [("c1.log", 2), ("c2.log", 1), ("c2.log", 2)])

    def test_canonical_member_with_dropped_reference_is_counted(self):
        d = self.tmp.name
        o = os.path.join(d, "k0.log")
        x = os.path.join(d, "k1.log")
        with open(o, "wb") as f:
            f.write(v3("A1"))
        with open(x, "wb") as f:
            f.write(v3("A1") + v3("GHOST"))
        report = rehearse_linked_logs([[o], [x]], links=LINK,
                                      on_bad="skip", quiesce=0.005)
        member = report.members[1]
        self.assertTrue(member.would_replace)
        self.assertEqual(member.records_migrated, 1)
        self.assertEqual(member.records_dropped, 1)
        self.assertEqual(member.predicted_bytes, v3("A1"))


# ---------------------------------------------------------------------------
# Rehearsal: read-only guarantees
# ---------------------------------------------------------------------------


class TestRehearsalReadOnly(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        d = self.tmp.name
        self.orders, self.details = write_pair(
            d, b"".join(v1(f"O{i}") for i in range(20)),
            b"".join(v1(f"O{i}") for i in range(20)))
        self.groups = [[self.orders], [self.details]]

    def tearDown(self):
        self.tmp.cleanup()

    def test_creates_and_modifies_nothing(self):
        before = {n: os.stat(os.path.join(self.tmp.name, n))
                  for n in os.listdir(self.tmp.name)}
        for on_bad in ("strict", "skip"):
            rehearse_linked_logs(self.groups, links=LINK, on_bad=on_bad,
                                 quiesce=0.005)
        after_names = set(os.listdir(self.tmp.name))
        self.assertEqual(after_names, set(before))
        for name, st in before.items():
            now = os.stat(os.path.join(self.tmp.name, name))
            self.assertEqual((st.st_size, st.st_mtime_ns, st.st_ino),
                             (now.st_size, now.st_mtime_ns, now.st_ino))

    def test_runs_while_a_live_instance_holds_the_leases(self):
        import fcntl
        locks = [open(p + ".migrate.lock", "a+b")
                 for p in (self.orders, self.details)]
        try:
            for lock in locks:
                fcntl.flock(lock, fcntl.LOCK_EX)
            # No MigrationLockedError, and it still reads the content.
            report = rehearse_linked_logs(self.groups, links=LINK,
                                          on_bad="skip", quiesce=0.005)
            self.assertEqual(
                sum(m.records_migrated for m in report.members), 40)
        finally:
            for lock in locks:
                fcntl.flock(lock, fcntl.LOCK_UN)
                lock.close()
        # The rehearsal created no work directory or marker of its
        # own (only the test-created lock files are present).
        created = [n for n in os.listdir(self.tmp.name)
                   if n.endswith(".committed")
                   or n.startswith(".migrate-linked-tmp")]
        self.assertEqual(created, [])

    def test_does_not_block_a_concurrent_migration(self):
        errors = []

        def migrate():
            try:
                migrate_linked_logs(self.groups, links=LINK,
                                    on_bad="skip", quiesce=0.005,
                                    audit=io.BytesIO())
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        t = threading.Thread(target=migrate)
        t.start()
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            rehearse_linked_logs(self.groups, links=LINK, on_bad="skip",
                                 quiesce=0.005)
            if not t.is_alive():
                break
            time.sleep(0.002)
        t.join(timeout=10)
        self.assertEqual(errors, [])

    def test_pinned_locations_do_not_drift_under_appender(self):
        stop = threading.Event()

        def appender():
            i = 0
            while not stop.is_set():
                with open(self.details, "ab") as f:
                    f.write(v1(f"O{i % 20}"))
                i += 1
                time.sleep(0.0005)

        ta = threading.Thread(target=appender)
        ta.start()
        try:
            for _ in range(25):
                report = rehearse_linked_logs(
                    self.groups, links=LINK, on_bad="skip", quiesce=0.005)
                details = report.members[1]
                # The original 20 lines are always seen, clean, at the
                # same locations; the report is internally consistent.
                self.assertGreaterEqual(details.records_migrated
                                        + details.records_dropped, 20)
                self.assertEqual(details.bad_records, ())
                self.assertEqual(
                    [json.loads(line)["order_id"]
                     for line in details.predicted_bytes.splitlines()][:20],
                    [f"O{i}" for i in range(20)])
        finally:
            stop.set()
            ta.join(timeout=5)

    def test_validation_parity(self):
        with self.assertRaises(TypeError):
            rehearse_linked_logs(self.orders)
        with self.assertRaises(ValueError):
            rehearse_linked_logs([])
        with self.assertRaises(ValueError):
            rehearse_linked_logs([[self.orders], [self.orders]],
                                 links=LINK)
        missing = os.path.join(self.tmp.name, "nope")
        with self.assertRaises(FileNotFoundError):
            rehearse_linked_logs([[self.orders], [missing]], links=LINK)

    def test_cli_exit_codes_and_output(self):
        argv = linked_argv(self.orders, self.details, "skip")
        proc = cli(argv, REHEARSAL_RUNNER)
        self.assertEqual(proc.returncode, EXIT_OK, proc.stderr)
        self.assertIn(b"migrated=40", proc.stdout)
        self.assertIn(b"refs_bad=0", proc.stdout)
        # Malformed link -> usage 2; missing member -> error 1.
        bad = ["--group", self.orders, "--group", self.details,
               "--link", "bogus", "--skip"]
        self.assertEqual(cli(bad, REHEARSAL_RUNNER).returncode, 2)
        miss = ["--group", self.orders, "--group",
                os.path.join(self.tmp.name, "x"), "--skip"]
        self.assertEqual(cli(miss, REHEARSAL_RUNNER).returncode, 1)

    def test_cli_strict_reports_first_error_but_exits_zero(self):
        d = self.tmp.name
        o = os.path.join(d, "gh0.log")
        x = os.path.join(d, "gh1.log")
        with open(o, "wb") as f:
            f.write(v1("GHOST"))
        with open(x, "wb") as f:
            f.write(v1("A1"))
        proc = cli(["--group", o, "--group", x,
                    "--link", "0:1:order_id:order_id", "--strict"],
                   REHEARSAL_RUNNER)
        self.assertEqual(proc.returncode, EXIT_OK, proc.stderr)
        self.assertIn(b"first-error[bad_reference]", proc.stdout)


# ---------------------------------------------------------------------------
# Compaction
# ---------------------------------------------------------------------------


class TestCompaction(unittest.TestCase):
    ORDERS = b"".join(v1(f"O{i}") for i in range(120))
    DETAILS = b"".join(v1(f"O{i}") for i in range(90)) + v1("GHOST")

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        d = self.tmp.name
        self.orders = os.path.join(d, "orders.log")
        self.details = os.path.join(d, "details.log")
        with open(self.orders, "wb") as f:
            f.write(self.ORDERS)
        with open(self.details, "wb") as f:
            f.write(self.DETAILS)
        self.groups = [[self.orders], [self.details]]
        self.argv = linked_argv(self.orders, self.details, "skip",
                                extra=("--segment-size=200",))
        # Reference: uninterrupted migration.
        d0 = tempfile.mkdtemp()
        self.ref_o = os.path.join(d0, "orders.log")
        self.ref_d = os.path.join(d0, "details.log")
        with open(self.ref_o, "wb") as f:
            f.write(self.ORDERS)
        with open(self.ref_d, "wb") as f:
            f.write(self.DETAILS)
        migrate_linked_logs([[self.ref_o], [self.ref_d]], links=LINK,
                            on_bad="skip", quiesce=0.005, segment_size=200,
                            audit=io.BytesIO())
        self.ref = (read_bytes(self.ref_o), read_bytes(self.ref_d))

    def tearDown(self):
        self.tmp.cleanup()

    def _cp_lines(self, path):
        with open(os.path.join(path + ".migrate-linked-tmp",
                               "checkpoint"), "rb") as f:
            return f.read().splitlines()

    def test_compact_form_is_member_sized_and_deterministic(self):
        first = compact_linked_logs(self.groups, links=LINK, on_bad="skip",
                                    quiesce=0.005, segment_size=200,
                                    audit=io.BytesIO())
        self.assertEqual((first.members_prepared, first.members_compacted),
                         (2, 2))
        self.assertEqual(first.references_bad, 1)
        for path in (self.orders, self.details):
            md = path + ".migrate-linked-tmp"
            names = set(os.listdir(md))
            self.assertEqual(len(self._cp_lines(path)), 2)  # header+record
            self.assertEqual(len([n for n in names
                                  if n.startswith("seg-")]), 1)
            self.assertIn("compacted", names)
            self.assertNotIn("lines", names)
        # The originals are untouched.
        self.assertEqual(read_bytes(self.orders), self.ORDERS)
        self.assertEqual(read_bytes(self.details), self.DETAILS)
        # Idempotent: a second call compacts/prepares nothing and
        # reaches exactly the same bytes.
        snap = {
            (p, n): read_bytes(os.path.join(p + ".migrate-linked-tmp", n))
            for p in (self.orders, self.details)
            for n in os.listdir(p + ".migrate-linked-tmp")
        }
        second = compact_linked_logs(self.groups, links=LINK,
                                     on_bad="skip", quiesce=0.005,
                                     segment_size=200, audit=io.BytesIO())
        self.assertEqual((second.members_prepared,
                          second.members_compacted), (0, 0))
        for (path, name), blob in snap.items():
            self.assertEqual(
                read_bytes(os.path.join(path + ".migrate-linked-tmp",
                                        name)),
                blob)

    def test_migration_after_compaction_is_byte_identical(self):
        compact_linked_logs(self.groups, links=LINK, on_bad="skip",
                            quiesce=0.005, segment_size=200,
                            audit=io.BytesIO())
        result = migrate_linked_logs(self.groups, links=LINK,
                                     on_bad="skip", quiesce=0.005,
                                     segment_size=200, audit=io.BytesIO())
        self.assertEqual(result.records_migrated, 0)
        self.assertEqual(result.references_bad, 0)
        self.assertEqual((read_bytes(self.orders),
                          read_bytes(self.details)), self.ref)
        leftovers = [n for n in os.listdir(self.tmp.name)
                     if "migrate-linked" in n]
        self.assertEqual(leftovers, [])

    def test_no_rescan_after_members_are_compacted(self):
        # The first compaction prepares (scans) the members, as a
        # migration run would.  From then on neither a compaction rerun
        # nor the finishing migration decodes a source line.
        compact_linked_logs(self.groups, links=LINK, on_bad="skip",
                            quiesce=0.005, segment_size=200,
                            audit=io.BytesIO())
        calls = []
        orig = lm.loads
        lm.loads = lambda raw: (calls.append(raw), orig(raw))[1]
        try:
            again = compact_linked_logs(self.groups, links=LINK,
                                        on_bad="skip", quiesce=0.005,
                                        segment_size=200, audit=io.BytesIO())
            self.assertEqual((again.members_prepared,
                              again.members_compacted), (0, 0))
            self.assertEqual(calls, [])
            migrate_linked_logs(self.groups, links=LINK, on_bad="skip",
                                quiesce=0.005, segment_size=200,
                                audit=io.BytesIO())
            self.assertEqual(calls, [])
        finally:
            lm.loads = orig

    def test_finishing_run_audits_the_dropped_record_once(self):
        # The dropped bad-reference record is audited by the run that
        # physically performs the filter (the finishing migration),
        # exactly as on a killed-run resume; a further rerun is silent.
        compact_linked_logs(self.groups, links=LINK, on_bad="skip",
                            quiesce=0.005, segment_size=200,
                            audit=io.BytesIO())
        audit = io.BytesIO()
        migrate_linked_logs(self.groups, links=LINK, on_bad="skip",
                            quiesce=0.005, segment_size=200, audit=audit)
        entries = audit.getvalue().splitlines()
        self.assertEqual(len(entries), 1)
        self.assertIn(b"details.log:91:", entries[0])
        # The committed rerun audits nothing.
        audit2 = io.BytesIO()
        migrate_linked_logs(self.groups, links=LINK, on_bad="skip",
                            quiesce=0.005, segment_size=200, audit=audit2)
        self.assertEqual(audit2.getvalue(), b"")

    CRASH_POINTS = ("compact-lock", "compact-prepare", "compact-refs",
                    "compact-resolve", "compact-member", "compact-done")

    def test_crash_recovery_at_every_point(self):
        for point in self.CRASH_POINTS:
            with self.subTest(point=point):
                with open(self.orders, "wb") as f:
                    f.write(self.ORDERS)
                with open(self.details, "wb") as f:
                    f.write(self.DETAILS)
                for p in (self.orders, self.details):
                    shutil.rmtree(p + ".migrate-linked-tmp",
                                  ignore_errors=True)
                proc = cli([point, *self.argv], COMPACT_RUNNER)
                self.assertEqual(proc.returncode, 1,
                                 (point, proc.stderr))
                proc = cli(["-", *self.argv], COMPACT_RUNNER)
                self.assertEqual(proc.returncode, 0,
                                 (point, proc.stderr))
                migrate_linked_logs(self.groups, links=LINK,
                                    on_bad="skip", quiesce=0.005,
                                    segment_size=200, audit=io.BytesIO())
                self.assertEqual((read_bytes(self.orders),
                                  read_bytes(self.details)), self.ref,
                                 point)

    def test_compact_after_killed_migration_then_finish(self):
        linked_runner = (
            "import os,sys;sys.path.insert(0,%r);"
            "os.environ['PROTO_MIGRATE_CRASH_AT']=sys.argv[1];"
            "from proto_migrate.linked_migration import run_linked_cli;"
            "raise SystemExit(run_linked_cli(sys.argv[2:]))"
        ) % REPO_ROOT
        for crash in ("linked-prepare", "linked-stage"):
            with self.subTest(crash=crash):
                with open(self.orders, "wb") as f:
                    f.write(self.ORDERS)
                with open(self.details, "wb") as f:
                    f.write(self.DETAILS)
                for p in (self.orders, self.details):
                    shutil.rmtree(p + ".migrate-linked-tmp",
                                  ignore_errors=True)
                argv = linked_argv(self.orders, self.details, "skip",
                                   extra=("--segment-size=200",))
                proc = subprocess.run(
                    [sys.executable, "-c", linked_runner, crash, *argv],
                    capture_output=True)
                self.assertEqual(proc.returncode, 1, proc.stderr)
                self.assertGreater(
                    len(self._cp_lines(self.orders)), 2)
                result = compact_linked_logs(
                    self.groups, links=LINK, on_bad="skip",
                    quiesce=0.005, segment_size=200, audit=io.BytesIO())
                self.assertEqual(result.members_compacted, 2)
                self.assertEqual(len(self._cp_lines(self.orders)), 2)
                calls = []
                orig = lm.loads
                lm.loads = lambda raw: (calls.append(raw), orig(raw))[1]
                try:
                    migrate_linked_logs(
                        self.groups, links=LINK, on_bad="skip",
                        quiesce=0.005, segment_size=200,
                        audit=io.BytesIO())
                finally:
                    lm.loads = orig
                self.assertEqual(calls, [])
                self.assertEqual((read_bytes(self.orders),
                                  read_bytes(self.details)), self.ref)

    def test_strict_first_error_is_preserved(self):
        d = self.tmp.name
        o = os.path.join(d, "s0.log")
        x = os.path.join(d, "s1.log")
        with open(o, "wb") as f:
            f.write(v1("A1") + b"BROKEN\n")
        with open(x, "wb") as f:
            f.write(v1("GHOST"))
        result = compact_linked_logs([[o], [x]], links=LINK,
                                     on_bad="strict", quiesce=0.005)
        self.assertEqual(result.first_error_kind, "bad_record")
        self.assertIsInstance(result.first_error, GroupBadRecordError)
        with self.assertRaises(GroupBadRecordError) as cm:
            migrate_linked_logs([[o], [x]], links=LINK, quiesce=0.005)
        self.assertEqual(str(cm.exception), str(result.first_error))
        # Originals untouched, no commit marker.
        self.assertEqual(read_bytes(o), v1("A1") + b"BROKEN\n")
        self.assertFalse(os.path.exists(o + ".committed"))

    def test_held_lease_raises_locked(self):
        import fcntl
        lock = open(self.orders + ".migrate.lock", "a+b")
        fcntl.flock(lock, fcntl.LOCK_EX)
        try:
            with self.assertRaises(MigrationLockedError):
                compact_linked_logs(self.groups, links=LINK,
                                    on_bad="skip", quiesce=0.005,
                                    segment_size=200)
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)
            lock.close()

    def test_cli(self):
        proc = cli(["-", *self.argv], COMPACT_RUNNER)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertIn(b"compacted=2", proc.stdout)
        again = cli(["-", *self.argv], COMPACT_RUNNER)
        self.assertEqual(again.returncode, 0, again.stderr)
        self.assertIn(b"compacted=0", again.stdout)


# ---------------------------------------------------------------------------
# Streaming session descriptor reclaim
# ---------------------------------------------------------------------------


class TestStreamSessionReclaim(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        d = self.tmp.name
        self.orders = os.path.join(d, "orders.log")
        self.details = os.path.join(d, "details.log")
        with open(self.orders, "wb") as f:
            f.write(v1("A1") + v1("A2") + v1("A3"))
        with open(self.details, "wb") as f:
            f.write(v1("A1") + v1("A2"))
        self.groups = [[self.orders], [self.details]]

    def tearDown(self):
        self.tmp.cleanup()

    def test_close_reclaims_fds_but_cursor_stays_resumable(self):
        batch, cursor = read_linked_logs_stream(
            self.groups, None, batch_records=2, quiesce=0.005)
        self.assertEqual([r["order_id"] for r in batch], ["A1", "A2"])
        self.assertEqual(len(_STREAM_SESSIONS), 1)
        close_linked_logs_stream(cursor)
        self.assertEqual(_STREAM_SESSIONS, {})
        # The cursor rebuilds the pinned snapshot and finishes exactly.
        rest = []
        while cursor is not None:
            batch, cursor = read_linked_logs_stream(
                self.groups, cursor, batch_records=2, quiesce=0.005)
            rest.extend(batch)
        self.assertEqual([r["order_id"] for r in rest],
                         ["A3", "A1", "A2"])

    def test_close_rejects_non_string_cursor(self):
        with self.assertRaises(TypeError):
            close_linked_logs_stream(42)
        with self.assertRaises(ValueError):
            close_linked_logs_stream("not a cursor")

    def test_idle_reaper_reclaims_pinned_descriptors(self):
        from proto_migrate import linked_migration as mod
        old_ttl = mod._STREAM_SESSION_TTL
        mod._STREAM_SESSION_TTL = -1.0
        try:
            _batch, cursor = read_linked_logs_stream(
                self.groups, None, batch_records=2, quiesce=0.005)
            # The next call's reaping reclaims the idle session; the
            # cursor itself still resumes from the pinned snapshot.
            batch, cursor2 = read_linked_logs_stream(
                self.groups, cursor, batch_records=2, quiesce=0.005)
            self.assertEqual([r["order_id"] for r in batch], ["A3"])
        finally:
            mod._STREAM_SESSION_TTL = old_ttl
        if cursor2 is not None:
            while cursor2 is not None:
                _b, cursor2 = read_linked_logs_stream(
                    self.groups, cursor2, batch_records=2, quiesce=0.005)


# ---------------------------------------------------------------------------
# Codec contract: illegal amounts, negative zero, migration exit codes
# ---------------------------------------------------------------------------


class TestCodecContract(unittest.TestCase):
    def test_overflow_literal_is_a_bad_record(self):
        for literal in (b"1e999", b"-1e999", b"1E400"):
            with self.subTest(literal=literal):
                with self.assertRaises(ValueError):
                    lm.loads(
                        b'{"v":1,"order_id":"x","amount":' + literal
                        + b',"tags":[]}\n')

    def test_negative_zero_roundtrips_through_codec(self):
        import math
        blob = dumps({
            "order_id": "x", "amount": -0.0, "status": "new",
            "note": "", "updated_at": 0,
        })
        self.assertIn(b"-0.0", blob)
        decoded = lm.loads(blob)
        self.assertTrue(math.copysign(1.0, decoded["amount"]) < 0)
        self.assertEqual(lm._convert(blob), blob)

    def test_entry_exit_codes(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "a.log")
            with open(path, "wb") as f:
                f.write(b'{"v":1,"order_id":"x","amount":1e999,'
                        b'"tags":[]}\n')
            run = lambda *a: subprocess.run(
                [sys.executable, "-m", "proto_migrate", *a],
                capture_output=True, cwd=REPO_ROOT)
            # Strict -> 3; the file stays untouched.
            proc = run("migrate-log", path, "--strict")
            self.assertEqual(proc.returncode, EXIT_BAD_RECORD)
            self.assertIn(b"bad record", proc.stderr)
            # Skip -> 0 and the bad line is gone.
            proc = run("migrate-log", path, "--skip")
            self.assertEqual(proc.returncode, 0)
            self.assertEqual(read_bytes(path), b"")
            # Usage -> 2.
            proc = run("migrate-log")
            self.assertEqual(proc.returncode, 2)
            # Missing file -> 1.
            proc = run("migrate-log", os.path.join(d, "missing"))
            self.assertEqual(proc.returncode, EXIT_ERROR)
            # Selftest stays green.
            proc = run("--selftest")
            self.assertEqual(proc.returncode, 0)
            self.assertEqual(proc.stdout, b"ok\n")


if __name__ == "__main__":
    unittest.main()
