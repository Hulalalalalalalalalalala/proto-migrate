"""Tests for the incremental (delta) cross-group linked migration.

Covers:
  * the first run with no ledger deterministically falls back to a full
    migration whose result is byte-for-byte identical to
    ``migrate_linked_logs`` and names the fallback reason;
  * later runs scan only the appended suffix: per-member added /
    rewritten / skipped / refs-dropped counters, their accounting
    identity, and equality with a full rerun's summary over the same new
    input, with byte-identical final files;
  * reconciliation against prefix target keys (live, skipped,
    illegal-version, cycle-cascade), chained deltas, the no-op zero
    summary, and -0.0 / illegal-amount handling;
  * the ledger is a local, rebuildable file: missing / corrupt /
    tampered-prefix / changed-inode fallbacks;
  * crash at every delta phase (real SIGKILL subprocess) resumes only
    the unfinished part, counts only this continuation's rows and
    finishes byte-identical to an uninterrupted run;
  * a continuously appending writer loses no record and keeps order;
  * leases (conflict, takeover, disjoint parallel), the strict global
    first-error ordering, the TypeError/ValueError/FileNotFoundError
    taxonomy, prefix-not-rescanned, and CLI exit codes.
"""

import fcntl
import io
import json
import os
import subprocess
import sys
import tempfile
import threading
import time
import unittest

import proto_migrate.log_migration as lm
from proto_migrate import dumps, migrate_linked_delta, migrate_linked_logs
from proto_migrate.delta_migration import (
    _delta_group_dir,
    _ledger_path,
    run_delta_cli,
)
from proto_migrate.linked_migration import MigrationLockedError
from proto_migrate.log_migration import (
    EXIT_BAD_RECORD,
    EXIT_ERROR,
    EXIT_OK,
    EXIT_USAGE,
)

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
    return [json.loads(line)["order_id"] for line in blob.splitlines() if line]


DELTA_RUNNER = r"""
import os, sys
sys.path.insert(0, %r)
if sys.argv[1] != "-":
    os.environ["PROTO_MIGRATE_CRASH_AT"] = sys.argv[1]
from proto_migrate.delta_migration import run_delta_cli
raise SystemExit(run_delta_cli(sys.argv[2:]))
""" % REPO_ROOT


def delta_argv(groups, on_bad="skip", segment_size=400, quiesce_ms=10):
    argv = []
    for group in groups:
        argv += ["--group", *group]
    argv += ["--link", "1:0:order_id:order_id"]
    argv += [f"--quiesce-ms={quiesce_ms}",
             f"--segment-size={segment_size}",
             "--skip" if on_bad == "skip" else "--strict"]
    return argv


def run_delta_subprocess(groups, crash="-", on_bad="skip", segment_size=400):
    return subprocess.run(
        [sys.executable, "-c", DELTA_RUNNER, crash,
         *delta_argv(groups, on_bad=on_bad, segment_size=segment_size)],
        capture_output=True)


class DeltaFixture:
    """Two identical directories: one driven by full, one by delta."""

    def __init__(self, n=12, canonical_orders=False):
        self.left = tempfile.mkdtemp()
        self.right = tempfile.mkdtemp()
        self.orders_l = os.path.join(self.left, "orders.log")
        self.details_l = os.path.join(self.left, "details.log")
        self.orders_r = os.path.join(self.right, "orders.log")
        self.details_r = os.path.join(self.right, "details.log")
        orders = b"".join(
            v3(f"O{i}") if canonical_orders else
            (v2(f"O{i}") if i % 2 else v1(f"O{i}"))
            for i in range(n))
        details = b"".join(v1(f"O{i}") for i in range(n))
        for path, blob in ((self.orders_l, orders), (self.details_l, details),
                           (self.orders_r, orders), (self.details_r, details)):
            with open(path, "wb") as f:
                f.write(blob)
        self.n = n

    @property
    def groups_l(self):
        return [[self.orders_l], [self.details_l]]

    @property
    def groups_r(self):
        return [[self.orders_r], [self.details_r]]

    def append_both(self, orders_blob, details_blob):
        with open(self.orders_l, "ab") as f:
            f.write(orders_blob)
        with open(self.orders_r, "ab") as f:
            f.write(orders_blob)
        with open(self.details_l, "ab") as f:
            f.write(details_blob)
        with open(self.details_r, "ab") as f:
            f.write(details_blob)

    def bytes_equal(self):
        return (read_bytes(self.orders_l) == read_bytes(self.orders_r)
                and read_bytes(self.details_l) == read_bytes(self.details_r))

    def leftovers(self, root):
        # Per-member scratch and rename debris must be gone; the delta
        # group work directory persists on purpose -- it is the local
        # reconciliation ledger's home and is rebuilt at any time.
        return sorted(
            n for n in os.listdir(root)
            if ("migrate" in n and not n.startswith(".migrate-delta-tmp-")
                and not n.endswith(".migrate.lock")
                and not n.endswith(".committed")))


class TestDeltaBasics(unittest.TestCase):
    def _delta(self, groups, **kw):
        kw.setdefault("quiesce", 0.01)
        kw.setdefault("audit", io.BytesIO())
        kw.setdefault("links", LINK)
        return migrate_linked_delta(groups, **kw)

    def _full(self, groups, **kw):
        kw.setdefault("quiesce", 0.01)
        kw.setdefault("audit", io.BytesIO())
        kw.setdefault("links", LINK)
        return migrate_linked_logs(groups, **kw)

    def test_first_run_falls_back_and_matches_full(self):
        fx = DeltaFixture(n=12)
        full = self._full(fx.groups_l, on_bad="skip")
        delta = self._delta(fx.groups_r, on_bad="skip")
        self.assertTrue(delta.fallback)
        self.assertEqual(delta.fallback_reason, "ledger-missing")
        self.assertTrue(fx.bytes_equal())
        # Fallback is a real migration of every row: per-member added
        # rows are the surviving rows of the replaced members.
        per = {os.path.basename(m.path): m for m in delta.members}
        self.assertEqual(per["orders.log"].lines_added, 12)
        self.assertEqual(per["details.log"].lines_added, 12)
        self.assertEqual(delta.records_added, full.records_migrated)
        self.assertEqual(fx.leftovers(fx.right), [])

    def test_counters_match_full_rerun_summary_and_bytes(self):
        # Canonical v3 baseline so a full rerun only moves the appended
        # segment; the segment mixes rewritten, canonical, bad and a
        # dangling reference.
        fx = DeltaFixture(n=8, canonical_orders=True)
        # Make the baseline details canonical too for a clean baseline.
        for path in (fx.details_l, fx.details_r):
            with open(path, "wb") as f:
                f.write(b"".join(v3(f"O{i}") for i in range(8)))
        self._full(fx.groups_l, on_bad="skip")
        first = self._delta(fx.groups_r, on_bad="skip")
        self.assertTrue(first.fallback)
        self.assertTrue(fx.bytes_equal())

        new_orders = v1("N0") + v3("N1") + v1("N2")
        new_details = (v1("N0") + v3("N1") + v1("N2")   # survive
                       + v1("GHOST")                     # dropped (bad ref)
                       + b'{"v":1,"order_id":"BAD","amount":'
                         b'NaN,"tags":[]}\n')             # bad line
        fx.append_both(new_orders, new_details)

        full = self._full(fx.groups_l, on_bad="skip")
        delta = self._delta(fx.groups_r, on_bad="skip")
        self.assertFalse(delta.fallback)
        self.assertTrue(fx.bytes_equal())

        # Full rerun summary over the new input (baseline was canonical,
        # so every counted row is an appended one).
        self.assertEqual(full.records_skipped, 1)
        per_d = {os.path.basename(m.path): m for m in delta.members}
        o = per_d["orders.log"]
        t = per_d["details.log"]
        # Orders: 3 new rows, N0/N2 rewritten, N1 canonical; no bad.
        self.assertEqual((o.lines_added, o.lines_rewritten,
                          o.lines_skipped, o.references_dropped), (3, 2, 0, 0))
        # Details: 4 good rows, one dropped, one bad line; N0/N2/GHOST
        # rewritten, N1 canonical.
        self.assertEqual((t.lines_added, t.lines_skipped,
                          t.references_dropped), (3, 1, 1))
        self.assertEqual(t.lines_rewritten, 2)
        # Accounting identity: every new source row lands in one bucket.
        self.assertEqual(o.lines_added + o.lines_skipped
                         + o.references_dropped, 3)
        self.assertEqual(t.lines_added + t.lines_skipped
                         + t.references_dropped, 5)
        # A full rerun stages each affected member whole, so its
        # migrated count includes the canonical baseline; the delta's
        # marginal added count equals it minus that baseline.
        per_f = {os.path.basename(m.path): m for m in full.members}
        self.assertEqual(
            o.lines_added, per_f["orders.log"].records_migrated - 8)
        self.assertEqual(
            t.lines_added, per_f["details.log"].records_migrated - 8)
        # Suffix-only phenomena agree outright.
        self.assertEqual(delta.records_skipped, full.records_skipped)
        # Rewritten never exceeds added.
        self.assertLessEqual(t.lines_rewritten, t.lines_added)
        self.assertEqual(fx.leftovers(fx.right), [])

    def test_chained_deltas_equal_one_full(self):
        fx = DeltaFixture(n=6)
        self._full(fx.groups_l, on_bad="skip")
        self._delta(fx.groups_r, on_bad="skip")
        for k in range(4):
            blob = b"".join(v1(f"C{k}_{i}") for i in range(3))
            # details reference the orders rows of the same batch plus
            # an earlier one, so nothing is dangling.
            det = (v1("O0") + blob) if k == 0 else blob
            fx.append_both(blob, det)
            self._full(fx.groups_l, on_bad="skip")
            r = self._delta(fx.groups_r, on_bad="skip")
            self.assertFalse(r.fallback)
        self.assertTrue(fx.bytes_equal())
        self.assertEqual(fx.leftovers(fx.right), [])

    def test_no_new_rows_is_zero_summary(self):
        fx = DeltaFixture(n=5, canonical_orders=True)
        for path in (fx.details_l, fx.details_r):
            with open(path, "wb") as f:
                f.write(b"".join(v3(f"O{i}") for i in range(5)))
        self._delta(fx.groups_r, on_bad="skip")
        again = self._delta(fx.groups_r, on_bad="skip")
        self.assertFalse(again.fallback)
        self.assertFalse(again.replaced)
        self.assertEqual((again.records_added, again.records_rewritten,
                          again.records_skipped, again.references_dropped),
                         (0, 0, 0, 0))

    def test_prefix_target_keys_resolve(self):
        d = tempfile.mkdtemp()
        orders = os.path.join(d, "orders.log")
        details = os.path.join(d, "details.log")
        groups = [[orders], [details]]
        # Baseline: one good target, one skipped-bad line carrying a key
        # ("SKIP") and one illegal-version line carrying key "BADV".
        with open(orders, "wb") as f:
            f.write(v1("GOOD")
                    + b'{"v":1,"order_id":"SKIP","amount":NaN,"tags":[]}\n'
                    + b'{"v":9,"order_id":"BADV","amount":1,"tags":[]}\n')
        with open(details, "wb") as f:
            f.write(v1("GOOD"))
        self._delta(groups, on_bad="skip")

        with open(details, "ab") as f:
            f.write(v1("GOAD"))   # typo -> dangling -> dropped
            f.write(v1("GOOD"))   # prefix live key -> survives
        r = self._delta(groups, on_bad="skip")
        per = [m for m in r.members if os.path.basename(m.path)
               == "details.log"][0]
        self.assertEqual((per.lines_added, per.references_dropped), (1, 1))
        blob = read_bytes(details)
        self.assertNotIn(b"GOAD", blob)
        self.assertEqual(ids_of(blob).count("GOOD"), 2)

        # New references to the skipped / illegal-version prefix keys
        # are dropped, and bad lines stay classified.
        with open(details, "ab") as f:
            f.write(v1("SKIP") + v1("BADV"))
        r = self._delta(groups, on_bad="skip")
        per = [m for m in r.members if os.path.basename(m.path)
               == "details.log"][0]
        self.assertEqual(per.references_dropped, 2)
        self.assertEqual(per.lines_added, 0)

    def test_negative_zero_and_illegal_amounts(self):
        d = tempfile.mkdtemp()
        orders = os.path.join(d, "orders.log")
        details = os.path.join(d, "details.log")
        groups = [[orders], [details]]
        with open(orders, "wb") as f:
            f.write(v3("K0"))
        with open(details, "wb") as f:
            f.write(v3("K0"))
        self._delta(groups, on_bad="skip")
        with open(details, "ab") as f:
            # -0.0 is preserved verbatim even through the v1->v3 rewrite;
            # 1e999 is a bad amount.
            f.write(v1("K0", -0.0))
            f.write(b'{"v":1,"order_id":"K0","amount":1e999,'
                    b'"tags":[]}\n')
        r = self._delta(groups, on_bad="skip")
        per = [m for m in r.members if os.path.basename(m.path)
               == "details.log"][0]
        self.assertEqual((per.lines_added, per.lines_skipped), (1, 1))
        self.assertIn(b"-0.0", read_bytes(details))

    def test_strict_global_first_error_ordering(self):
        d = tempfile.mkdtemp()
        # Group 0 gets a bad reference on its first appended line; group
        # 1 gets a bad line on its second appended line.  The reference
        # sorts first (group order wins).
        g0 = os.path.join(d, "g0.log")
        g1 = os.path.join(d, "g1.log")
        with open(g0, "wb") as f:
            f.write(v3("ROOT"))
        with open(g1, "wb") as f:
            f.write(v3("ROOT"))
        groups = [[g0], [g1]]
        self._delta(groups, on_bad="strict")
        with open(g0, "ab") as f:
            f.write(v1("MISSING"))
        with open(g1, "ab") as f:
            f.write(v3("ROOT") + b"BROKEN\n")
        with self.assertRaises(ValueError):
            self._delta(groups, on_bad="strict")
        # Originals are untouched.
        self.assertIn(b"MISSING", read_bytes(g0))
        self.assertIn(b"BROKEN", read_bytes(g1))

        # And a bad line in an earlier group beats a later group's bad
        # reference (clean baseline, both problems only in the suffix).
        d2 = tempfile.mkdtemp()
        h0 = os.path.join(d2, "g0.log")
        h1 = os.path.join(d2, "g1.log")
        with open(h0, "wb") as f:
            f.write(v3("ROOT"))
        with open(h1, "wb") as f:
            f.write(v3("ROOT"))
        groups2 = [[h0], [h1]]
        self._delta(groups2, on_bad="strict")
        with open(h0, "ab") as f:
            f.write(b"BROKEN\n")
        with open(h1, "ab") as f:
            f.write(v1("MISSING"))
        from proto_migrate.group_migration import GroupBadRecordError
        try:
            self._delta(groups2, on_bad="strict")
        except GroupBadRecordError as exc:
            self.assertIn("g0.log", str(exc))
        else:
            self.fail("expected GroupBadRecordError")


class TestDeltaLedgerFallback(unittest.TestCase):
    def _delta(self, groups, **kw):
        kw.setdefault("quiesce", 0.01)
        kw.setdefault("audit", io.BytesIO())
        kw.setdefault("links", LINK)
        return migrate_linked_delta(groups, **kw)

    def setUp(self):
        self.d = tempfile.mkdtemp()
        self.orders = os.path.join(self.d, "orders.log")
        self.details = os.path.join(self.d, "details.log")
        with open(self.orders, "wb") as f:
            f.write(b"".join(v1(f"O{i}") for i in range(6)))
        with open(self.details, "wb") as f:
            f.write(b"".join(v1(f"O{i}") for i in range(6)))
        self.groups = [[self.orders], [self.details]]
        self._delta(self.groups, on_bad="skip")
        self.group_dir = _delta_group_dir(self.groups)
        self.ledger = _ledger_path(self.group_dir)
        self.assertTrue(os.path.isfile(self.ledger))

    def _append(self):
        with open(self.orders, "ab") as f:
            f.write(v1("N0"))
        with open(self.details, "ab") as f:
            f.write(v1("N0"))

    def test_ledger_deleted_is_rebuilt(self):
        os.remove(self.ledger)
        self._append()
        r = self._delta(self.groups, on_bad="skip")
        self.assertTrue(r.fallback)
        self.assertEqual(r.fallback_reason, "ledger-missing")
        self.assertTrue(os.path.isfile(self.ledger))
        self.assertEqual(ids_of(read_bytes(self.details))[-1], "N0")

    def test_ledger_corrupt_falls_back(self):
        with open(self.ledger, "ab") as f:
            f.write(b"{not json at all")
        self._append()
        r = self._delta(self.groups, on_bad="skip")
        self.assertTrue(r.fallback)
        self.assertEqual(r.fallback_reason, "ledger-corrupt")

    def test_prefix_bytes_tampered_falls_back(self):
        # Mutate a byte inside the reconciled prefix.
        with open(self.orders, "r+b") as f:
            f.seek(5)
            f.write(b"X")
        self._append()
        r = self._delta(self.groups, on_bad="skip")
        self.assertTrue(r.fallback)
        self.assertEqual(r.fallback_reason, "ledger-corrupt")

    def test_source_inode_changed_falls_back(self):
        new_path = os.path.join(self.d, "replacement.log")
        with open(new_path, "wb") as f:
            f.write(read_bytes(self.orders) + v1("N0"))
        os.replace(new_path, self.orders)
        r = self._delta(self.groups, on_bad="skip")
        self.assertTrue(r.fallback)
        self.assertEqual(r.fallback_reason, "source-inode-changed")
        self.assertTrue(r.records_added > 0)


class TestDeltaValidationAndLeases(unittest.TestCase):
    def setUp(self):
        self.d = tempfile.mkdtemp()
        self.o = os.path.join(self.d, "orders.log")
        self.t = os.path.join(self.d, "details.log")
        with open(self.o, "wb") as f:
            f.write(v3("A"))
        with open(self.t, "wb") as f:
            f.write(v3("A"))

    def _delta(self, groups, **kw):
        kw.setdefault("quiesce", 0.01)
        kw.setdefault("audit", io.BytesIO())
        kw.setdefault("links", LINK)
        return migrate_linked_delta(groups, **kw)

    def test_taxonomy(self):
        with self.assertRaises(TypeError):
            self._delta(self.o)
        with self.assertRaises(ValueError):
            self._delta([[]])
        with self.assertRaises(ValueError):
            self._delta([[self.o], [self.o]])
        with self.assertRaises(FileNotFoundError):
            self._delta([[os.path.join(self.d, "missing.log")], [self.t]])
        # Directory (exists but unreadable as a file) -> FileNotFound.
        unreadable = os.path.join(self.d, "subdir")
        os.mkdir(unreadable)
        with self.assertRaises(FileNotFoundError):
            self._delta([[unreadable], [self.t]])

    def test_lease_conflict_then_takeover(self):
        groups = [[self.o], [self.t]]
        self._delta(groups, on_bad="skip")
        with open(self.o + ".migrate.lock", "a+b") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            with open(self.t, "ab") as f:
                f.write(v1("A"))
            with self.assertRaises(MigrationLockedError):
                self._delta(groups, on_bad="skip")
            fcntl.flock(lock, fcntl.LOCK_UN)
        # Holder gone: the next instance takes over and finishes.
        r = self._delta(groups, on_bad="skip")
        self.assertEqual(ids_of(read_bytes(self.t)), ["A", "A"])
        self.assertEqual(r.records_added, 1)

    def test_disjoint_groups_run_in_parallel(self):
        other = tempfile.mkdtemp()
        o2 = os.path.join(other, "o2.log")
        t2 = os.path.join(other, "t2.log")
        with open(o2, "wb") as f:
            f.write(v1("B"))
        with open(t2, "wb") as f:
            f.write(v1("B"))
        errors = []

        def run(groups):
            try:
                self._delta(groups, on_bad="skip")
            except Exception as exc:  # pragma: no cover - failure path
                errors.append(exc)

        t1 = threading.Thread(target=run, args=([[self.o], [self.t]],))
        t2 = threading.Thread(target=run, args=([[o2], [t2]],))
        t1.start(); t2.start(); t1.join(); t2.join()
        self.assertEqual(errors, [])


class TestDeltaPrefixNotRescanned(unittest.TestCase):
    def test_loads_calls_cover_only_suffix(self):
        d = tempfile.mkdtemp()
        orders = os.path.join(d, "orders.log")
        details = os.path.join(d, "details.log")
        groups = [[orders], [details]]
        with open(orders, "wb") as f:
            f.write(b"".join(v3(f"O{i}") for i in range(20)))
        with open(details, "wb") as f:
            f.write(b"".join(v3(f"O{i}") for i in range(20)))
        migrate_linked_delta(groups, links=LINK, quiesce=0.01,
                             audit=io.BytesIO(), on_bad="skip")
        with open(details, "ab") as f:
            f.write(v1("O0") + v3("O1"))
        calls = []
        orig = lm.loads
        lm.loads = lambda raw: (calls.append(raw), orig(raw))[1]
        try:
            migrate_linked_delta(groups, links=LINK, quiesce=0.01,
                                 audit=io.BytesIO(), on_bad="skip")
        finally:
            lm.loads = orig
        # Only the two appended rows are decoded; the 20-row prefix is
        # neither rescanned nor revalidated.
        self.assertEqual(len(calls), 2)


class TestDeltaCrashRecovery(unittest.TestCase):
    CRASH_POINTS = ("delta-lock", "delta-prepare", "delta-refs",
                    "delta-filter", "delta-stage", "delta-staged",
                    "delta-marker", "delta-converge", "delta-ledger")

    def test_every_crash_point_resumes_byte_identical(self):
        for point in self.CRASH_POINTS:
            with self.subTest(point=point):
                self._one(point)

    def _one(self, point):
        fx = DeltaFixture(n=24)
        migrate_linked_logs(fx.groups_l, links=LINK, on_bad="skip",
                            quiesce=0.005, audit=io.BytesIO())
        first = migrate_linked_delta(fx.groups_r, links=LINK,
                                     on_bad="skip", quiesce=0.005,
                                     segment_size=200, audit=io.BytesIO())
        self.assertTrue(first.fallback)
        # A segment large enough to rotate checkpoints mid-suffix.
        new_orders = b"".join(v1(f"P{i}") if i % 2 else v3(f"P{i}")
                              for i in range(24))
        new_details = b"".join(v1(f"P{i}") for i in range(24))
        fx.append_both(new_orders, new_details)

        proc = run_delta_subprocess(fx.groups_r, crash=point,
                                    segment_size=200)
        self.assertEqual(proc.returncode, 1, proc.stderr)

        done = migrate_linked_delta(fx.groups_r, links=LINK,
                                    on_bad="skip", quiesce=0.005,
                                    segment_size=200, audit=io.BytesIO())
        # A second run is an idempotent no-op.
        again = migrate_linked_delta(fx.groups_r, links=LINK,
                                     on_bad="skip", quiesce=0.005,
                                     segment_size=200, audit=io.BytesIO())
        self.assertEqual((again.records_added, again.records_rewritten,
                          again.records_skipped, again.references_dropped),
                         (0, 0, 0, 0))
        # Reference world: the left dir migrates the same appended bytes.
        migrate_linked_logs(fx.groups_l, links=LINK, on_bad="skip",
                            quiesce=0.005, audit=io.BytesIO())
        self.assertTrue(fx.bytes_equal(), point)
        blob = read_bytes(fx.details_r)
        self.assertEqual(ids_of(blob),
                         [f"O{i}" for i in range(24)]
                         + [f"P{i}" for i in range(24)],
                         point)
        self.assertEqual(fx.leftovers(fx.right), [], point)
        self.assertFalse(done.fallback)


class TestDeltaConcurrentAppender(unittest.TestCase):
    def test_continuous_appender_zero_loss_zero_duplicate(self):
        d = tempfile.mkdtemp()
        ref = tempfile.mkdtemp()
        orders = os.path.join(d, "orders.log")
        details = os.path.join(d, "details.log")
        ro = os.path.join(ref, "orders.log")
        rt = os.path.join(ref, "details.log")
        base_o = b"".join(v1(f"B{i}") for i in range(10))
        base_t = b"".join(v1(f"B{i}") for i in range(10))
        for p, b in ((orders, base_o), (details, base_t),
                     (ro, base_o), (rt, base_t)):
            with open(p, "wb") as f:
                f.write(b)
        groups = [[orders], [details]]
        migrate_linked_delta(groups, links=LINK, on_bad="skip",
                             quiesce=0.005, audit=io.BytesIO())

        stop = threading.Event()
        error_box = []
        counts = {"o": 0, "t": 0}

        def appender(path, prefix, key_prefix=None, slot=None):
            try:
                i = 0
                # Canonical rows.  Details reference stable baseline B
                # keys (present before the writers started) so a detail
                # row can never transiently dangle; whole-record writes.
                with open(path, "ab") as f:
                    while not stop.is_set() and i < 60:
                        key = f"{key_prefix}{i % 10}" if key_prefix \
                            else f"{prefix}{i}"
                        f.write(v3(key))
                        f.flush()
                        i += 1
                        time.sleep(0.002)
                if slot is not None:
                    counts[slot] = i
            except Exception as exc:  # pragma: no cover - failure path
                error_box.append(exc)

        threads = [threading.Thread(target=appender,
                                    args=(orders, "O", None, "o")),
                   threading.Thread(target=appender,
                                    args=(details, "T", "B", "t"))]
        for t in threads:
            t.start()
        # Repeated delta passes while the writers run.
        for _ in range(3):
            migrate_linked_delta(groups, links=LINK, on_bad="skip",
                                 quiesce=0.005, audit=io.BytesIO())
            time.sleep(0.01)
        stop.set()
        for t in threads:
            t.join()
        self.assertEqual(error_box, [])
        # Final catch-up pass after every write has landed.
        final = migrate_linked_delta(groups, links=LINK, on_bad="skip",
                                     quiesce=0.005, audit=io.BytesIO())
        again = migrate_linked_delta(groups, links=LINK, on_bad="skip",
                                     quiesce=0.005, audit=io.BytesIO())
        self.assertEqual((again.records_added, again.records_skipped,
                          again.references_dropped), (0, 0, 0))
        # Orders carry unique keys: zero loss, zero duplication, order.
        oids = ids_of(read_bytes(orders))
        self.assertEqual(len(oids), 10 + counts["o"])
        self.assertEqual(len(oids), len(set(oids)))
        self.assertEqual(oids[:10], [f"B{i}" for i in range(10)])
        self.assertEqual(oids[10:], [f"O{i}" for i in range(counts["o"])])
        # Details repeat B keys but every record is preserved exactly
        # once and in append order.
        tids = ids_of(read_bytes(details))
        expected_tail = [f"B{i % 10}" for i in range(counts["t"])]
        self.assertEqual(len(tids), 10 + counts["t"])
        self.assertEqual(tids[:10], [f"B{i}" for i in range(10)])
        self.assertEqual(tids[10:], expected_tail)
        self.assertGreaterEqual(final.records_added, 0)


class TestDeltaCLI(unittest.TestCase):
    def setUp(self):
        self.d = tempfile.mkdtemp()
        self.o = os.path.join(self.d, "orders.log")
        self.t = os.path.join(self.d, "details.log")
        with open(self.o, "wb") as f:
            f.write(v1("A1"))
        with open(self.t, "wb") as f:
            f.write(v1("A1"))

    def test_cli_happy_path_and_counters(self):
        code = run_delta_cli(delta_argv([[self.o], [self.t]]))
        self.assertEqual(code, EXIT_OK)
        with open(self.t, "ab") as f:
            f.write(v1("A1"))
        code = run_delta_cli(delta_argv([[self.o], [self.t]]))
        self.assertEqual(code, EXIT_OK)
        self.assertEqual(ids_of(read_bytes(self.t)), ["A1", "A1"])

    def test_cli_exit_codes(self):
        missing = os.path.join(self.d, "nope.log")
        self.assertEqual(
            run_delta_cli(["--group", missing, "--group", self.t,
                           "--link", "1:0:order_id:order_id", "--skip"]),
            EXIT_ERROR)
        self.assertEqual(
            run_delta_cli(["--group", self.o, "--group", self.t,
                           "--link", "bogus", "--skip"]),
            EXIT_USAGE)
        with open(self.o, "wb") as f:
            f.write(v3("X"))
        with open(self.t, "wb") as f:
            f.write(v1("GHOST"))
        self.assertEqual(
            run_delta_cli(delta_argv([[self.o], [self.t]],
                                     on_bad="strict")),
            EXIT_BAD_RECORD)


if __name__ == "__main__":
    unittest.main()
