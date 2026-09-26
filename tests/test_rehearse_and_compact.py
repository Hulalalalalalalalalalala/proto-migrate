"""Tests for rehearsal (read-only preflight) and state compaction.

Covers:
  * ``rehearse_linked_logs``: per-member migrated / bad / dropped
    counts identical to a subsequent real migration, bad-record and
    bad-reference locations in group / member / line order, the strict
    global first-error attribution, the skip-mode drop list with audit
    snippets, read-only behavior (no file touched, no scratch left),
    no blocking while an instance holds a lease or an appender writes,
    pinned-snapshot location stability, and the validation/CLI
    taxonomies;
  * ``compact_linked_state``: checkpoint logs collapse to one record
    per member and line indexes disappear regardless of row count,
    crash recovery at every compaction point with no rescan of prepared
    members, byte-identical migration output (and skip audit) versus an
    uninterrupted run, idempotency, strict-mode rejection before a
    resolved marker is published, lease exclusion and CLI exit codes;
  * ``close_linked_stream`` / idle-cap reaping: pinned descriptors are
    reclaimable once a cursor is abandoned or explicitly closed, and a
    reaped cursor still resumes its pinned snapshot.
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
from proto_migrate import (
    compact_linked_state,
    migrate_linked_logs,
    rehearse_linked_logs,
)
from proto_migrate.compaction import (
    group_resolved_path,
    member_resolved_path,
)
from proto_migrate.group_migration import GroupBadRecordError
from proto_migrate.linked_migration import (
    LinkedBadReferenceError,
    MigrationLockedError,
    _STREAM_SESSIONS,
    _linked_group_dir,
    close_linked_stream,
    read_linked_logs_stream,
)
from proto_migrate.log_migration import EXIT_BAD_RECORD, EXIT_OK

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


def read_bytes(path):
    with open(path, "rb") as f:
        return f.read()


def audit_set(stream_or_bytes):
    data = stream_or_bytes if isinstance(
        stream_or_bytes, bytes) else stream_or_bytes.getvalue()
    # Normalize the directory prefix away; compare as a set because a
    # direct run interleaves prepare-time and filter-time audits while
    # the rehearsal reports them in global order.
    lines = data.splitlines()
    return {line.split(b":", 1)[-1] for line in lines}


def leftovers(d):
    return sorted(n for n in os.listdir(d)
                  if "migrate" in n or "rehearse" in n)


def lock_artifacts(d):
    return sorted(n for n in os.listdir(d) if n.endswith(".migrate.lock"))


# ---------------------------------------------------------------------------
# Subprocess drivers
# ---------------------------------------------------------------------------


LINKED_PREPARE_CRASH = r"""
import os, sys
sys.path.insert(0, %r)
os.environ["PROTO_MIGRATE_CRASH_AT"] = "linked-prepare"
from proto_migrate.linked_migration import run_linked_cli
raise SystemExit(run_linked_cli(sys.argv[1:]))
""" % REPO_ROOT


COMPACT_RUNNER = r"""
import os, sys
sys.path.insert(0, %r)
if sys.argv[1] != "-":
    os.environ["PROTO_MIGRATE_CRASH_AT"] = sys.argv[1]
from proto_migrate.compaction import run_compact_cli
raise SystemExit(run_compact_cli(sys.argv[2:]))
""" % REPO_ROOT


def linked_argv(groups, links=("1:0:order_id:order_id"), on_bad="skip",
                segment_size=300, quiesce_ms=5):
    argv = []
    for group in groups:
        argv += ["--group", *group]
    if links:
        argv += ["--link", links] if isinstance(links, str) else \
            [a for spec in links for a in ("--link", spec)]
    argv += [f"--quiesce-ms={quiesce_ms}",
             f"--segment-size={segment_size}",
             "--skip" if on_bad == "skip" else "--strict"]
    return argv


def crash_prepare(groups, on_bad="skip", segment_size=300):
    return subprocess.run(
        [sys.executable, "-c", LINKED_PREPARE_CRASH,
         *linked_argv(groups, on_bad=on_bad, segment_size=segment_size)],
        capture_output=True)


def run_compact_cli_subprocess(groups, crash="-", on_bad="skip",
                               segment_size=300):
    return subprocess.run(
        [sys.executable, "-c", COMPACT_RUNNER, crash,
         *linked_argv(groups, on_bad=on_bad, segment_size=segment_size)],
        capture_output=True)


# ---------------------------------------------------------------------------
# Rehearsal
# ---------------------------------------------------------------------------


class TestRehearsalBasics(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        d = self.tmp.name
        self.orders = os.path.join(d, "orders.log")
        self.details = os.path.join(d, "details.log")
        with open(self.orders, "wb") as f:
            f.write(v1("A1") + v2("A2") + v1("A3", -0.0))
        with open(self.details, "wb") as f:
            f.write(v1("A1") + v1("A2"))
        self.groups = [[self.orders], [self.details]]

    def tearDown(self):
        self.tmp.cleanup()

    def test_counts_and_no_findings_on_clean_groups(self):
        rep = rehearse_linked_logs(self.groups, links=LINK, quiesce=0.01)
        per = {(m.group, m.member): m for m in rep.members}
        self.assertTrue(all(m.replaced for m in rep.members))
        self.assertEqual(per[(0, 0)].records_migrated, 3)
        self.assertEqual(per[(1, 0)].records_migrated, 2)
        self.assertEqual(rep.findings, ())
        self.assertIsNone(rep.first_error)
        self.assertEqual(rep.dropped, ())
        self.assertEqual(rep.total_migrated, 5)

    def test_canonical_member_is_untouched(self):
        from proto_migrate import dumps
        v3 = lambda oid: dumps({"order_id": oid, "amount": 3.0,
                                "status": "paid", "note": "",
                                "updated_at": 0})
        clean = os.path.join(self.tmp.name, "clean.log")
        with open(clean, "wb") as f:
            f.write(v3("A1") + v3("A2"))
        rep = rehearse_linked_logs([[clean], [self.details]],
                                   links=LINK, quiesce=0.01)
        per = {(m.group, m.member): m for m in rep.members}
        self.assertFalse(per[(0, 0)].replaced)
        self.assertEqual(per[(0, 0)].records_migrated, 0)
        self.assertTrue(per[(1, 0)].replaced)

    def test_matches_real_skip_run_member_by_member(self):
        # Add bad records and a dangling reference.
        with open(self.details, "ab") as f:
            f.write(v1("GHOST") + b"BROKEN\n")
        audit = io.BytesIO()
        real = migrate_linked_logs(
            self.groups, links=LINK, on_bad="skip", quiesce=0.01,
            audit=audit)
        # Rebuild the same input for the rehearsal (real run replaced the
        # files, so rehearse first on a fresh identical fixture).
        self._rebuild()
        rep = rehearse_linked_logs(
            self.groups, links=LINK, on_bad="skip", quiesce=0.01)
        real_per = {os.path.basename(m.path): m for m in real.members}
        rep_per = {os.path.basename(m.path): m for m in rep.members}
        for name, m in rep_per.items():
            self.assertEqual(
                m.records_migrated, real_per[name].records_migrated, name)
        self.assertEqual(rep.total_migrated, real.records_migrated)
        self.assertEqual(rep.total_bad_records, real.records_skipped)
        self.assertEqual(rep.total_bad_references,
                         real.references_bad)
        # Drop list: one bad line and one dangling-reference record.
        drops = {(os.path.basename(d.path), d.lineno, d.why)
                 for d in rep.dropped}
        self.assertEqual(
            drops,
            {("details.log", 3, "bad_reference"),
             ("details.log", 4, "bad_record")})
        # Snippets follow the 32-byte audit convention.
        dropped = {d.lineno: d for d in rep.dropped
                   if os.path.basename(d.path) == "details.log"}
        self.assertEqual(dropped[4].snippet, b"BROKEN")
        self.assertTrue(
            dropped[3].snippet.startswith(b'{"v":1,"order_id":"GHOST"'))
        self.assertLessEqual(len(dropped[3].snippet), 32)

    def _rebuild(self):
        with open(self.orders, "wb") as f:
            f.write(v1("A1") + v2("A2") + v1("A3", -0.0))
        with open(self.details, "wb") as f:
            f.write(v1("A1") + v1("A2") + v1("GHOST") + b"BROKEN\n")

    def test_findings_in_global_order(self):
        self._rebuild()
        rep = rehearse_linked_logs(
            self.groups, links=LINK, on_bad="skip", quiesce=0.01)
        locs = [(f.group, f.member, f.lineno, f.kind) for f in rep.findings]
        self.assertEqual(
            locs,
            sorted(locs, key=lambda x: (x[0], x[1], x[2],
                                        0 if x[3] == "bad_record" else 1)))
        self.assertEqual(
            [(l, k) for _g, _m, l, k in locs],
            [(3, "bad_reference"), (4, "bad_record")])

    def test_strict_first_error_bad_reference(self):
        with open(self.orders, "wb") as f:
            f.write(v1("GHOST"))
        with open(self.details, "wb") as f:
            f.write(v1("A1") + b"BROKEN\n")
        # Group 0 references group 1: its dangling ref beats group 1's
        # later bad line.
        rep = rehearse_linked_logs(
            [[self.orders], [self.details]],
            links=[(0, 1, "order_id", "order_id")], quiesce=0.01)
        fe = rep.first_error
        self.assertEqual((fe.kind, fe.group, fe.member, fe.lineno),
                         ("bad_reference", 0, 0, 1))
        self.assertIn("dangling", fe.detail)
        self.assertEqual(fe.raw, v1("GHOST"))

    def test_strict_first_error_bad_line(self):
        with open(self.orders, "wb") as f:
            f.write(v1("A1") + b"BROKEN\n")
        with open(self.details, "wb") as f:
            f.write(v1("GHOST"))
        rep = rehearse_linked_logs(self.groups, links=LINK, quiesce=0.01)
        fe = rep.first_error
        self.assertEqual((fe.kind, fe.group, fe.lineno),
                         ("bad_record", 0, 2))

    def test_illegal_version_is_bad_record(self):
        with open(self.orders, "wb") as f:
            f.write(b'{"v":9,"order_id":"X","amount":1,"tags":[]}\n')
        with open(self.details, "wb") as f:
            f.write(v1("X"))
        rep = rehearse_linked_logs(self.groups, links=LINK, quiesce=0.01)
        self.assertEqual(rep.findings[0].kind, "bad_record")
        self.assertEqual(rep.findings[0].lineno, 1)

    def test_illegal_amounts_are_bad_records(self):
        # NaN / Infinity / an overflow literal (1e999) are bad records
        # through the rehearsal entry exactly as through the migration.
        with open(self.orders, "wb") as f:
            f.write(
                b'{"v":1,"order_id":"nan","amount":NaN,"tags":[]}\n'
                b'{"v":1,"order_id":"inf","amount":Infinity,'
                b'"tags":[]}\n'
                b'{"v":1,"order_id":"big","amount":1e999,"tags":[]}\n'
            )
        with open(self.details, "wb") as f:
            f.write(v1("nan") + v1("inf") + v1("big"))
        rep = rehearse_linked_logs(self.groups, links=LINK,
                                   on_bad="skip", quiesce=0.01)
        bad = sorted(f.lineno for f in rep.findings
                     if f.kind == "bad_record" and f.group == 0)
        self.assertEqual(bad, [1, 2, 3])
        # A record naming a skipped bad line's key is a bad reference of
        # class "target skipped", located on group 1.
        refs = sorted((f.lineno, f.detail) for f in rep.findings
                      if f.kind == "bad_reference" and f.group == 1)
        self.assertEqual([line for line, _ in refs], [1, 2, 3])
        self.assertTrue(all("skipped" in detail for _, detail in refs))

    def test_negative_zero_is_preserved_not_flagged(self):
        # -0.0 is a legal amount, kept verbatim and never a finding.
        with open(self.orders, "wb") as f:
            f.write(v1("Z", -0.0))
        with open(self.details, "wb") as f:
            f.write(v1("Z"))
        rep = rehearse_linked_logs(self.groups, links=LINK, quiesce=0.01)
        self.assertEqual(rep.findings, ())
        self.assertIsNone(rep.first_error)
        # A real migration of the same pair keeps "-0.0" verbatim.
        from proto_migrate import migrate_linked_logs
        migrate_linked_logs(self.groups, links=LINK, quiesce=0.01)
        self.assertIn(b"-0.0", read_bytes(self.orders))


    def test_read_only_leaves_nothing(self):
        before = {p: read_bytes(p) for p in (self.orders, self.details)}
        rep = rehearse_linked_logs(self.groups, links=LINK,
                                   on_bad="skip", quiesce=0.01)
        self.assertTrue(rep.members)
        self.assertEqual(read_bytes(self.orders), before[self.orders])
        self.assertEqual(read_bytes(self.details), before[self.details])
        # Only reusable lock files (created by migrations, not
        # rehearsals) may remain -- a rehearsal creates none.
        self.assertEqual(
            [n for n in leftovers(self.tmp.name)
             if not n.endswith(".migrate.lock")], [])

    def test_proceeds_while_lease_held(self):
        lock = open(self.orders + ".migrate.lock", "a+b")
        fcntl.flock(lock, fcntl.LOCK_EX)
        try:
            rep = rehearse_linked_logs(self.groups, links=LINK,
                                       quiesce=0.01)
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)
            lock.close()
        self.assertEqual(rep.total_migrated, 5)

    def test_validation_matches_migration(self):
        with self.assertRaises(TypeError):
            rehearse_linked_logs(self.orders)
        with self.assertRaises(ValueError):
            rehearse_linked_logs([[]], links=[])
        with self.assertRaises(ValueError):
            rehearse_linked_logs(self.groups,
                                 links=[(9, 0, "a", "b")])
        missing = os.path.join(self.tmp.name, "nope.log")
        with self.assertRaises(FileNotFoundError):
            rehearse_linked_logs([[self.orders], [missing]])

    def test_rehearsal_concurrent_with_real_migration(self):
        # Another instance is actively migrating the same members: the
        # rehearsal must not block, must not raise MigrationLockedError,
        # and must leave the migration able to finish cleanly.
        gate = threading.Event()
        errors = []

        def migrator():
            gate.wait(5)
            try:
                migrate_linked_logs(self.groups, links=LINK,
                                    on_bad="skip", quiesce=0.02)
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        t = threading.Thread(target=migrator)
        t.start()
        gate.set()
        rep = rehearse_linked_logs(self.groups, links=LINK,
                                   on_bad="skip", quiesce=0.02)
        t.join(timeout=30)
        self.assertEqual(errors, [])
        self.assertEqual(rep.total_migrated, 5)
        # The real migration still converged to v3.
        for group in self.groups:
            recs = [json.loads(x) for x in read_bytes(group[0]).splitlines()]
            self.assertTrue(all(r["v"] == 3 for r in recs))

    def test_report_matches_real_run_with_appender_present(self):
        # An appender is continuously present during BOTH the rehearsal
        # and the real migration (run on two identical fixtures).  It
        # appends only canonical current-version records, so however
        # many land while the bounded scan runs, the bad-record /
        # bad-reference / drop classification of each run is identical
        # and neither run blocks the writer.
        from proto_migrate import dumps

        v3 = lambda oid: dumps({"order_id": oid, "amount": 3.0,
                                "status": "paid", "note": "",
                                "updated_at": 0})

        def fixture(d):
            orders = os.path.join(d, "orders.log")
            details = os.path.join(d, "details.log")
            with open(orders, "wb") as f:
                f.write(v1("A1") + v2("A2") + v1("A3", -0.0))
            with open(details, "wb") as f:
                f.write(v1("A1") + v1("GHOST") + b"BROKEN\n")
            return [[orders], [details]]

        def with_appender(groups, action):
            stop = threading.Event()
            written = []

            def appender():
                i = 0
                while not stop.is_set():
                    with open(groups[0][0], "ab") as f:
                        f.write(v3(f"L{i}"))
                    written.append(i)
                    i += 1
                    time.sleep(0.0008)

            t = threading.Thread(target=appender)
            t.start()
            try:
                return action(), written
            finally:
                stop.set()
                t.join()

        rep_dir = tempfile.TemporaryDirectory()
        real_dir = tempfile.TemporaryDirectory()
        try:
            g_rep = fixture(rep_dir.name)
            g_real = fixture(real_dir.name)
            rep, written_rep = with_appender(
                g_rep, lambda: rehearse_linked_logs(
                    g_rep, links=LINK, on_bad="skip", quiesce=0.02))
            audit = io.BytesIO()
            real, written_real = with_appender(
                g_real, lambda: migrate_linked_logs(
                    g_real, links=LINK, on_bad="skip", quiesce=0.02,
                    audit=audit))
        finally:
            rep_dir.cleanup()
            real_dir.cleanup()

        # The appender wrote during both runs (never blocked).
        self.assertTrue(written_rep)
        self.assertTrue(written_real)
        # Identical classification of the shared bad prefix.
        self.assertEqual(rep.total_bad_records, real.records_skipped)
        self.assertEqual(rep.total_bad_references,
                         real.references_bad)
        locs = sorted((f.group, f.member, f.lineno, f.kind)
                      for f in rep.findings)
        self.assertEqual(locs, [
            (1, 0, 2, "bad_reference"),
            (1, 0, 3, "bad_record"),
        ])
        drops = sorted((d.group, d.member, d.lineno, d.why)
                       for d in rep.dropped)
        self.assertEqual(drops, [
            (1, 0, 2, "bad_reference"),
            (1, 0, 3, "bad_record"),
        ])

    def test_pinned_locations_stable_under_appends(self):        # A bad line sits at a known line; an appender keeps adding
        # records during the rehearsal.  The rehearsal pins the inode,
        # so its finding keeps pointing at the original bytes.
        with open(self.orders, "ab") as f:
            f.write(b"BROKEN\n")
        stop = threading.Event()

        def appender():
            i = 0
            while not stop.is_set():
                with open(self.details, "ab") as f:
                    f.write(v1(f"L{i}"))
                i += 1
                time.sleep(0.001)

        t = threading.Thread(target=appender)
        t.start()
        try:
            rep = rehearse_linked_logs(self.groups, links=LINK,
                                       quiesce=0.02)
        finally:
            stop.set()
            t.join()
        bad = [f for f in rep.findings if f.kind == "bad_record"]
        self.assertEqual(len(bad), 1)
        self.assertEqual((bad[0].group, bad[0].lineno), (0, 4))
        self.assertEqual(bad[0].raw, b"BROKEN\n")
        # Appender was never blocked.
        self.assertGreater(len(read_bytes(self.details).splitlines()), 2)


class TestRehearsalCLI(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        d = self.tmp.name
        self.orders = os.path.join(d, "orders.log")
        self.details = os.path.join(d, "details.log")
        with open(self.orders, "wb") as f:
            f.write(v1("A1"))
        with open(self.details, "wb") as f:
            f.write(v1("A1"))

    def tearDown(self):
        self.tmp.cleanup()

    def _run(self, *extra):
        from proto_migrate.rehearsal import run_rehearse_cli
        return run_rehearse_cli([
            "--group", self.orders, "--group", self.details,
            "--link", "1:0:order_id:order_id",
            "--quiesce-ms=5", *extra])

    def test_clean_exit_zero(self):
        self.assertEqual(self._run("--skip"), EXIT_OK)

    def test_strict_exit_three(self):
        with open(self.orders, "wb") as f:
            f.write(v1("GHOST"))
        self.assertEqual(self._run("--strict"), EXIT_BAD_RECORD)

    def test_usage_exit_two(self):
        from proto_migrate.rehearsal import run_rehearse_cli
        rc = run_rehearse_cli(["--group", self.orders, "--link", "bad"])
        self.assertEqual(rc, 2)


# ---------------------------------------------------------------------------
# Compaction
# ---------------------------------------------------------------------------


def build_pair(d, n=80, bad=False):
    orders = os.path.join(d, "orders.log")
    details = os.path.join(d, "details.log")
    with open(orders, "wb") as f:
        f.write(b"".join(
            v2(f"O{i}") if i % 2 else v1(f"O{i}") for i in range(n)))
    body = b"".join(v1(f"O{i}") for i in range(n))
    if bad:
        body += v1("ZZZ") + b"BROKEN\n"
    with open(details, "wb") as f:
        f.write(body)
    return [[orders], [details]]


def cp_record_count(member_dir):
    with open(os.path.join(member_dir, "checkpoint"), "rb") as f:
        return f.read().count(b"\n") - 1


class TestCompaction(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.d = self.tmp.name

    def tearDown(self):
        self.tmp.cleanup()

    def _prepare_crashed(self, groups, on_bad="skip", segment_size=300):
        proc = crash_prepare(groups, on_bad=on_bad,
                             segment_size=segment_size)
        self.assertEqual(proc.returncode, 1, proc.stderr)

    def test_collapses_checkpoint_and_index(self):
        groups = build_pair(self.d)
        self._prepare_crashed(groups)
        mdir = groups[0][0] + ".migrate-linked-tmp"
        before = cp_record_count(mdir)
        self.assertGreater(before, 1)
        self.assertTrue(os.path.exists(os.path.join(mdir, "lines")))

        result = compact_linked_state(
            groups, links=LINK, on_bad="skip", quiesce=0.005,
            audit=io.BytesIO())
        self.assertEqual(result.members_compacted, 2)
        self.assertEqual(result.checkpoint_records_before, before * 2)
        self.assertEqual(result.checkpoint_records_after, 2)
        for group in groups:
            md = group[0] + ".migrate-linked-tmp"
            self.assertEqual(cp_record_count(md), 1)
            self.assertEqual(
                [n for n in os.listdir(md) if n.startswith("seg-")],
                ["seg-000000"])
            self.assertFalse(os.path.exists(os.path.join(md, "lines")))
            self.assertTrue(
                os.path.exists(member_resolved_path(md)))
        gdir = _linked_group_dir(groups)
        self.assertTrue(os.path.exists(group_resolved_path(gdir)))
        self.assertFalse(
            os.path.exists(os.path.join(gdir, "refs.sqlite3")))

    def test_state_is_one_record_per_member_regardless_of_rows(self):
        # 4x the rows, tiny segments: still exactly one checkpoint
        # record and no line index per member.
        groups = build_pair(self.d, n=320)
        self._prepare_crashed(groups, segment_size=120)
        compact_linked_state(groups, links=LINK, on_bad="skip",
                             quiesce=0.005, audit=io.BytesIO())
        for group in groups:
            md = group[0] + ".migrate-linked-tmp"
            self.assertEqual(cp_record_count(md), 1)
            self.assertFalse(os.path.exists(os.path.join(md, "lines")))
            # All rows survived in the staged output.
            with open(os.path.join(md, "final"), "rb") as f:
                self.assertEqual(len(f.read().splitlines()), 320)

    def test_compacted_run_byte_identical_to_uninterrupted(self):
        groups_a = build_pair(self.d, n=100, bad=True)
        with tempfile.TemporaryDirectory() as d2:
            groups_b = build_pair(d2, n=100, bad=True)
            audit_a = io.BytesIO()
            migrate_linked_logs(groups_a, links=LINK, on_bad="skip",
                                quiesce=0.005, audit=audit_a)
            self._prepare_crashed(groups_b)
            compact_linked_state(groups_b, links=LINK, on_bad="skip",
                                 quiesce=0.005, audit=io.BytesIO())
            audit_b = io.BytesIO()
            res = migrate_linked_logs(groups_b, links=LINK,
                                      on_bad="skip", quiesce=0.005,
                                      audit=audit_b)
            self.assertEqual(res.records_migrated, 0)
            self.assertEqual(
                read_bytes(groups_a[0][0]), read_bytes(groups_b[0][0]))
            self.assertEqual(
                read_bytes(groups_a[1][0]), read_bytes(groups_b[1][0]))
            norm = lambda b: b.replace(self.d.encode(), b"D").replace(
                d2.encode(), b"D")
            self.assertEqual(norm(audit_b.getvalue()),
                             norm(audit_a.getvalue()))
            self.assertEqual(leftovers(d2), lock_artifacts(d2))

    def test_crash_at_every_compaction_point_recovers(self):
        groups = build_pair(self.d, n=120)
        self._prepare_crashed(groups, segment_size=200)
        for point in ("compact-prepare", "compact-filter",
                      "compact-resolve"):
            proc = run_compact_cli_subprocess(
                groups, crash=point, segment_size=200)
            self.assertEqual(proc.returncode, 1, proc.stderr)
        result = compact_linked_state(
            groups, links=LINK, on_bad="skip", quiesce=0.005,
            audit=io.BytesIO())
        self.assertEqual(result.members_compacted, 2)
        # The finishing migration rescans nothing and loses nothing.
        calls = []
        original = lm.loads
        lm.loads = lambda raw: (calls.append(raw), original(raw))[1]
        try:
            migrate_linked_logs(groups, links=LINK, on_bad="skip",
                                quiesce=0.005, audit=io.BytesIO())
        finally:
            lm.loads = original
        self.assertEqual(calls, [])
        for group in groups:
            recs = [json.loads(x)
                    for x in read_bytes(group[0]).splitlines()]
            self.assertEqual(len(recs), 120)
            self.assertTrue(all(r["v"] == 3 for r in recs))

    def test_idempotent_compaction(self):
        groups = build_pair(self.d)
        self._prepare_crashed(groups)
        first = compact_linked_state(
            groups, links=LINK, on_bad="skip", quiesce=0.005,
            audit=io.BytesIO())
        second = compact_linked_state(
            groups, links=LINK, on_bad="skip", quiesce=0.005,
            audit=io.BytesIO())
        # Second run sees the already-resolved state; cp stays at one
        # record per member and the decision is not republished.
        self.assertEqual(first.checkpoint_records_after, 2)
        self.assertEqual(second.checkpoint_records_before, 2)
        self.assertEqual(second.checkpoint_records_after, 2)

    def test_compaction_prepares_without_prior_crash(self):
        # Compaction drives prepare itself on a pristine fixture.
        groups = build_pair(self.d, n=60)
        compact_linked_state(groups, links=LINK, on_bad="skip",
                             quiesce=0.005, audit=io.BytesIO())
        res = migrate_linked_logs(groups, links=LINK, on_bad="skip",
                                  quiesce=0.005, audit=io.BytesIO())
        self.assertTrue(res.replaced)
        for group in groups:
            recs = [json.loads(x)
                    for x in read_bytes(group[0]).splitlines()]
            self.assertEqual(len(recs), 60)

    def test_strict_compaction_rejects_before_resolution(self):
        with open(os.path.join(self.d, "orders.log"), "wb") as f:
            f.write(v1("A1") + b"BROKEN\n")
        with open(os.path.join(self.d, "details.log"), "wb") as f:
            f.write(v1("GHOST"))
        groups = [[os.path.join(self.d, "orders.log")],
                  [os.path.join(self.d, "details.log")]]
        with self.assertRaises(GroupBadRecordError):
            compact_linked_state(groups, links=LINK, quiesce=0.005)
        gdir = _linked_group_dir(groups)
        # No resolved marker published; originals untouched.
        self.assertFalse(os.path.exists(group_resolved_path(gdir)))
        self.assertTrue(read_bytes(groups[0][0]).endswith(b"BROKEN\n"))

    def test_strict_bad_reference_compaction(self):
        with open(os.path.join(self.d, "orders.log"), "wb") as f:
            f.write(v1("GHOST"))
        with open(os.path.join(self.d, "details.log"), "wb") as f:
            f.write(v1("A1") + b"BROKEN\n")
        groups = [[os.path.join(self.d, "orders.log")],
                  [os.path.join(self.d, "details.log")]]
        with self.assertRaises(LinkedBadReferenceError):
            compact_linked_state(
                groups, links=[(0, 1, "order_id", "order_id")],
                quiesce=0.005)

    def test_lease_held_blocks_compaction(self):
        groups = build_pair(self.d)
        lock = open(groups[0][0] + ".migrate.lock", "a+b")
        fcntl.flock(lock, fcntl.LOCK_EX)
        try:
            with self.assertRaises(MigrationLockedError):
                compact_linked_state(groups, links=LINK, quiesce=0.005)
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)
            lock.close()

    def test_compact_cli_exit_codes(self):
        groups = build_pair(self.d)
        self._prepare_crashed(groups)
        proc = run_compact_cli_subprocess(groups)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        # Missing member -> 1.
        proc = subprocess.run(
            [sys.executable, "-c", COMPACT_RUNNER, "-",
             "--group", os.path.join(self.d, "missing.log"),
             "--group", groups[1][0], "--link",
             "1:0:order_id:order_id", "--skip", "--quiesce-ms=5"],
            capture_output=True)
        self.assertEqual(proc.returncode, 1)


# ---------------------------------------------------------------------------
# Streaming snapshot descriptor reclamation
# ---------------------------------------------------------------------------


class TestStreamReclamation(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        d = self.tmp.name
        self.orders = os.path.join(d, "orders.log")
        self.details = os.path.join(d, "details.log")
        with open(self.orders, "wb") as f:
            f.write(b"".join(v1(f"O{i}") for i in range(20)))
        with open(self.details, "wb") as f:
            f.write(b"".join(v1(f"O{i}") for i in range(20)))
        self.groups = [[self.orders], [self.details]]

    def tearDown(self):
        for session in list(_STREAM_SESSIONS.values()):
            for fd in session["fds"]:
                if fd is not None:
                    try:
                        fd.close()
                    except OSError:
                        pass
        _STREAM_SESSIONS.clear()
        self.tmp.cleanup()

    def test_explicit_close_frees_descriptors(self):
        batch, cursor = read_linked_logs_stream(
            self.groups, None, batch_records=5, quiesce=0.01)
        self.assertEqual(len(batch), 5)
        token = __import__("json").loads(cursor)["tok"]
        session = _STREAM_SESSIONS[token]
        self.assertTrue(any(fd is not None and not fd.closed
                            for fd in session["fds"]))
        close_linked_stream(cursor)
        self.assertNotIn(token, _STREAM_SESSIONS)
        self.assertTrue(all(fd is None or fd.closed
                            for fd in session["fds"]))
        # Idempotent and None is a no-op.
        close_linked_stream(cursor)
        close_linked_stream(None)

    def test_close_validates_cursor(self):
        with self.assertRaises(TypeError):
            close_linked_stream(42)
        with self.assertRaises(ValueError):
            close_linked_stream("not a cursor")

    def test_idle_reaping_closes_descriptors(self):
        import proto_migrate.linked_migration as lm_mod
        old_env = os.environ.pop("PROTO_MIGRATE_STREAM_IDLE_MS", None)
        os.environ["PROTO_MIGRATE_STREAM_IDLE_MS"] = "0"
        try:
            _batch, cursor = read_linked_logs_stream(
                self.groups, None, batch_records=5, quiesce=0.01)
            token = __import__("json").loads(cursor)["tok"]
            session = _STREAM_SESSIONS[token]
            # Opening any new session reaps the idle one.
            read_linked_logs_stream(self.groups, None,
                                    batch_records=5, quiesce=0.01)
            self.assertNotIn(token, _STREAM_SESSIONS)
            self.assertTrue(all(fd is None or fd.closed
                                for fd in session["fds"]))
        finally:
            if old_env is None:
                os.environ.pop("PROTO_MIGRATE_STREAM_IDLE_MS", None)
            else:
                os.environ["PROTO_MIGRATE_STREAM_IDLE_MS"] = old_env

    def test_reaped_cursor_still_resumes(self):
        import proto_migrate.linked_migration as lm_mod
        old_env = os.environ.pop("PROTO_MIGRATE_STREAM_IDLE_MS", None)
        os.environ["PROTO_MIGRATE_STREAM_IDLE_MS"] = "0"
        try:
            batch1, cursor = read_linked_logs_stream(
                self.groups, None, batch_records=10, quiesce=0.01)
            # Reap it, then resume from the persisted cursor: it rebuilds
            # the pinned (inode, length) snapshot and continues.
            for token, session in list(_STREAM_SESSIONS.items()):
                for fd in session["fds"]:
                    if fd is not None:
                        fd.close()
                _STREAM_SESSIONS.pop(token)
            rest = []
            while cursor is not None:
                batch, cursor = read_linked_logs_stream(
                    self.groups, cursor, batch_records=10, quiesce=0.01)
                rest.extend(batch)
            self.assertEqual(len(batch1) + len(rest), 40)
        finally:
            if old_env is None:
                os.environ.pop("PROTO_MIGRATE_STREAM_IDLE_MS", None)
            else:
                os.environ["PROTO_MIGRATE_STREAM_IDLE_MS"] = old_env

    def test_cap_evicts_oldest_sessions(self):
        import proto_migrate.linked_migration as lm_mod
        old_cap = lm_mod._STREAM_MAX_SESSIONS
        old_env = os.environ.pop("PROTO_MIGRATE_STREAM_IDLE_MS", None)
        os.environ["PROTO_MIGRATE_STREAM_IDLE_MS"] = "1000000"
        lm_mod._STREAM_MAX_SESSIONS = 3
        try:
            cursors = []
            for _ in range(5):
                _batch, cursor = read_linked_logs_stream(
                    self.groups, None, batch_records=2, quiesce=0.01)
                cursors.append(cursor)
            self.assertLessEqual(len(_STREAM_SESSIONS), 3)
            # The three newest survive.
            for cursor in cursors[-3:]:
                token = __import__("json").loads(cursor)["tok"]
                self.assertIn(token, _STREAM_SESSIONS)
        finally:
            lm_mod._STREAM_MAX_SESSIONS = old_cap
            if old_env is None:
                os.environ.pop("PROTO_MIGRATE_STREAM_IDLE_MS", None)
            else:
                os.environ["PROTO_MIGRATE_STREAM_IDLE_MS"] = old_env


if __name__ == "__main__":
    unittest.main()
