"""Tests for multi-instance coordinated linked migrations.

Covers:
  * overlapping group sets share member leases: a held lease raises
    MigrationLockedError and a holder that exits (or is killed) lets the
    waiting instance take over deterministically;
  * disjoint specs proceed independently;
  * a real os._exit crash mid-protocol is taken over by a second
    instance from the durable checkpoints with no rescan, finishing
    byte-for-byte identical to one serial run;
  * counters of a takeover/idempotent rerun are zero and match the
    per-member totals;
  * running an overlapping batch equals running the same specs serially.
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

from proto_migrate import (
    MigrationLockedError,
    MigrationSpec,
    run_coordinated_migrations,
)
from proto_migrate.coordinated_migration import (
    acquire_member_leases,
    migrate_linked_logs_coordinated,
    spec_work_dir,
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


def read_bytes(path):
    with open(path, "rb") as f:
        return f.read()


def ids_of(blob):
    return [json.loads(line)["order_id"]
            for line in blob.splitlines() if line]


LINK = [(1, 0, "order_id", "order_id")]


COORD_RUNNER = r"""
import os, sys, json
sys.path.insert(0, %r)
if sys.argv[1] != "-":
    os.environ["PROTO_MIGRATE_CRASH_AT"] = sys.argv[1]
from proto_migrate import run_coordinated_migrations, MigrationSpec
specs = json.loads(sys.argv[2])
ms = [MigrationSpec(s["groups"], [tuple(l) for l in s["links"]],
                    s["on_bad"]) for s in specs]
res = run_coordinated_migrations(ms, quiesce=0.005, segment_size=4096)
print(json.dumps([{"replaced": r.result.replaced,
                   "migrated": r.result.records_migrated,
                   "skipped": r.result.records_skipped,
                   "took_over": r.took_over} for r in res]))
""" % REPO_ROOT


def run_coord(groups_specs, crash="-"):
    payload = [
        {"groups": groups, "links": links, "on_bad": on_bad}
        for groups, links, on_bad in groups_specs
    ]
    return subprocess.run(
        [sys.executable, "-c", COORD_RUNNER, crash, json.dumps(payload)],
        capture_output=True,
    )


HOLDER_RUNNER = r"""
import sys, time, os
sys.path.insert(0, %r)
from proto_migrate.coordinated_migration import acquire_member_leases
import json
paths = json.loads(sys.argv[1])
leases = acquire_member_leases(paths, "external-holder")
sys.stdout.write("held\n")
sys.stdout.flush()
time.sleep(float(sys.argv[2]))
""" % REPO_ROOT


class TestCoordination(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        d = self.tmp.name
        self.a = os.path.join(d, "a.log")
        self.b = os.path.join(d, "b.log")
        self.c = os.path.join(d, "c.log")
        for path, key in ((self.a, "A"), (self.b, "A"), (self.c, "C")):
            with open(path, "wb") as f:
                f.write(b"".join(v1(f"{key}{i}") for i in range(10)))

    def tearDown(self):
        self.tmp.cleanup()

    def test_held_lease_raises_locked(self):
        leases = acquire_member_leases([self.a], "holder")
        try:
            with self.assertRaises(MigrationLockedError) as cm:
                migrate_linked_logs_coordinated(
                    MigrationSpec([[self.a]], links=[], on_bad="skip"),
                    instance_id="other", quiesce=0.01,
                )
            self.assertEqual(cm.exception.holder, "holder")
        finally:
            for lease in leases:
                lease.release()

    def test_lock_is_runtime_error(self):
        self.assertTrue(issubclass(MigrationLockedError, RuntimeError))

    def test_released_lease_is_reacquirable(self):
        leases = acquire_member_leases([self.a], "holder")
        for lease in leases:
            lease.release()
        again = acquire_member_leases([self.a], "holder2")
        for lease in again:
            lease.release()

    def test_overlapping_batch_succeeds_and_migrates(self):
        spec0 = MigrationSpec([[self.a], [self.b]], links=LINK,
                              on_bad="skip")
        spec1 = MigrationSpec([[self.b], [self.c]], links=LINK,
                              on_bad="skip")
        results = run_coordinated_migrations(
            [spec0, spec1], quiesce=0.01, audit=io.BytesIO()
        )
        self.assertEqual(len(results), 2)
        self.assertTrue(all(r.result.replaced for r in results))
        for path in (self.a, self.b, self.c):
            self.assertTrue(
                all(json.loads(line)["v"] == 3
                    for line in read_bytes(path).splitlines())
            )

    def test_overlapping_batch_equals_serial(self):
        # A copy migrated by the same two specs one at a time.
        clone = tempfile.mkdtemp(dir=self.tmp.name)
        clones = []
        for src in (self.a, self.b, self.c):
            dst = os.path.join(clone, os.path.basename(src) + ".clone")
            with open(dst, "wb") as f:
                f.write(read_bytes(src))
            clones.append(dst)
        ca, cb, cc = clones
        run_coordinated_migrations([
            MigrationSpec([[ca], [cb]], links=LINK, on_bad="skip"),
        ], quiesce=0.01, audit=io.BytesIO())
        run_coordinated_migrations([
            MigrationSpec([[cb], [cc]], links=LINK, on_bad="skip"),
        ], quiesce=0.01, audit=io.BytesIO())

        run_coordinated_migrations([
            MigrationSpec([[self.a], [self.b]], links=LINK, on_bad="skip"),
            MigrationSpec([[self.b], [self.c]], links=LINK, on_bad="skip"),
        ], quiesce=0.01, audit=io.BytesIO())
        self.assertEqual(read_bytes(ca), read_bytes(self.a))
        self.assertEqual(read_bytes(cb), read_bytes(self.b))
        self.assertEqual(read_bytes(cc), read_bytes(self.c))

    def test_takeover_after_external_holder_exits(self):
        # A foreign process holds the member leases then dies; the batch
        # waits (it sees MigrationLockedError internally) and proceeds.
        holder = subprocess.Popen(
            [sys.executable, "-c", HOLDER_RUNNER,
             json.dumps([self.a, self.b]), "0.6"],
            stdout=subprocess.PIPE,
        )
        self.assertEqual(holder.stdout.readline().strip(), b"held")
        spec = MigrationSpec([[self.a], [self.b]], links=LINK,
                             on_bad="skip")
        start = time.time()
        results = run_coordinated_migrations(
            [spec], wait_timeout=10.0, quiesce=0.01, audit=io.BytesIO()
        )
        self.assertGreater(time.time() - start, 0.4)
        self.assertTrue(results[0].result.replaced)
        holder.wait(timeout=5)
        holder.stdout.close()

    def test_wait_timeout_raises(self):
        holder = subprocess.Popen(
            [sys.executable, "-c", HOLDER_RUNNER,
             json.dumps([self.a]), "10"],
            stdout=subprocess.PIPE,
        )
        self.assertEqual(holder.stdout.readline().strip(), b"held")
        try:
            with self.assertRaises(MigrationLockedError):
                run_coordinated_migrations([
                    MigrationSpec([[self.a]], links=[], on_bad="skip")
                ], wait_timeout=0.3, poll_interval=0.02,
                    quiesce=0.01, audit=io.BytesIO())
        finally:
            holder.kill()
            holder.wait(timeout=5)
            holder.stdout.close()


class TestCoordinatedCrashTakeover(unittest.TestCase):
    SIZES = 300

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        d = self.tmp.name
        self.a = os.path.join(d, "a.log")
        self.b = os.path.join(d, "b.log")
        with open(self.a, "wb") as f:
            f.write(b"".join(v1(f"A{i}") for i in range(self.SIZES)))
        with open(self.b, "wb") as f:
            f.write(b"".join(v1(f"A{i}") for i in range(self.SIZES)))
        self.specs = [
            ([[self.a], [self.b]], LINK, "skip"),
        ]

    def tearDown(self):
        self.tmp.cleanup()

    def _reference(self):
        ref = tempfile.mkdtemp(dir=self.tmp.name)
        ra = os.path.join(ref, "ra.log")
        rb = os.path.join(ref, "rb.log")
        with open(ra, "wb") as f:
            f.write(b"".join(v1(f"A{i}") for i in range(self.SIZES)))
        with open(rb, "wb") as f:
            f.write(b"".join(v1(f"A{i}") for i in range(self.SIZES)))
        proc = run_coord([([[ra], [rb]], LINK, "skip")])
        self.assertEqual(proc.returncode, 0, proc.stderr)
        return read_bytes(ra), read_bytes(rb)

    def test_crash_then_takeover_byte_identical(self):
        reference = self._reference()
        for point in ("linked-prepare", "linked-stage", "linked-marker"):
            with self.subTest(point=point):
                # Reset members.
                with open(self.a, "wb") as f:
                    f.write(b"".join(v1(f"A{i}")
                                     for i in range(self.SIZES)))
                with open(self.b, "wb") as f:
                    f.write(b"".join(v1(f"A{i}")
                                     for i in range(self.SIZES)))
                crashed = run_coord(self.specs, crash=point)
                self.assertEqual(crashed.returncode, 1,
                                 (point, crashed.stderr))
                # Leases are kernel-released on death; a fresh instance
                # takes over.
                takeover = run_coord(self.specs)
                self.assertEqual(takeover.returncode, 0,
                                 (point, takeover.stderr))
                info = json.loads(takeover.stdout)
                # A crash at/after marker publication is finished via the
                # committed path (no further rename); earlier points
                # resume the staging and report replaced.
                if point != "linked-marker":
                    self.assertTrue(info[0]["replaced"])
                self.assertEqual(info[0]["migrated"], 0)
                self.assertTrue(info[0]["took_over"])
                self.assertEqual(read_bytes(self.a), reference[0])
                self.assertEqual(read_bytes(self.b), reference[1])
                # Idempotent third run.
                again = run_coord(self.specs)
                info = json.loads(again.stdout)
                self.assertFalse(info[0]["replaced"])
                self.assertEqual(info[0]["migrated"], 0)

    def test_disjoint_specs_each_own_work_dir(self):
        d = self.tmp.name
        x = os.path.join(d, "x.log")
        with open(x, "wb") as f:
            f.write(b"".join(v1("X%d" % i) for i in range(5)))
        s0 = MigrationSpec([[self.a], [self.b]], links=LINK, on_bad="skip")
        s1 = MigrationSpec([[x]], links=[], on_bad="skip")
        wd0 = spec_work_dir(s0.groups, s0.links, s0.on_bad)
        wd1 = spec_work_dir(s1.groups, s1.links, s1.on_bad)
        self.assertNotEqual(wd0, wd1)
        results = run_coordinated_migrations(
            [s0, s1], quiesce=0.01, audit=io.BytesIO()
        )
        self.assertTrue(all(r.result.replaced for r in results))


if __name__ == "__main__":
    unittest.main()
