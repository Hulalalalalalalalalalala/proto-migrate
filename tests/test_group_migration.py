"""Tests for the all-or-nothing group log migration.

Covers:
  * group atomicity (any pre-commit failure rolls the whole group back
    without touching any original file),
  * the recoverable two-phase rename protocol (kill before/after the
    group commit record; rerun completes renames or resumes checkpoints,
    byte-identical to one uninterrupted run),
  * group resume not rescanning/rewriting prepared members and the
    summary counting only records newly migrated this run,
  * consistent group snapshots via read_log_group,
  * manifest validation, bad-record policy and CLI exit codes.
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
from proto_migrate import CURRENT_VERSION, dumps
from proto_migrate.log_migration import (
    EXIT_BAD_RECORD,
    EXIT_ERROR,
    EXIT_OK,
    EXIT_USAGE,
    BadRecordError,
    migrate_log_group,
    read_log,
    read_log_group,
    run_group_cli,
)
from proto_migrate import migrate_log_file

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


# ---------------------------------------------------------------------------
# Record builders and helpers
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


def v3(order_id):
    return dumps({
        "order_id": order_id,
        "amount": 3.5,
        "status": "new",
        "note": "",
        "updated_at": 0,
    })


def read_bytes(path):
    with open(path, "rb") as f:
        return f.read()


def versions_of(blob):
    return [json.loads(line)["v"] for line in blob.splitlines() if line]


GROUP_RUNNER = r"""
import os, sys
sys.path.insert(0, %r)
if sys.argv[1] != "-":
    os.environ["PROTO_MIGRATE_CRASH_AT"] = sys.argv[1]
if sys.argv[2] != "-":
    os.environ["PROTO_MIGRATE_FAULT_AT"] = sys.argv[2]
from proto_migrate.log_migration import run_group_cli
flag = "--skip" if sys.argv[3] == "skip" else "--strict"
raise SystemExit(run_group_cli(sys.argv[6:] + [flag,
                          "--quiesce-ms", sys.argv[4],
                          "--segment-size", sys.argv[5]]))
""" % REPO_ROOT


def run_group_migrator(paths, crash="-", fault="-", on_bad="strict",
                       quiesce_ms="20", segment_size=str(16 * 1024 * 1024)):
    return subprocess.run(
        [sys.executable, "-c", GROUP_RUNNER, crash, fault, on_bad,
         quiesce_ms, segment_size] + list(paths),
        capture_output=True,
    )


def reference_migration(blob, on_bad="strict"):
    """One-shot, crash-free single-file migration; returns final bytes."""
    with tempfile.TemporaryDirectory() as d:
        p = os.path.join(d, "ref.log")
        with open(p, "wb") as f:
            f.write(blob)
        migrate_log_file(p, on_bad=on_bad, quiesce=0.01)
        return read_bytes(p)


class GroupBase(unittest.TestCase):
    N = 3

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = self.tmp.name
        self.paths = [os.path.join(self.dir, f"log{i}.jsonl")
                      for i in range(self.N)]
        self.blobs = [
            b"".join(v1(f"a-{i}") for i in range(12)),
            b"".join(v2(f"b-{i}") for i in range(7)),
            b"".join(v1(f"c-{i}") for i in range(5))
            + b"".join(v3(f"d-{i}") for i in range(3)),
        ]
        for p, blob in zip(self.paths, self.blobs):
            with open(p, "wb") as f:
                f.write(blob)

    def tearDown(self):
        self.tmp.cleanup()

    def references(self):
        return [reference_migration(b) for b in self.blobs]

    def assert_group_clean(self):
        for p in self.paths:
            self.assertFalse(os.path.exists(p + ".migrate-tmp"), p)
        self.assertFalse(
            os.path.exists(os.path.abspath(self.paths[0])
                           + ".migrate-group"))


# ---------------------------------------------------------------------------
# 1. Basic group migration
# ---------------------------------------------------------------------------


class TestGroupBasic(GroupBase):
    def test_migrates_every_member_atomically(self):
        refs = self.references()
        result = migrate_log_group(self.paths, quiesce=0.01)
        self.assertEqual(result.replaced, self.N)
        self.assertEqual(result.records_migrated, 12 + 7 + 8)
        self.assertEqual(result.records_skipped, 0)
        self.assertIsNone(result.post_commit_error)
        self.assertEqual(len(result.results), self.N)
        for p, ref, member in zip(self.paths, refs, result.results):
            self.assertEqual(read_bytes(p), ref)
            self.assertTrue(member.replaced)
            self.assertTrue(os.path.exists(p + ".committed"))
        self.assert_group_clean()

    def test_clean_group_is_left_untouched(self):
        migrate_log_group(self.paths, quiesce=0.01)
        before = [read_bytes(p) for p in self.paths]
        mtimes = [os.stat(p).st_mtime_ns for p in self.paths]
        result = migrate_log_group(self.paths, quiesce=0.01)
        self.assertEqual(result.replaced, 0)
        self.assertEqual([read_bytes(p) for p in self.paths], before)
        self.assertEqual([os.stat(p).st_mtime_ns for p in self.paths],
                         mtimes)
        self.assert_group_clean()

    def test_partially_clean_group(self):
        # Member 1 is already canonical; only members 0 and 2 need work.
        with open(self.paths[1], "wb") as f:
            f.write(b"".join(v3(f"b-{i}") for i in range(4)))
        before_clean = read_bytes(self.paths[1])
        result = migrate_log_group(self.paths, quiesce=0.01)
        self.assertEqual(result.replaced, 2)
        self.assertFalse(result.results[1].replaced)
        self.assertEqual(read_bytes(self.paths[1]), before_clean)
        self.assertFalse(os.path.exists(self.paths[1] + ".committed"))
        for p in (self.paths[0], self.paths[2]):
            self.assertTrue(os.path.exists(p + ".committed"))
        self.assert_group_clean()

    def test_single_member_group(self):
        result = migrate_log_group([self.paths[0]], quiesce=0.01)
        self.assertEqual(result.replaced, 1)
        self.assertEqual(read_bytes(self.paths[0]),
                         reference_migration(self.blobs[0]))

    def test_members_in_different_directories(self):
        other = os.path.join(self.dir, "sub")
        os.makedirs(other)
        p = os.path.join(other, "elsewhere.jsonl")
        with open(p, "wb") as f:
            f.write(v1("x"))
        result = migrate_log_group([self.paths[0], p], quiesce=0.01)
        self.assertEqual(result.replaced, 2)
        self.assertEqual(versions_of(read_bytes(p)), [3])

    def test_negative_zero_amount_preserved(self):
        with open(self.paths[0], "wb") as f:
            f.write(b'{"v":1,"order_id":"z","amount":-0.0,"tags":[]}\n')
        migrate_log_group(self.paths, quiesce=0.01)
        self.assertIn(b"-0.0", read_bytes(self.paths[0]))


# ---------------------------------------------------------------------------
# 2. All-or-nothing rollback
# ---------------------------------------------------------------------------


class TestGroupRollback(GroupBase):
    def test_strict_bad_record_rolls_back_whole_group(self):
        # The bad line sits in the LAST member: members 0 and 1 are
        # fully prepared when the group aborts.
        with open(self.paths[2], "ab") as f:
            f.write(b"not json\n")
        before = [read_bytes(p) for p in self.paths]
        with self.assertRaises(BadRecordError):
            migrate_log_group(self.paths, quiesce=0.01)
        self.assertEqual([read_bytes(p) for p in self.paths], before)
        self.assert_group_clean()
        for p in self.paths:
            self.assertFalse(os.path.exists(p + ".committed"), p)

    def test_strict_bad_version_key_rolls_back(self):
        for bad_line in (
            b'{"order_id":"x","amount":1}\n',          # missing v
            b'{"v":"3","order_id":"x","amount":1}\n',  # non-integer v
            b'{"v":9,"order_id":"x","amount":1}\n',    # unsupported v
            b'{"v":1,"order_id":"x","amount":1e999,"tags":[]}\n',
            b'{"v":1,"order_id":"x","amount":NaN,"tags":[]}\n',
        ):
            with self.subTest(bad_line=bad_line):
                with open(self.paths[1], "wb") as f:
                    f.write(v1("ok") + bad_line)
                before = [read_bytes(p) for p in self.paths]
                with self.assertRaises(ValueError):
                    migrate_log_group(self.paths, quiesce=0.01)
                self.assertEqual([read_bytes(p) for p in self.paths],
                                 before)
                self.assert_group_clean()

    def test_missing_member_aborts_before_any_work(self):
        before = [read_bytes(p) for p in self.paths]
        with self.assertRaises(FileNotFoundError):
            migrate_log_group(self.paths + [self.paths[0] + ".gone"],
                              quiesce=0.01)
        self.assertEqual([read_bytes(p) for p in self.paths], before)
        self.assert_group_clean()

    def test_io_error_mid_group_rolls_back(self):
        # Member 1 is a directory: the manifest pre-check rejects it
        # before any member is touched.
        os.unlink(self.paths[1])
        os.mkdir(self.paths[1])
        before0 = read_bytes(self.paths[0])
        before2 = read_bytes(self.paths[2])
        with self.assertRaises(FileNotFoundError):
            migrate_log_group(self.paths, quiesce=0.01)
        self.assertEqual(read_bytes(self.paths[0]), before0)
        self.assertEqual(read_bytes(self.paths[2]), before2)
        self.assertFalse(os.path.exists(self.paths[0] + ".migrate-tmp"))
        self.assertFalse(os.path.exists(self.paths[2] + ".migrate-tmp"))


# ---------------------------------------------------------------------------
# 3. Manifest validation and entry-point errors
# ---------------------------------------------------------------------------


class TestManifestValidation(GroupBase):
    def test_non_sequence_manifest_raises_type_error(self):
        for bad in (None, 42, self.paths[0],
                    (p for p in self.paths), {"a": 1}):
            with self.subTest(bad=type(bad).__name__):
                with self.assertRaises(TypeError):
                    migrate_log_group(bad)
                with self.assertRaises(TypeError):
                    read_log_group(bad)

    def test_empty_manifest_raises_value_error(self):
        with self.assertRaises(ValueError):
            migrate_log_group([])
        with self.assertRaises(ValueError):
            read_log_group([])

    def test_duplicate_paths_raise_value_error(self):
        with self.assertRaises(ValueError):
            migrate_log_group([self.paths[0], self.paths[0]])
        # Duplicates after path normalization count too.
        rel = os.path.relpath(self.paths[1])
        with self.assertRaises(ValueError):
            read_log_group([self.paths[1], rel])
        with self.assertRaises(ValueError):
            read_log_group([self.paths[1], self.paths[1]])

    def test_missing_or_unreadable_member_raises_filenotfound(self):
        gone = os.path.join(self.dir, "gone.jsonl")
        with self.assertRaises(FileNotFoundError):
            migrate_log_group([self.paths[0], gone])
        with self.assertRaises(FileNotFoundError):
            read_log_group([self.paths[0], gone])

    def test_bad_on_bad_and_segment_size_rejected(self):
        with self.assertRaises(ValueError):
            migrate_log_group(self.paths, on_bad="explode")
        with self.assertRaises(ValueError):
            migrate_log_group(self.paths, segment_size=0)


# ---------------------------------------------------------------------------
# 4. Bad-record policy
# ---------------------------------------------------------------------------


class TestGroupBadRecords(GroupBase):
    def test_strict_stops_at_first_bad_line_of_group(self):
        with open(self.paths[0], "ab") as f:
            f.write(b"garbage\n")
            f.write(b'{"v":1,"order_id":"after","amount":1,"tags":[]}\n')
        with self.assertRaises(BadRecordError) as ctx:
            migrate_log_group(self.paths, quiesce=0.01)
        self.assertEqual(ctx.exception.lineno, 13)

    def test_skip_audits_file_line_and_snippet(self):
        with open(self.paths[0], "ab") as f:
            f.write(b"garbage\n")
        with open(self.paths[2], "ab") as f:
            f.write(b'{"v":7}\n')
        audit = io.BytesIO()
        result = migrate_log_group(self.paths, on_bad="skip",
                                   quiesce=0.01, audit=audit)
        self.assertEqual(result.records_skipped, 2)
        lines = audit.getvalue().splitlines()
        self.assertEqual(len(lines), 2)
        self.assertEqual(lines[0],
                         self.paths[0].encode() + b":13:garbage")
        self.assertEqual(lines[1],
                         self.paths[2].encode() + b":9:" + b'{"v":7}')
        # Bad lines are gone; everything else migrated.
        for p in self.paths:
            self.assertTrue(all(v == 3 for v in versions_of(read_bytes(p))))
        self.assert_group_clean()

    def test_audit_snippet_truncated_at_32_bytes(self):
        long_line = b'{"v":"x","order_id":"' + b"y" * 100 + b'"}\n'
        with open(self.paths[0], "ab") as f:
            f.write(long_line)
        audit = io.BytesIO()
        migrate_log_group(self.paths, on_bad="skip", quiesce=0.01,
                          audit=audit)
        entry = audit.getvalue().splitlines()[0]
        snippet = entry.split(b":", 2)[2]
        self.assertEqual(snippet, long_line[:32])


# ---------------------------------------------------------------------------
# 5. Crash injection and recovery of the two-phase protocol
# ---------------------------------------------------------------------------


class TestGroupCrashRecovery(GroupBase):
    SEG = "128"

    def _crash(self, point, on_bad="strict"):
        return run_group_migrator(self.paths, crash=point, on_bad=on_bad,
                                  segment_size=self.SEG)

    def _finish(self):
        proc = run_group_migrator(self.paths)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        return proc

    def test_each_crash_point_recovers_byte_identical(self):
        refs = self.references()
        for point in ("group-lock", "group-prepare", "group-segments",
                      "group-commit", "group-rename", "group-converge",
                      "group-cleanup"):
            with self.subTest(point=point):
                for p, blob in zip(self.paths, self.blobs):
                    with open(p, "wb") as f:
                        f.write(blob)
                    for suffix in (".migrate-tmp", ".committed",
                                   ".migrate.lock"):
                        try:
                            os.remove(p + suffix)
                        except (FileNotFoundError, IsADirectoryError):
                            pass
                proc = self._crash(point)
                self.assertNotEqual(proc.returncode, 0, proc.stderr)
                self._finish()
                for p, ref in zip(self.paths, refs):
                    self.assertEqual(read_bytes(p), ref, point)
                self.assert_group_clean()

    def test_crash_mid_rename_sequence_completes_remaining_renames(self):
        # Killed right after the first member's rename: the commit
        # record is durable, so the rerun must finish the renames, not
        # roll anything back.
        refs = self.references()
        proc = self._crash("group-rename")
        self.assertNotEqual(proc.returncode, 0)
        group_dir = os.path.abspath(self.paths[0]) + ".migrate-group"
        self.assertTrue(os.path.exists(
            os.path.join(group_dir, "commit")))
        self._finish()
        for p, ref in zip(self.paths, refs):
            self.assertEqual(read_bytes(p), ref)
        self.assert_group_clean()

    def test_prepared_members_are_not_rescanned(self):
        # Killed after member 0 was fully prepared: the rerun neither
        # rescans nor rewrites it, and the summary counts only records
        # migrated during the rerun itself.
        proc = self._crash("group-prepare")
        self.assertNotEqual(proc.returncode, 0)
        result = migrate_log_group(self.paths, quiesce=0.01)
        self.assertEqual(result.results[0].records_migrated, 0)
        self.assertEqual(result.results[1].records_migrated, 7)
        self.assertEqual(result.results[2].records_migrated, 8)
        for p, ref in zip(self.paths, self.references()):
            self.assertEqual(read_bytes(p), ref)

    def test_summary_counts_only_records_migrated_this_run(self):
        # Kill during the scan (after a durable rotation checkpoint):
        # the rerun's summary excludes the checkpointed prefix.
        proc = self._crash("checkpoint")
        self.assertNotEqual(proc.returncode, 0)
        result = migrate_log_group(self.paths, quiesce=0.01)
        total = 12 + 7 + 8
        self.assertGreater(result.records_migrated, 0)
        self.assertLess(result.records_migrated, total)
        for p, ref in zip(self.paths, self.references()):
            self.assertEqual(read_bytes(p), ref)

    def test_crash_after_all_renames_rerun_is_noop(self):
        refs = self.references()
        proc = self._crash("group-cleanup")
        self.assertNotEqual(proc.returncode, 0)
        # Artifacts remain, but every rename already landed.
        for p, ref in zip(self.paths, refs):
            self.assertEqual(read_bytes(p), ref)
        result = migrate_log_group(self.paths, quiesce=0.01)
        self.assertEqual(result.records_migrated, 0)
        self.assertEqual(result.replaced, self.N)
        for p, ref in zip(self.paths, refs):
            self.assertEqual(read_bytes(p), ref)
        self.assert_group_clean()

    def test_torn_group_commit_record_rolls_back_to_prepare(self):
        # A commit record torn before its member list completed never
        # authorized renames: the rerun re-prepares from checkpoints.
        refs = self.references()
        proc = self._crash("group-commit")
        self.assertNotEqual(proc.returncode, 0)
        group_dir = os.path.abspath(self.paths[0]) + ".migrate-group"
        commit = os.path.join(group_dir, "commit")
        with open(commit, "r+b") as f:
            f.truncate(10)  # tear the record mid-header
        self._finish()
        for p, ref in zip(self.paths, refs):
            self.assertEqual(read_bytes(p), ref)
        self.assert_group_clean()


# ---------------------------------------------------------------------------
# 6. Post-commit fault classification
# ---------------------------------------------------------------------------


class TestGroupPostCommitFaults(GroupBase):
    def test_dirfsync_fault_is_warning_not_failure(self):
        refs = self.references()
        proc = run_group_migrator(self.paths, fault="dirfsync")
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertIn(b"warning:", proc.stderr)
        for p, ref in zip(self.paths, refs):
            self.assertEqual(read_bytes(p), ref)

    def test_cleanup_fault_is_warning_not_failure(self):
        refs = self.references()
        proc = run_group_migrator(self.paths, fault="cleanup")
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertIn(b"warning:", proc.stderr)
        for p, ref in zip(self.paths, refs):
            self.assertEqual(read_bytes(p), ref)
        # A plain rerun finishes the interrupted cleanup.
        proc2 = run_group_migrator(self.paths)
        self.assertEqual(proc2.returncode, 0, proc2.stderr)
        self.assert_group_clean()


# ---------------------------------------------------------------------------
# 7. Consistent group snapshots
# ---------------------------------------------------------------------------


class TestGroupSnapshots(GroupBase):
    def test_pre_migration_view_is_as_stored(self):
        views = read_log_group(self.paths)
        self.assertEqual(len(views), self.N)
        self.assertEqual([r["v"] for r in views[0]], [1] * 12)
        self.assertEqual(views[0][0]["order_id"], "a-0")
        self.assertIn("tags", views[0][0])  # v1 shape, as stored
        self.assertEqual([r["v"] for r in views[1]], [2] * 7)

    def test_post_migration_view_is_normalized(self):
        migrate_log_group(self.paths, quiesce=0.01)
        views = read_log_group(self.paths)
        for view in views:
            self.assertTrue(all(r["v"] == CURRENT_VERSION for r in view))
            self.assertTrue(all("note" in r for r in view))  # v3 shape

    def test_any_member_marker_normalizes_the_whole_group(self):
        # Simulate a commit sequence that has renamed member 0 but not
        # member 1: the group view must still be uniform.
        with open(self.paths[0] + ".committed", "wb") as f:
            f.write(b"1\n")
        views = read_log_group(self.paths)
        for view in views:
            self.assertTrue(all(r["v"] == CURRENT_VERSION for r in view))
        self.assertEqual(views[1][0]["order_id"], "b-0")

    def test_view_excludes_torn_tail(self):
        with open(self.paths[0], "ab") as f:
            f.write(v1("torn")[:-1])
        views = read_log_group(self.paths)
        self.assertEqual(len(views[0]), 12)

    def test_result_aligns_with_manifest_order(self):
        views = read_log_group(self.paths)
        self.assertEqual([v[0]["order_id"] for v in views],
                         ["a-0", "b-0", "c-0"])

    def test_snapshot_stays_uniform_with_concurrent_appender(self):
        # Each member stores one uniform old version, so a consistent
        # snapshot shows exactly one version per member: either as
        # stored (pre-commit) or normalized (commit started).
        for p, enc in zip(self.paths, (v1, v2, v1)):
            with open(p, "wb") as f:
                f.write(b"".join(enc(f"r-{i}") for i in range(2000)))
        stop_flag = os.path.join(self.dir, "stop")
        appender = subprocess.Popen(
            [sys.executable, "-c",
             "import os,sys,time\n"
             "path,stop=sys.argv[1],sys.argv[2]\n"
             "for i in range(30):\n"
             "    with open(path,'ab') as f:\n"
             "        f.write(b'{\"v\":1,\"order_id\":\"late-%d\","
             "\"amount\":4.0,\"tags\":[]}\\n' % i)\n"
             "    time.sleep(0.01)\n"
             "open(stop,'w').close()\n",
             self.paths[0], stop_flag],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        bad_views = []
        done = False

        def reader():
            while not done:
                for view in read_log_group(self.paths, quiesce=0.01):
                    versions = {r["v"] for r in view}
                    if len(versions) > 1:
                        bad_views.append(versions)

        t = threading.Thread(target=reader)
        t.start()
        migrate_log_group(self.paths, quiesce=0.02)
        done = True
        t.join(timeout=10)
        appender.wait(timeout=10)
        self.assertEqual(bad_views, [])
        # A record landing after convergence exited is the documented
        # rerun case; one idempotent run migrates any such tail.
        migrate_log_group(self.paths, quiesce=0.02)
        # Every appended record was migrated in order, none lost.
        ids = [json.loads(line)["order_id"]
               for line in read_bytes(self.paths[0]).splitlines()]
        late = [i for i in ids if i.startswith("late-")]
        self.assertEqual(late, [f"late-{i}" for i in range(30)])
        self.assertEqual(len(ids), 2000 + 30)


# ---------------------------------------------------------------------------
# 8. CLI
# ---------------------------------------------------------------------------


class TestGroupCli(GroupBase):
    def test_success_exit_zero_and_summary(self):
        proc = run_group_migrator(self.paths)
        self.assertEqual(proc.returncode, EXIT_OK, proc.stderr)
        self.assertIn(b"migrated=27", proc.stdout)
        self.assertIn(b"replaced=3", proc.stdout)

    def test_missing_member_exits_one(self):
        proc = run_group_migrator(self.paths + [self.paths[0] + ".gone"])
        self.assertEqual(proc.returncode, EXIT_ERROR)
        self.assertIn(b"error:", proc.stderr)

    def test_duplicate_paths_exit_two(self):
        proc = run_group_migrator([self.paths[0], self.paths[0]])
        self.assertEqual(proc.returncode, EXIT_USAGE)
        self.assertIn(b"error:", proc.stderr)

    def test_strict_bad_record_exits_three_and_rolls_back(self):
        with open(self.paths[2], "ab") as f:
            f.write(b"garbage\n")
        before = [read_bytes(p) for p in self.paths]
        proc = run_group_migrator(self.paths)
        self.assertEqual(proc.returncode, EXIT_BAD_RECORD)
        self.assertEqual([read_bytes(p) for p in self.paths], before)
        self.assert_group_clean()

    def test_skip_mode_audits_to_stderr(self):
        with open(self.paths[1], "ab") as f:
            f.write(b"garbage\n")
        proc = run_group_migrator(self.paths, on_bad="skip")
        self.assertEqual(proc.returncode, EXIT_OK, proc.stderr)
        self.assertIn(self.paths[1].encode() + b":8:garbage",
                      proc.stderr)
        self.assertIn(b"skipped=1", proc.stdout)

    def test_module_entry_point(self):
        proc = subprocess.run(
            [sys.executable, "-m", "proto_migrate", "migrate-logs",
             self.paths[0], "--quiesce-ms", "20"],
            capture_output=True, cwd=REPO_ROOT)
        self.assertEqual(proc.returncode, EXIT_OK, proc.stderr)
        self.assertIn(b"replaced=1", proc.stdout)

    def test_module_usage_lists_group_command(self):
        proc = subprocess.run(
            [sys.executable, "-m", "proto_migrate"],
            capture_output=True, cwd=REPO_ROOT)
        self.assertEqual(proc.returncode, EXIT_USAGE)
        self.assertIn(b"migrate-logs", proc.stderr)


if __name__ == "__main__":
    unittest.main()
