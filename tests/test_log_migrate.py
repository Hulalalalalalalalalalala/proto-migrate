"""Tests for crash-safe append-log migration.

Three required scenario families:

* crash injection at every durability point, then rerun;
* records concurrently appended during the rewrite are not lost;
* repeated runs are byte-for-byte idempotent.

Plus strict/skip handling, audit format and the CLI surface.
"""

from __future__ import annotations

import io
import json
import math
import os
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path

import proto_migrate
from proto_migrate import CURRENT_VERSION, dumps, loads, migrate_log
from proto_migrate.log_migrate import (
    EXIT_BAD_RECORD,
    LOG_SKIP,
    LOG_STRICT,
    BadRecordError,
)

# Fast settle windows for tests (default is ~1 second of quiet).
KW = dict(settle_attempts=50, settle_interval=0.002)


def v1(order_id, amount=1):
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
        + b',"amount":2,"tags":["a"],"status":'
        + json.dumps(status).encode()
        + b"}\n"
    )


def v3(order_id, amount=3.0, status="paid", note="n", updated_at=5):
    return dumps(
        {
            "order_id": order_id,
            "amount": amount,
            "status": status,
            "note": note,
            "updated_at": updated_at,
        }
    )


def expect_v3(line):
    obj = json.loads(line)
    assert obj["v"] == CURRENT_VERSION, line
    return obj


class TestBasicMigration(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmp.name, "log.jsonl")

    def tearDown(self):
        self.tmp.cleanup()

    def test_mixed_versions_all_become_current(self):
        data = v1("A") + v2("B") + v3("C")
        with open(self.path, "wb") as fh:
            fh.write(data)
        result = migrate_log(self.path, **KW)
        self.assertTrue(result.changed)
        lines = Path(self.path).read_bytes().splitlines()
        self.assertEqual(len(lines), 3)
        objs = [expect_v3(line) for line in lines]
        self.assertEqual([o["order_id"] for o in objs], ["A", "B", "C"])
        # v1 gains defaults; v2 keeps status, tags dropped.
        self.assertEqual(objs[0]["status"], "new")
        self.assertEqual(objs[0]["note"], "")
        self.assertEqual(objs[0]["updated_at"], 0)
        self.assertNotIn("tags", objs[1])
        self.assertEqual(objs[1]["status"], "paid")
        # No new/old field mixing anywhere in the file.
        for line in lines:
            self.assertNotIn(b"tags", line)

    def test_current_version_record_preserved_byte_for_byte(self):
        line = v3("C")
        with open(self.path, "wb") as fh:
            fh.write(v1("A") + line)
        migrate_log(self.path, **KW)
        self.assertIn(line, Path(self.path).read_bytes())

    def test_file_without_trailing_newline(self):
        with open(self.path, "wb") as fh:
            fh.write(v1("A") + v2("B").rstrip(b"\n"))
        result = migrate_log(self.path, **KW)
        self.assertTrue(result.changed)
        lines = Path(self.path).read_bytes().splitlines()
        self.assertEqual(len(lines), 2)
        for line in lines:
            expect_v3(line)

    def test_segmented_parts_merged_in_order(self):
        data = b"".join(v1(f"O{i}") for i in range(200))
        with open(self.path, "wb") as fh:
            fh.write(data)
        result = migrate_log(self.path, segment_size=128, **KW)
        self.assertTrue(result.changed)
        lines = Path(self.path).read_bytes().splitlines()
        self.assertEqual(len(lines), 200)
        ids = [expect_v3(line)["order_id"] for line in lines]
        self.assertEqual(ids, [f"O{i}" for i in range(200)])

    def test_empty_file_is_noop(self):
        open(self.path, "wb").close()
        result = migrate_log(self.path, **KW)
        self.assertFalse(result.changed)
        self.assertEqual(Path(self.path).read_bytes(), b"")

    def test_negative_zero_amount_preserved(self):
        line = v3("Z", amount=-0.0)
        self.assertIn(b"-0.0", line)
        with open(self.path, "wb") as fh:
            fh.write(v1("A") + line)
        migrate_log(self.path, **KW)
        out = Path(self.path).read_bytes()
        self.assertIn(b"-0.0", out)
        amount = expect_v3(out.splitlines()[1])["amount"]
        self.assertEqual(amount, 0.0)
        self.assertTrue(math.copysign(1.0, amount) < 0)


class TestBadRecords(unittest.TestCase):
    BAD_LINES = {
        "not_json": b"not json at all\n",
        "nan": b'{"v":1,"order_id":"N","amount":NaN,"tags":[]}\n',
        "infinity": b'{"v":1,"order_id":"I","amount":Infinity,"tags":[]}\n',
        "neg_infinity": b'{"v":1,"order_id":"I","amount":-Infinity,"tags":[]}\n',
        "overflow": b'{"v":1,"order_id":"O","amount":1e999,"tags":[]}\n',
        "missing_v": b'{"order_id":"M","amount":1,"tags":[]}\n',
        "float_v": b'{"v":3.0,"order_id":"F","amount":1}\n',
        "string_v": b'{"v":"3","order_id":"S","amount":1}\n',
        "unsupported_v": b'{"v":9,"order_id":"U","amount":1}\n',
        "bad_field": b'{"v":1,"order_id":7,"amount":1,"tags":[]}\n',
    }

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmp.name, "log.jsonl")

    def tearDown(self):
        self.tmp.cleanup()

    def test_strict_stops_at_first_bad_line_and_file_untouched(self):
        data = v1("A") + self.BAD_LINES["not_json"] + v1("B")
        with open(self.path, "wb") as fh:
            fh.write(data)
        with self.assertRaises(BadRecordError) as cm:
            migrate_log(self.path, mode=LOG_STRICT, **KW)
        self.assertEqual(cm.exception.lineno, 2)
        self.assertEqual(Path(self.path).read_bytes(), data)

    def test_every_bad_kind_rejected_strict(self):
        for name, line in self.BAD_LINES.items():
            with self.subTest(name=name):
                with open(self.path, "wb") as fh:
                    fh.write(v1("A") + line)
                original = Path(self.path).read_bytes()
                with self.assertRaises(BadRecordError):
                    migrate_log(self.path, mode=LOG_STRICT, **KW)
                self.assertEqual(Path(self.path).read_bytes(), original)

    def test_skip_writes_audit_and_drops_bad_lines(self):
        ordered = [
            self.BAD_LINES["not_json"],
            v1("ok1"),
            self.BAD_LINES["nan"],
            self.BAD_LINES["overflow"],
            v2("ok2"),
        ]
        with open(self.path, "wb") as fh:
            fh.write(b"".join(ordered))
        audit = io.BytesIO()
        result = migrate_log(
            self.path, mode=LOG_SKIP, audit_stream=audit, **KW
        )
        self.assertEqual(result.skipped, 3)
        # ok1 (v1) and ok2 (v2) are both re-encoded to current version.
        self.assertEqual(result.migrated_lines, 2)
        out = Path(self.path).read_bytes().splitlines()
        self.assertEqual(len(out), 2)
        ids = [expect_v3(line)["order_id"] for line in out]
        self.assertEqual(ids, ["ok1", "ok2"])

        entries = audit.getvalue().splitlines()
        self.assertEqual(len(entries), 3)
        # "<lineno>:<first 32 raw bytes>"
        self.assertTrue(entries[0].startswith(b"1:"))
        self.assertEqual(entries[0][2:], ordered[0].rstrip(b"\r\n")[:32])
        self.assertTrue(entries[1].startswith(b"3:"))
        self.assertEqual(entries[1][2:], ordered[2].rstrip(b"\r\n")[:32])
        self.assertTrue(entries[2].startswith(b"4:"))

    def test_audit_snippet_is_capped_at_32_bytes(self):
        long_bad = b"x" * 100 + b"\n"
        with open(self.path, "wb") as fh:
            fh.write(long_bad)
        audit = io.BytesIO()
        migrate_log(self.path, mode=LOG_SKIP, audit_stream=audit, **KW)
        entry = audit.getvalue()
        self.assertEqual(entry, b"1:" + b"x" * 32 + b"\n")

    def test_result_audit_matches_stream(self):
        with open(self.path, "wb") as fh:
            fh.write(self.BAD_LINES["string_v"] + b"}\n" + v1("A"))
        audit = io.BytesIO()
        result = migrate_log(
            self.path, mode=LOG_SKIP, audit_stream=audit, **KW
        )
        streamed = audit.getvalue()
        for number, snippet in result.audit:
            self.assertIn(f"{number}:".encode() + snippet + b"\n", streamed)


class TestIdempotency(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmp.name, "log.jsonl")

    def tearDown(self):
        self.tmp.cleanup()

    def test_second_run_changes_no_bytes(self):
        with open(self.path, "wb") as fh:
            fh.write(v1("A") + v2("B") + v3("C"))
        first = migrate_log(self.path, mode=LOG_SKIP, **KW)
        self.assertTrue(first.changed)
        migrated = Path(self.path).read_bytes()
        st = os.stat(self.path)

        audit = io.BytesIO()
        second = migrate_log(
            self.path, mode=LOG_SKIP, audit_stream=audit, **KW
        )
        self.assertFalse(second.changed)
        self.assertEqual(second.skipped, 0)
        self.assertEqual(audit.getvalue(), b"")
        self.assertEqual(Path(self.path).read_bytes(), migrated)
        self.assertEqual(os.stat(self.path).st_ino, st.st_ino)
        self.assertEqual(os.stat(self.path).st_mtime_ns, st.st_mtime_ns)

    def test_already_current_file_never_rewritten(self):
        with open(self.path, "wb") as fh:
            fh.write(v3("A") + v3("B"))
        st = os.stat(self.path)
        result = migrate_log(self.path, **KW)
        self.assertFalse(result.changed)
        self.assertEqual(os.stat(self.path).st_ino, st.st_ino)


# --------------------------------------------------------------------------
# Crash injection
# --------------------------------------------------------------------------

CHILD_SCRIPT = r"""
import os, sys
sys.path.insert(0, os.environ["PM_PYTHONPATH"])
from proto_migrate import migrate_log

point = os.environ["PM_POINT"]
count = int(os.environ.get("PM_COUNT", "1"))
late = os.environ.get("PM_LATE", "")
late_kind = os.environ.get("PM_LATE_KIND", "good")

state = {"n": 0}

def crash(p):
    if p == "after_rename" and late:
        from proto_migrate import dumps
        link = os.path.join(os.path.dirname(os.environ["PM_FILE"]),
                            "." + os.path.basename(os.environ["PM_FILE"])
                            + ".migrate.old")
        with open(link, "r+b") as fh:
            fh.seek(0, os.SEEK_END)
            for i in range(int(late)):
                if late_kind == "bad":
                    fh.write(b"garbage residual line\n")
                else:
                    fh.write(dumps({"order_id": f"LATE{i}", "amount": i,
                                    "status": "new", "note": "",
                                    "updated_at": 0}))
    if p == point:
        state["n"] += 1
        if state["n"] >= count:
            sys.stderr.flush()
            os._exit(99)
    return False

audit = open(os.environ["PM_AUDIT"], "wb") if os.environ.get("PM_AUDIT") else None
migrate_log(
    os.environ["PM_FILE"],
    mode=os.environ.get("PM_MODE", "strict"),
    audit_stream=audit,
    segment_size=int(os.environ.get("PM_SEGMENT", str(8 * 1024 * 1024))),
    settle_attempts=int(os.environ.get("PM_SETTLE_ATTEMPTS", "50")),
    settle_interval=float(os.environ.get("PM_SETTLE_INTERVAL", "0.002")),
    crash=crash,
)
"""


def expected_migrated(data, mode):
    """Reference migration used as the expected final content."""
    out = []
    for line in data.splitlines():
        try:
            decoded = loads(line + b"\n")
        except ValueError:
            if mode == LOG_STRICT:
                raise
            continue
        message = proto_migrate.migrate(decoded, CURRENT_VERSION)
        payload = {k: v for k, v in message.items() if k != "v"}
        out.append(dumps(payload))
    return b"".join(out)


def run_child(path, mode, point, count=1, env_extra=None, segment=None):
    env = dict(os.environ)
    env.update(
        PM_PYTHONPATH=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        PM_FILE=path,
        MODE_ENV="",
        PM_POINT=point,
        PM_COUNT=str(count),
        PM_MODE=mode,
        PM_SETTLE_ATTEMPTS="50",
        PM_SETTLE_INTERVAL="0.002",
    )
    if segment is not None:
        env["PM_SEGMENT"] = str(segment)
    if env_extra:
        env.update(env_extra)
    return subprocess.run(
        [sys.executable, "-c", CHILD_SCRIPT],
        env=env,
        capture_output=True,
    )


def leftovers(path):
    """Recovery artifacts that must not remain after a completed run."""
    directory = os.path.dirname(path)
    base = os.path.basename(path)
    found = []
    for name in os.listdir(directory):
        if name == base:
            continue
        if name.endswith(".migrate.lock"):
            continue
        if ".migrate" in name:
            found.append(name)
    return found


class TestCrashInjection(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmp.name, "log.jsonl")
        self.data = b"".join(v1(f"O{i}") for i in range(30))
        with open(self.path, "wb") as fh:
            fh.write(self.data)
        self.final = expected_migrated(self.data, LOG_STRICT)

    def tearDown(self):
        self.tmp.cleanup()

    def assertOriginalOrComplete(self, content):
        """Right after a kill: original bytes OR complete migrated bytes."""
        self.assertIn(content, (self.data, self.final))

    def crash_and_recover(self, point, count=1, mode=LOG_STRICT,
                          segment=None, extra_env=None):
        proc = run_child(
            self.path, mode, point, count=count,
            segment=segment, env_extra=extra_env,
        )
        self.assertEqual(
            proc.returncode, 99,
            msg=f"child did not crash at {point}: {proc.stderr!r}",
        )
        crashed_content = Path(self.path).read_bytes()
        self.assertOriginalOrComplete(crashed_content)

        # Rerun completes the migration.
        result = migrate_log(self.path, mode=mode, **KW)
        final = Path(self.path).read_bytes()
        self.assertEqual(final, self.final)
        for line in final.splitlines():
            expect_v3(line)
        self.assertEqual(leftovers(self.path), [])
        return result

    def test_crash_scan_line(self):
        self.crash_and_recover("scan_line", count=5)

    def test_crash_part_fsync(self):
        self.crash_and_recover("part_fsync", segment=200)

    def test_crash_before_merge(self):
        self.crash_and_recover("before_merge")

    def test_crash_after_merge(self):
        self.crash_and_recover("after_merge")

    def test_crash_after_merge_fsync(self):
        self.crash_and_recover("after_merge_fsync")

    def test_crash_after_link(self):
        self.crash_and_recover("after_link")

    def test_crash_before_rename(self):
        self.crash_and_recover("before_rename")

    def test_crash_after_rename(self):
        self.crash_and_recover("after_rename")

    def test_crash_after_residual(self):
        self.crash_and_recover("after_residual")

    def test_crash_after_dir_fsync(self):
        self.crash_and_recover("after_dir_fsync")

    def test_repeated_crashes_still_converges(self):
        # Kill at several points across successive runs, then finish.
        for point, count, segment in (
            ("scan_line", 10, None),
            ("part_fsync", 1, 200),
            ("before_merge", 1, None),
            ("after_link", 1, None),
            ("after_rename", 1, None),
        ):
            proc = run_child(
                self.path, LOG_STRICT, point, count=count, segment=segment
            )
            self.assertEqual(proc.returncode, 99, msg=proc.stderr)
            self.assertOriginalOrComplete(Path(self.path).read_bytes())
        result = migrate_log(self.path, **KW)
        self.assertEqual(Path(self.path).read_bytes(), self.final)
        self.assertEqual(leftovers(self.path), [])
        self.assertTrue(result.changed)


class TestResidualRecovery(unittest.TestCase):
    """Late appends stranded on the old inode by a post-rename kill."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmp.name, "log.jsonl")
        self.data = b"".join(v1(f"O{i}") for i in range(20))
        with open(self.path, "wb") as fh:
            fh.write(self.data)

    def tearDown(self):
        self.tmp.cleanup()

    def test_late_records_recovered_after_rename_crash(self):
        proc = run_child(
            self.path, LOG_STRICT, "after_rename",
            env_extra={"PM_LATE": "4"},
        )
        self.assertEqual(proc.returncode, 99, msg=proc.stderr)
        result = migrate_log(self.path, **KW)
        lines = Path(self.path).read_bytes().splitlines()
        ids = [expect_v3(line)["order_id"] for line in lines]
        self.assertEqual(
            ids, [f"O{i}" for i in range(20)] + [f"LATE{i}" for i in range(4)]
        )
        self.assertEqual(leftovers(self.path), [])
        self.assertTrue(result.changed)

    def test_late_records_survive_second_crash_at_checkpoint(self):
        proc = run_child(
            self.path, LOG_STRICT, "after_rename",
            env_extra={"PM_LATE": "3"},
        )
        self.assertEqual(proc.returncode, 99, msg=proc.stderr)

        # Recovery dies after fsyncing/checkpointing the first late line.
        proc = run_child(self.path, LOG_STRICT, "checkpoint")
        self.assertEqual(proc.returncode, 99, msg=proc.stderr)
        partial = Path(self.path).read_bytes().splitlines()
        self.assertIn("LATE0", [json.loads(x)["order_id"] for x in partial])
        self.assertNotIn("LATE1", [json.loads(x)["order_id"] for x in partial])

        # Exactly-once: LATE0 must not be duplicated on the final run.
        result = migrate_log(self.path, **KW)
        ids = [
            expect_v3(line)["order_id"]
            for line in Path(self.path).read_bytes().splitlines()
        ]
        self.assertEqual(
            ids, [f"O{i}" for i in range(20)] + ["LATE0", "LATE1", "LATE2"]
        )
        self.assertEqual(leftovers(self.path), [])
        self.assertTrue(result.changed)

    def test_bad_residual_in_strict_rolls_back_then_skip_completes(self):
        proc = run_child(
            self.path, LOG_STRICT, "after_rename",
            env_extra={"PM_LATE": "2", "PM_LATE_KIND": "bad"},
        )
        self.assertEqual(proc.returncode, 99, msg=proc.stderr)

        with self.assertRaises(BadRecordError):
            migrate_log(self.path, mode=LOG_STRICT, **KW)
        # Rename rolled back: original inode (with its late appends) is
        # back at the path; nothing migrated is visible to readers.
        content = Path(self.path).read_bytes()
        self.assertTrue(content.startswith(self.data))
        self.assertIn(b"garbage residual line", content)
        self.assertEqual(leftovers(self.path), [])

        audit = io.BytesIO()
        migrate_log(
            self.path, mode=LOG_SKIP, audit_stream=audit, **KW
        )
        lines = Path(self.path).read_bytes().splitlines()
        self.assertEqual(len(lines), 20)
        for line in lines:
            expect_v3(line)
        self.assertEqual(audit.getvalue().count(b"garbage"), 2)


# --------------------------------------------------------------------------
# Concurrent appends
# --------------------------------------------------------------------------

class TestConcurrentAppend(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmp.name, "log.jsonl")

    def tearDown(self):
        self.tmp.cleanup()

    def test_appender_during_rewrite_loses_nothing(self):
        n_base = 20000
        n_appended = 100
        with open(self.path, "wb") as fh:
            fh.write(b"".join(v1(f"base{i}") for i in range(n_base)))

        # Descriptor opened on the original inode and held across the
        # rename: pre-rename writes are followed by the scan, post-rename
        # writes reach the old inode and are drained via the hard link.
        app_fd = os.open(self.path, os.O_WRONLY | os.O_APPEND)
        errors = []

        def append_forever():
            try:
                for i in range(n_appended):
                    os.write(app_fd, v1(f"added{i}"))
                    time.sleep(0.0015)
            except OSError as exc:  # pragma: no cover - diagnostic only
                errors.append(exc)

        thread = threading.Thread(target=append_forever)
        thread.start()
        try:
            result = migrate_log(self.path, **KW)
        finally:
            thread.join()
            os.close(app_fd)
        self.assertEqual(errors, [])
        self.assertTrue(result.changed)

        objs = [
            json.loads(line)
            for line in Path(self.path).read_bytes().splitlines()
        ]
        ids = [o["order_id"] for o in objs]
        self.assertEqual(len(ids), len(set(ids)))  # no duplication
        self.assertEqual(
            set(ids),
            {f"base{i}" for i in range(n_base)}
            | {f"added{i}" for i in range(n_appended)},
        )
        self.assertTrue(all(o["v"] == CURRENT_VERSION for o in objs))

        # Rerun is a byte-level no-op.
        before = Path(self.path).read_bytes()
        again = migrate_log(self.path, **KW)
        self.assertFalse(again.changed)
        self.assertEqual(Path(self.path).read_bytes(), before)


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------

class TestCli(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmp.name, "log.jsonl")

    def tearDown(self):
        self.tmp.cleanup()

    def run_cli(self, *args):
        return subprocess.run(
            [sys.executable, "-m", "proto_migrate", *args],
            capture_output=True,
            text=True,
        )

    def test_selftest_unchanged(self):
        proc = self.run_cli("--selftest")
        self.assertEqual(proc.returncode, 0)
        self.assertEqual(proc.stdout, "ok\n")

    def test_migrate_log_success(self):
        with open(self.path, "wb") as fh:
            fh.write(v1("A") + v2("B"))
        proc = self.run_cli("migrate-log", self.path)
        self.assertEqual(proc.returncode, 0, msg=proc.stderr)
        lines = Path(self.path).read_bytes().splitlines()
        self.assertEqual(len(lines), 2)
        for line in lines:
            expect_v3(line)

        # Second run: success, already up to date, no bytes changed.
        before = Path(self.path).read_bytes()
        proc = self.run_cli("migrate-log", self.path)
        self.assertEqual(proc.returncode, 0)
        self.assertIn("already up to date", proc.stdout)
        self.assertEqual(Path(self.path).read_bytes(), before)

    def test_strict_exit_code_and_audit_on_stderr_skip(self):
        with open(self.path, "wb") as fh:
            fh.write(v1("A") + b"broken line\n" + v1("B"))
        original = Path(self.path).read_bytes()

        proc = self.run_cli("migrate-log", self.path, "--mode", "strict")
        self.assertEqual(proc.returncode, EXIT_BAD_RECORD)
        self.assertIn("line 2", proc.stderr)
        self.assertEqual(Path(self.path).read_bytes(), original)

        proc = self.run_cli("migrate-log", self.path, "--mode", "skip")
        self.assertEqual(proc.returncode, 0, msg=proc.stderr)
        self.assertTrue(proc.stderr.startswith("2:broken line"))
        self.assertEqual(len(Path(self.path).read_bytes().splitlines()), 2)

    def test_usage_errors(self):
        proc = self.run_cli("migrate-log")
        self.assertEqual(proc.returncode, 2)
        proc = self.run_cli("migrate-log", self.path, "--mode", "bogus")
        self.assertEqual(proc.returncode, 2)
        proc = self.run_cli("migrate-log", os.path.join(self.tmp.name, "nope"))
        self.assertEqual(proc.returncode, 2)
        proc = self.run_cli("bogus-command")
        self.assertEqual(proc.returncode, 2)


if __name__ == "__main__":
    unittest.main()
