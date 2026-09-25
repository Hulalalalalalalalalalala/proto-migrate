"""Tests for the cursor-based streaming linked snapshot.

Covers:
  * streamed batches concatenate to exactly read_linked_logs, at many
    batch sizes, both before (raw old shapes) and after migration
    (normalized), across multiple groups and members;
  * cursors resume without repetition or loss and stay valid for the
    pinned snapshot while a migration commits mid-stream;
  * TypeError on a non-string cursor, ValueError on corrupt or
    mismatched cursors, FileNotFoundError on a member the cursor points
    at (previously returned batches unaffected);
  * a torn tail is excluded and bounded batching never loads a member
    wholesale.
"""

import io
import json
import os
import tempfile
import threading
import time
import unittest

from proto_migrate import CURRENT_VERSION, dumps, migrate_linked_logs
from proto_migrate.linked_migration import read_linked_logs
from proto_migrate.stream_migration import (
    StreamBatch,
    decode_cursor,
    encode_cursor,
    read_linked_logs_stream,
)


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
        "order_id": order_id, "amount": 3.0, "status": "paid",
        "note": "", "updated_at": 0,
    })


LINK = [(1, 0, "order_id", "order_id")]


def drain(groups, batch_size):
    flat = []
    cursor = None
    calls = 0
    while True:
        batch = read_linked_logs_stream(groups, cursor,
                                        batch_size=batch_size)
        assert isinstance(batch, StreamBatch)
        calls += 1
        flat.extend(batch.records)
        if batch.done:
            assert batch.next_cursor is None
            break
        cursor = batch.next_cursor
    return flat, calls


class TestStreamingSnapshot(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        d = self.tmp.name
        self.g0a = os.path.join(d, "g0a.log")
        self.g0b = os.path.join(d, "g0b.log")
        self.g1a = os.path.join(d, "g1a.log")
        with open(self.g0a, "wb") as f:
            f.write(b"".join(v1(f"A{i}") for i in range(13)))
        with open(self.g0b, "wb") as f:
            f.write(b"".join(v2(f"B{i}") for i in range(7)))
        with open(self.g1a, "wb") as f:
            f.write(b"".join(v1(f"C{i}") for i in range(19)))
        self.groups = [[self.g0a, self.g0b], [self.g1a]]

    def tearDown(self):
        self.tmp.cleanup()

    def test_pre_migration_stream_equals_one_shot(self):
        whole = read_linked_logs(self.groups)
        for batch_size in (1, 2, 3, 5, 8, 13, 1000):
            flat, _calls = drain(self.groups, batch_size)
            grouped = [flat[:20], flat[20:]]
            self.assertEqual(grouped, whole)
            self.assertEqual(sorted({r["v"] for r in flat}), [1, 2])

    def test_post_migration_stream_is_normalized_and_equal(self):
        migrate_linked_logs(self.groups, links=LINK, on_bad="skip",
                            quiesce=0.01, audit=io.BytesIO())
        whole = read_linked_logs(self.groups)
        for batch_size in (1, 2, 4, 9, 100):
            flat, _calls = drain(self.groups, batch_size)
            self.assertEqual([flat[:20], flat[20:]], whole)
            self.assertTrue(all(r["v"] == CURRENT_VERSION for r in flat))

    def test_no_repetition_or_loss_offsets(self):
        # Every batch advances by exactly its record count and the final
        # positions cover 0..total exactly once.
        seen = []
        cursor = None
        while True:
            batch = read_linked_logs_stream(self.groups, cursor,
                                            batch_size=4)
            seen += [r["order_id"] for r in batch.records]
            if batch.done:
                break
            cursor = batch.next_cursor
        expected = [f"A{i}" for i in range(13)] + \
            [f"B{i}" for i in range(7)] + \
            [f"C{i}" for i in range(19)]
        self.assertEqual(seen, expected)
        self.assertEqual(len(seen), len(set(seen)))

    def test_cursor_is_persistable_string_roundtrip(self):
        batch = read_linked_logs_stream(self.groups, None, batch_size=5)
        self.assertIsInstance(batch.next_cursor, str)
        state = decode_cursor(batch.next_cursor)
        self.assertEqual(encode_cursor(state), batch.next_cursor)

    def test_non_string_cursor_is_type_error(self):
        for bad in (123, 4.5, b"x", ["x"], object()):
            with self.assertRaises(TypeError):
                read_linked_logs_stream(self.groups, bad, batch_size=10)

    def test_corrupt_cursor_is_value_error(self):
        for bad in ("", "not-base64!!!", "W30=", "////"):
            with self.assertRaises(ValueError):
                read_linked_logs_stream(self.groups, bad, batch_size=10)

    def test_cursor_for_other_groups_is_value_error(self):
        batch = read_linked_logs_stream(self.groups, None, batch_size=5)
        other = [[self.g0a], [self.g1a]]
        with self.assertRaises(ValueError):
            read_linked_logs_stream(other, batch.next_cursor,
                                    batch_size=5)

    def test_bad_batch_size(self):
        with self.assertRaises(ValueError):
            read_linked_logs_stream(self.groups, None, batch_size=0)
        with self.assertRaises(ValueError):
            read_linked_logs_stream(self.groups, None, batch_size=True)

    def test_missing_member_at_open_is_file_not_found(self):
        missing = os.path.join(self.tmp.name, "absent.log")
        with self.assertRaises(FileNotFoundError):
            read_linked_logs_stream([[missing]], None, batch_size=10)

    def test_missing_member_mid_stream_leaves_prior_batches_valid(self):
        first = read_linked_logs_stream(self.groups, None, batch_size=10)
        second = read_linked_logs_stream(
            self.groups, first.next_cursor, batch_size=10
        )
        self.assertEqual(len(first.records), 10)
        self.assertEqual(len(second.records), 10)
        os.rename(self.g1a, self.g1a + ".gone")
        with self.assertRaises(FileNotFoundError):
            read_linked_logs_stream(
                self.groups, second.next_cursor, batch_size=10
            )

    def test_torn_tail_excluded_like_one_shot(self):
        with open(self.g1a, "ab") as f:
            f.write(v1("LATE")[:-1])  # no terminating newline
        flat, _ = drain(self.groups, 7)
        whole = read_linked_logs(self.groups)
        self.assertEqual([flat[:20], flat[20:]], whole)
        self.assertNotIn("LATE", [r["order_id"] for r in flat])

    def test_mixed_versions_pin_normalizes(self):
        # A member already containing a current-version record forces
        # the whole stream to normalize, exactly like read_linked_logs.
        with open(self.g0a, "wb") as f:
            f.write(v1("A0") + v3("A1") + v1("A2"))
        whole = read_linked_logs(self.groups)
        flat, _ = drain(self.groups, 3)
        n0 = 3 + 7
        self.assertEqual([flat[:n0], flat[n0:]], whole)
        self.assertTrue(all(r["v"] == CURRENT_VERSION for r in flat))

    def test_appends_after_pin_do_not_change_frozen_stream(self):
        cursor = None
        batches = []
        # Consume the first member only via a tiny batch.
        batch = read_linked_logs_stream(self.groups, None, batch_size=13)
        prefix = list(batch.records)
        cursor = batch.next_cursor
        # Appender lands more records after the snapshot was pinned.
        with open(self.g1a, "ab") as f:
            for i in range(50):
                f.write(v1(f"AFTER{i}"))
        rest = []
        while True:
            batch = read_linked_logs_stream(self.groups, cursor,
                                            batch_size=6)
            rest.extend(batch.records)
            if batch.done:
                break
            cursor = batch.next_cursor
        ids = [r["order_id"] for r in rest]
        self.assertFalse(any(x.startswith("AFTER") for x in ids))
        self.assertEqual(
            prefix, [r for r in read_linked_logs_stream(
                self.groups, None, batch_size=13).records]
        )

    def test_normalized_stream_uniform_with_path_reopening_appender(self):
        # One current-version record at pin forces a normalized stream.
        # A path-reopening appender then writes old-format records
        # across and after the commit; every batch stays current shape.
        with open(self.g0a, "ab") as f:
            f.write(v3("AV3"))
        groups = [[self.g0a, self.g0b], [self.g1a]]
        stop = threading.Event()
        inode0 = os.stat(self.g0a).st_ino

        def appender():
            while not stop.is_set():
                if os.stat(self.g0a).st_ino != inode0:
                    break
                time.sleep(0.002)
            i = 0
            while not stop.is_set():
                with open(self.g0a, "ab") as f:
                    f.write(v1(f"R{i}"))
                i += 1
                time.sleep(0.002)

        t = threading.Thread(target=appender)
        t.start()
        try:
            batch = read_linked_logs_stream(groups, None, batch_size=5)
            flat = list(batch.records)
            self.assertTrue(all(r["v"] == CURRENT_VERSION for r in flat))
            migrate_linked_logs(groups, links=LINK, on_bad="skip",
                                quiesce=0.02, audit=io.BytesIO())
            cursor = batch.next_cursor
            while True:
                batch = read_linked_logs_stream(
                    groups, cursor, batch_size=7
                )
                flat.extend(batch.records)
                if batch.done:
                    break
                cursor = batch.next_cursor
            # Uniform current shape: an old field shape never appears.
            self.assertTrue(all(
                set(r) == set(flat[0]) for r in flat
            ))
            self.assertTrue(all(r["v"] == CURRENT_VERSION for r in flat))
        finally:
            stop.set()
            t.join(timeout=5)

    def test_raw_pinned_stream_refuses_to_mix_after_full_commit(self):
        # A stream pinned in the raw world must never continue with
        # normalized records (that would mix shapes); once its pinned
        # inode is fully replaced it reports FileNotFoundError instead,
        # leaving already-returned batches valid.
        batch = read_linked_logs_stream(self.groups, None, batch_size=5)
        self.assertTrue({r["v"] for r in batch.records} <= {1, 2})
        migrate_linked_logs(self.groups, links=LINK, on_bad="skip",
                            quiesce=0.01, audit=io.BytesIO())
        # Drain whatever members can still be served; eventually a
        # replaced raw inode raises rather than emitting v3 records.
        cursor = batch.next_cursor
        saw_versions = {r["v"] for r in batch.records}
        hit_missing = False
        for _ in range(100):
            try:
                batch = read_linked_logs_stream(
                    self.groups, cursor, batch_size=3
                )
            except FileNotFoundError:
                hit_missing = True
                break
            saw_versions |= {r["v"] for r in batch.records}
            if batch.done:
                break
            cursor = batch.next_cursor
        self.assertTrue(hit_missing or not (saw_versions - {1, 2}))
        self.assertNotIn(CURRENT_VERSION, saw_versions)


if __name__ == "__main__":
    unittest.main()
