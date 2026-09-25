"""Cursor-based streaming snapshot of linked log groups.

:func:`read_linked_logs_stream` is the batched, resumable companion of
:func:`proto_migrate.read_linked_logs`: it returns every record of every
group in group order, member order and line order, one bounded batch at
a time.  A whole group is never held in memory -- each batch only opens
the member its cursor points at and reads complete lines from a stored
byte offset -- and the opaque cursor returned with a batch resumes the
next call exactly where the previous one ended: no record is repeated
or dropped, and the concatenation of the batches is identical to the
single-shot :func:`~proto_migrate.linked_migration.read_linked_logs`
view taken when the stream was opened.

Snapshot model
--------------
The first call (``cursor=None``) pins the snapshot before returning any
record: every member is read once *without retaining its content* to
pin its inode, count its complete lines and observe the member commit
marker exactly like
:func:`~proto_migrate.group_migration.read_log_group`'s gate.  Those
pins and the global version policy ("raw old shapes" versus
"normalized to the current version") are carried in the cursor, so:

  * the series never mixes old and new field shapes between groups,
    members or batches -- including while a path-reopening appender
    writes old-format records during a migration's wrap-up;
  * appends that land after the snapshot was pinned do not move the
    frozen line counts (a record caught mid-append, without a
    terminating newline, is excluded just like the one-shot view);
  * already returned batches stay valid if a later member becomes
    unreadable -- only the failing call raises ``FileNotFoundError``.

The cursor is an ASCII token (base64 of a JSON document).  Passing
anything that is not a ``str`` raises ``TypeError``; a token whose
contents cannot be parsed or validated raises ``ValueError``; the member
the cursor currently points at being missing or unreadable raises
``FileNotFoundError``.
"""

from __future__ import annotations

import base64
import json
import os
from typing import NamedTuple

from . import CURRENT_VERSION, loads, migrate
from .log_migration import DEFAULT_QUIESCE
from .linked_migration import _normalize_groups

__all__ = [
    "StreamBatch",
    "read_linked_logs_stream",
    "encode_cursor",
    "decode_cursor",
]

_CURSOR_VERSION = 1
_BACKUP_SUFFIXES = (
    ".migrate-linked-backup",
    ".migrate-group-backup",
)


class StreamBatch(NamedTuple):
    """One batch of a streaming linked snapshot.

    ``records`` is a flat list in group/member/line order;
    ``next_cursor`` resumes the following call and is ``None`` once the
    stream is exhausted (``done`` is true).
    """

    records: list
    next_cursor: str | None
    done: bool


# ---------------------------------------------------------------------------
# Cursor codec
# ---------------------------------------------------------------------------


def encode_cursor(state):
    """Encode the resumable stream state as an opaque ASCII token."""
    raw = json.dumps(state, ensure_ascii=True, separators=(",", ":"))
    return base64.urlsafe_b64encode(raw.encode("ascii")).decode("ascii")


def decode_cursor(cursor):
    """Decode a cursor token; raise ValueError on any corruption."""
    try:
        raw = base64.b64decode(cursor.encode("ascii"), altchars=b"-_",
                               validate=True)
        state = json.loads(raw.decode("ascii"))
    except (ValueError, TypeError, UnicodeDecodeError) as exc:
        raise ValueError("corrupt stream cursor") from exc
    if not isinstance(state, dict) or state.get("v") != _CURSOR_VERSION:
        raise ValueError("corrupt stream cursor")
    plan, groups, pos = state.get("plan"), state.get("groups"), state.get(
        "pos"
    )
    if not isinstance(plan, list) or not isinstance(groups, list):
        raise ValueError("corrupt stream cursor")
    if not (isinstance(pos, list) and len(pos) == 3):
        raise ValueError("corrupt stream cursor")
    entry, offset, line_index = pos
    if not (isinstance(entry, int) and entry >= 0
            and isinstance(offset, int) and offset >= 0
            and isinstance(line_index, int) and line_index >= 0):
        raise ValueError("corrupt stream cursor")
    if entry > len(plan) or not isinstance(state.get("normalize"), bool):
        raise ValueError("corrupt stream cursor")
    for item in plan:
        if not (isinstance(item, list) and len(item) == 4):
            raise ValueError("corrupt stream cursor")
        abspath, inode, line_count, post = item
        if not (isinstance(abspath, str) and isinstance(inode, int)
                and isinstance(line_count, int) and line_count >= 0
                and isinstance(post, bool)):
            raise ValueError("corrupt stream cursor")
    if not (all(isinstance(g, list) and g
                and all(isinstance(p, str) for p in g) for g in groups)):
        raise ValueError("corrupt stream cursor")
    return state


# ---------------------------------------------------------------------------
# Snapshot pinning
# ---------------------------------------------------------------------------


_SCAN_CHUNK = 1 << 20


def _scan_member(path):
    """Pin one member with a single streaming pass (bounded memory).

    Opens the path once, reads it in chunks to count complete
    (newline-terminated) lines and to note whether any stored record is
    at the current version, closes it and only then rechecks the commit
    marker -- mirroring the one-shot gate's rule that marker publication
    strictly precedes the rename.  Returns
    ``(inode, line_count, has_current, post_commit)``; no content is
    retained.
    """
    marker = path + ".committed"
    post = os.path.exists(marker)
    line_count = 0
    has_current = False
    carry = b""
    with open(path, "rb") as fh:
        inode = os.fstat(fh.fileno()).st_ino
        while True:
            chunk = fh.read(_SCAN_CHUNK)
            if not chunk:
                break
            data = carry + chunk
            nl = data.rfind(b"\n")
            if nl < 0:
                carry = data
                continue
            complete, carry = data[:nl + 1], data[nl + 1:]
            start = 0
            for idx, byte in enumerate(complete):
                if byte == 0x0A:  # b"\n"
                    line = complete[start:idx + 1]
                    line_count += 1
                    if not has_current and \
                            loads(line)["v"] == CURRENT_VERSION:
                        has_current = True
                    start = idx + 1
    if not post:
        post = os.path.exists(marker)
    return inode, line_count, has_current, post


def _build_plan(groups):
    """Pin every member once; return (plan, normalize, groups_abspaths).

    Members are streamed one at a time and their bytes are not retained,
    so even pinning never loads a whole group into memory.  The whole
    stream is raw only while every member is pre-commit with no
    current-version record (the exact rule of read_linked_logs);
    otherwise every record of every batch is normalized.
    """
    plan = []
    any_post = False
    all_old = True
    group_abs = []
    for group in groups:
        abs_group = []
        for path in group:
            inode, line_count, has_current, post = _scan_member(path)
            if post:
                any_post = True
            if has_current:
                all_old = False
            plan.append([os.path.abspath(path), inode, line_count, post])
            abs_group.append(os.path.abspath(path))
        group_abs.append(abs_group)
    normalize = any_post or not all_old
    return plan, normalize, group_abs


# ---------------------------------------------------------------------------
# Pinned-inode reopening
# ---------------------------------------------------------------------------


def _open_pinned(abspath, inode, normalize, line_index):
    """Open the coherent snapshot inode for one pinned member.

    The live path normally still names the pinned inode.  After a
    migration commits while a stream is open, a normalized (post-commit)
    snapshot continues on the replacement inode -- its records are the
    same current-version shape; the descriptor is repositioned to
    *line_index* by line count rather than a stored byte offset (which
    belongs to the old inode).  A pre-commit snapshot needs the original
    inode and falls back to a surviving rename backup before failing
    with FileNotFoundError.

    Returns ``(file, offset, switched)`` where *offset* is the byte
    position of the next unread line and *switched* says the live inode
    was not the pinned one (so stored offsets must not be reused).
    """
    try:
        fh = open(abspath, "rb")
    except FileNotFoundError:
        fh = None
    if fh is not None and os.fstat(fh.fileno()).st_ino == inode:
        return fh, None, False
    if fh is not None:
        fh.close()
    if normalize:
        fh = open(abspath, "rb")
        available = 0
        while fh.readline().endswith(b"\n"):
            available += 1
        if line_index >= available:
            # A post-commit skip migration dropped records, so the
            # frozen position is at/past this inode's content: finish
            # the member (relocated == -1 signals that to the caller).
            fh.seek(0, os.SEEK_END)
            return fh, -1, True
        fh.seek(0)
        for _ in range(line_index):
            fh.readline()
        return fh, fh.tell(), True
    for suffix in _BACKUP_SUFFIXES:
        try:
            candidate = open(abspath + suffix, "rb")
        except OSError:
            continue
        if os.fstat(candidate.fileno()).st_ino == inode:
            return candidate, None, False
        candidate.close()
    raise FileNotFoundError(
        f"snapshot member is no longer readable: {abspath!r}"
    )


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def read_linked_logs_stream(groups, cursor=None, *, batch_size=1000,
                            quiesce=DEFAULT_QUIESCE):
    """Return one batch of a streaming version-consistent linked snapshot.

    The record list of a :class:`StreamBatch` follows group order, then
    member order, then line order; concatenating every batch yields
    exactly :func:`~proto_migrate.linked_migration.read_linked_logs` at
    the moment the stream opened.  Pass ``cursor=None`` for the first
    call and the previous batch's ``next_cursor`` afterwards; the final
    batch has ``done=True`` and a ``None`` cursor.

    Raises :class:`TypeError` when *cursor* is not a string (and is not
    ``None``), :class:`ValueError` for a corrupt or mismatched cursor or
    a non-positive *batch_size*, and :class:`FileNotFoundError` when the
    member the cursor points at is missing or unreadable (previously
    returned batches are unaffected).
    """
    if cursor is not None and not isinstance(cursor, str):
        raise TypeError("cursor must be a str or None")
    if not isinstance(batch_size, int) or isinstance(batch_size, bool) \
            or batch_size <= 0:
        raise ValueError("batch_size must be a positive integer")

    if cursor is None:
        groups = _normalize_groups(groups)
        for group in groups:
            for path in group:
                try:
                    with open(path, "rb"):
                        pass
                except OSError as exc:
                    raise FileNotFoundError(
                        f"group member missing or unreadable: {path!r}: "
                        f"{exc}"
                    ) from exc
        plan, normalize, group_abs = _build_plan(groups)
        state = {
            "v": _CURSOR_VERSION,
            "groups": group_abs,
            "plan": plan,
            "normalize": normalize,
            "pos": [0, 0, 0],
        }
    else:
        state = decode_cursor(cursor)
        groups = _normalize_groups(groups)
        asked = [[os.path.abspath(p) for p in g] for g in groups]
        if asked != state["groups"]:
            raise ValueError("cursor was opened for a different group list")

    plan = state["plan"]
    normalize = state["normalize"]
    entry_index, byte_offset, line_index = state["pos"]

    if entry_index >= len(plan):
        return StreamBatch([], None, True)

    records = []
    fh = None
    current_entry = -1
    try:
        while len(records) < batch_size:
            if entry_index >= len(plan):
                break
            abspath, inode, line_count, _post = plan[entry_index]
            if fh is None or current_entry != entry_index:
                if fh is not None:
                    fh.close()
                fh, relocated, _switched = _open_pinned(
                    abspath, inode, normalize, line_index
                )
                current_entry = entry_index
                if relocated == -1:
                    # Switched inode already contains no more frozen
                    # lines (a skip migration dropped the tail): finish
                    # the member immediately.
                    line_index = line_count
                else:
                    if relocated is not None:
                        byte_offset = relocated
                    fh.seek(byte_offset)
            if line_index >= line_count:
                fh.close()
                fh = None
                entry_index += 1
                byte_offset = 0
                line_index = 0
                continue
            raw = fh.readline()
            if not raw or not raw.endswith(b"\n"):
                # The pinned content must remain on its inode up to the
                # frozen line count; anything shorter means that member
                # is no longer readable for this snapshot.
                raise FileNotFoundError(
                    f"snapshot member is no longer readable: {abspath!r}"
                )
            byte_offset += len(raw)
            line_index += 1
            record = loads(raw)
            if normalize:
                record = migrate(record, CURRENT_VERSION)
            records.append(record)
    finally:
        if fh is not None:
            fh.close()

    if entry_index >= len(plan):
        return StreamBatch(records, None, True)
    next_state = dict(state)
    next_state["pos"] = [entry_index, byte_offset, line_index]
    return StreamBatch(records, encode_cursor(next_state), False)
