"""Multi-instance coordination for linked log migrations.

Several migration instances may run at once over *overlapping* sets of
groups.  Coordination is entirely local (files and file locks -- no
schema registry, no network) and has two rules:

  * **overlapping members are mutually exclusive.**  Each member log has
    a lease file ``<member>.migrate-linked-lease``; an instance acquires
    every lease its groups name with a *non-blocking* exclusive
    ``flock``.  Two instances whose group sets share a member can never
    prepare or rename that member at the same time; instances with
    disjoint member sets hold no common lease and proceed fully in
    parallel.
  * **a holder that is preempted or crashes releases its leases.**  An
    ``flock`` is released by the kernel the moment the holding process
    dies (however abruptly), so no stale lock ever outlives its owner:
    another instance deterministically takes over and resumes from the
    killed run's durable per-member checkpoints and line indexes --
    prepared members are never rescanned.  The finished bytes are
    identical to running the same instances one after another in spec
    order, because migration itself is deterministic and idempotent.

While an instance holds a lease its identity, pid and heartbeat are
written into the lease file (rebuildable state: the authoritative lease
is the kernel-side lock, not the bytes).  A lease already held by a live
instance makes acquisition raise :class:`MigrationLockedError`; once the
holder is gone the same call acquires and the run takes over.

:func:`run_coordinated_migrations` schedules a batch of instance specs:
disjoint specs run in parallel, and whenever two specs share a member
they execute deterministically in spec order (a lower-index overlapping
spec always completes first), so the observable result is byte-for-byte
the same as running the batch serially in that order.
"""

from __future__ import annotations

import errno
import fcntl
import hashlib
import json
import os
import threading
import time
from typing import NamedTuple

from .linked_migration import (
    _normalize_groups,
    _normalize_links,
    _probe_members,
    migrate_linked_logs,
)

__all__ = [
    "MigrationLockedError",
    "MigrationSpec",
    "CoordinatedMigrationResult",
    "acquire_member_leases",
    "migrate_linked_logs_coordinated",
    "run_coordinated_migrations",
    "spec_work_dir",
]

# The lease is the same file the baseline migration uses for its
# per-member flock (log/group/linked all lock "<member>.migrate.lock"),
# so a coordinated instance also excludes an uncoordinated migration of
# the same member, not merely another coordinated instance.
_LEASE_SUFFIX = ".migrate.lock"
_HEARTBEAT_SECONDS = 0.25


class MigrationLockedError(RuntimeError):
    """Raised when a member lease is held by another live instance."""

    def __init__(self, path, holder=None):
        self.path = path
        self.holder = holder
        who = f" (held by {holder})" if holder else ""
        super().__init__(
            f"migration lease for {path!r} is held by another instance{who}"
        )


class MigrationSpec(NamedTuple):
    """One coordinated instance: groups plus its reference declarations."""

    groups: list
    links: list = []
    on_bad: str = "strict"


class CoordinatedMigrationResult(NamedTuple):
    spec_index: int
    instance_id: str
    result: object
    took_over: bool


# ---------------------------------------------------------------------------
# Spec identity and on-disk layout
# ---------------------------------------------------------------------------


def _spec_identity(groups, links, on_bad):
    return json.dumps({
        "groups": [[os.path.abspath(p) for p in g] for g in groups],
        "links": [list(link) for link in links],
        "on_bad": on_bad,
    }, sort_keys=True).encode("ascii")


def spec_work_dir(groups, links, on_bad):
    """This spec's group work directory next to its first member.

    Distinct specs hash to distinct directories, so instances over
    disjoint group sets never share a group lock or commit marker and
    run in parallel; two attempts of the *same* spec hash identically
    and resume the same group marker and per-member checkpoints on
    takeover.  Per-member work directories stay per-path (shared), and
    access to them is serialized by the member leases.
    """
    digest = hashlib.sha1(_spec_identity(groups, links, on_bad)).hexdigest()
    digest = digest[:16]
    first_dir = os.path.dirname(os.path.abspath(groups[0][0]))
    return os.path.join(first_dir, f".migrate-linked-tmp-{digest}")


def spec_lock_path(groups, links, on_bad):
    return os.path.join(spec_work_dir(groups, links, on_bad), "group.lock")


def _lease_path(member_path):
    return member_path + _LEASE_SUFFIX


# ---------------------------------------------------------------------------
# Leases
# ---------------------------------------------------------------------------


class _MemberLease:
    def __init__(self, path, fh, instance_id):
        self.path = path
        self.fh = fh
        self.instance_id = instance_id
        self._stop = threading.Event()
        self._thread = None

    def _write(self):
        self.fh.seek(0)
        self.fh.truncate()
        self.fh.write(json.dumps({
            "instance": self.instance_id,
            "pid": os.getpid(),
        }).encode("ascii"))
        self.fh.flush()

    def _beat(self):
        while not self._stop.wait(_HEARTBEAT_SECONDS):
            try:
                self._write()
            except OSError:
                return

    def start_heartbeat(self):
        self._write()
        self._thread = threading.Thread(
            target=self._beat, name="lease-heartbeat", daemon=True
        )
        self._thread.start()

    def release(self):
        self._stop.set()
        try:
            self.fh.seek(0)
            self.fh.truncate()
            self.fh.flush()
        except OSError:
            pass
        try:
            fcntl.flock(self.fh.fileno(), fcntl.LOCK_UN)
        except OSError:
            pass
        try:
            self.fh.close()
        except OSError:
            pass


def acquire_member_leases(member_paths, instance_id):
    """Non-blocking acquire of every member lease; raise if any is held.

    The acquisition is all-or-nothing with no hold-and-wait: if one
    lease is busy the already-acquired leases are released and
    :class:`MigrationLockedError` is raised, so a batch of instances can
    never deadlock.  Returns a list of :class:`_MemberLease` in member
    order; callers release them (process death releases them
    automatically -- that is the takeover guarantee).
    """
    acquired = []
    try:
        for path in member_paths:
            fh = open(_lease_path(path), "a+b")
            try:
                fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError as exc:
                if exc.errno not in (errno.EAGAIN, errno.EACCES,
                                     errno.EWOULDBLOCK):
                    fh.close()
                    raise
                holder = None
                try:
                    fh.seek(0)
                    raw = fh.read()
                    if raw:
                        holder = json.loads(raw).get("instance")
                except (OSError, ValueError):
                    holder = None
                fh.close()
                for lease in acquired:
                    lease.release()
                raise MigrationLockedError(path, holder) from exc
            lease = _MemberLease(path, fh, instance_id)
            lease.start_heartbeat()
            acquired.append(lease)
    except BaseException:
        for lease in acquired:
            lease.release()
        raise
    return acquired


# ---------------------------------------------------------------------------
# One coordinated instance
# ---------------------------------------------------------------------------


def _prior_work_present(paths, work_dir):
    if os.path.exists(os.path.join(work_dir, "linked-committed")):
        return True
    for path in paths:
        member_dir = path + ".migrate-linked-tmp"
        try:
            if os.path.isdir(member_dir) and os.listdir(member_dir):
                return True
        except OSError:
            continue
    return False


def migrate_linked_logs_coordinated(spec, *, instance_id=None,
                                    segment_size=None, quiesce=None,
                                    audit=None):
    """Run one coordinated linked-migration instance.

    Acquires the non-blocking member leases, then drives the ordinary
    linked migration against this spec's own work directory and group
    lock while holding the leases; raises
    :class:`MigrationLockedError` with no side effect when any member is
    currently leased by another instance.  Returns
    :class:`CoordinatedMigrationResult`; ``took_over`` says a previous
    (dead) instance had left checkpoints or a commit marker for this
    spec, which this run resumed.
    """
    groups, links, on_bad = spec
    groups = _normalize_groups(groups)
    links = _normalize_links(links, len(groups))
    paths = [path for group in groups for path in group]
    _probe_members(paths)
    if instance_id is None:
        instance_id = f"{os.getpid()}-{id(spec)}"

    work_dir = spec_work_dir(groups, links, on_bad)
    os.makedirs(work_dir, exist_ok=True)
    lock_path = spec_lock_path(groups, links, on_bad)
    took_over = _prior_work_present(paths, work_dir)

    leases = acquire_member_leases(paths, instance_id)
    kwargs = {}
    if segment_size is not None:
        kwargs["segment_size"] = segment_size
    if quiesce is not None:
        kwargs["quiesce"] = quiesce
    try:
        result = migrate_linked_logs(
            groups, links=links, on_bad=on_bad, audit=audit,
            _group_dir_override=work_dir,
            _lock_path_override=lock_path,
            _member_lock_fhs=[lease.fh for lease in leases],
            **kwargs,
        )
    finally:
        for lease in leases:
            lease.release()
    return CoordinatedMigrationResult(0, instance_id, result, took_over)


# ---------------------------------------------------------------------------
# Batch scheduling: parallel disjoint specs, ordered overlap
# ---------------------------------------------------------------------------


def run_coordinated_migrations(specs, *, instance_ids=None,
                               wait_timeout=30.0, poll_interval=0.01,
                               segment_size=None, quiesce=None, audit=None):
    """Run several migration instances with member-level coordination.

    Each spec is one instance.  Specs with disjoint member sets run in
    parallel; whenever two specs share a member the lower-index spec
    completes first, so the batch finishes byte-for-byte like the same
    specs run serially in list order.  A holder that crashes releases
    its kernel leases and the waiting attempt then takes over from the
    durable checkpoints (no rescan of completed members).

    Returns a tuple of :class:`CoordinatedMigrationResult` in spec
    order.  *wait_timeout* bounds how long a spec waits for a contended
    member; expiring it re-raises :class:`MigrationLockedError`.
    """
    normalized = []
    for spec in specs:
        groups, links, on_bad = (
            (spec.groups, spec.links, spec.on_bad)
            if isinstance(spec, MigrationSpec) else spec
        )
        groups = _normalize_groups(groups)
        links = _normalize_links(links, len(groups))
        if on_bad not in ("strict", "skip"):
            raise ValueError(
                f"on_bad must be 'strict' or 'skip', got {on_bad!r}"
            )
        normalized.append(MigrationSpec(groups, list(links), on_bad))
    count = len(normalized)
    if count == 0:
        return ()
    if instance_ids is None:
        instance_ids = [f"inst-{i}" for i in range(count)]
    if len(instance_ids) != count:
        raise ValueError("one instance id per spec is required")

    # A spec may start once every lower-index spec that shares a member
    # with it has finished.  Disjoint specs are not ordered relative to
    # each other and run concurrently; the overlap graph is honored in
    # list order, giving the serial-in-order byte identity.
    member_to_specs = {}
    for index, (groups, _links, _mode) in enumerate(normalized):
        for group in groups:
            for path in group:
                member_to_specs.setdefault(os.path.abspath(path), []).append(
                    index
                )
    predecessors = [set() for _ in range(count)]
    for indices in member_to_specs.values():
        ordered = sorted(indices)
        for pos, index in enumerate(ordered):
            predecessors[index].update(ordered[:pos])

    finished = [threading.Event() for _ in range(count)]
    outcomes = [None] * count
    errors = [None] * count

    def worker(index):
        groups, links, on_bad = normalized[index]
        for pred in predecessors[index]:
            finished[pred].wait()
        paths = [path for group in groups for path in group]
        work_dir = spec_work_dir(groups, links, on_bad)
        os.makedirs(work_dir, exist_ok=True)
        took_over = _prior_work_present(paths, work_dir)
        deadline = time.monotonic() + max(0.0, wait_timeout)
        try:
            while True:
                try:
                    leases = acquire_member_leases(paths,
                                                   instance_ids[index])
                except MigrationLockedError:
                    if time.monotonic() >= deadline:
                        raise
                    time.sleep(poll_interval)
                    continue
                kwargs = {}
                if segment_size is not None:
                    kwargs["segment_size"] = segment_size
                if quiesce is not None:
                    kwargs["quiesce"] = quiesce
                try:
                    result = migrate_linked_logs(
                        groups, links=links, on_bad=on_bad, audit=audit,
                        _group_dir_override=work_dir,
                        _lock_path_override=spec_lock_path(
                            groups, links, on_bad),
                        _member_lock_fhs=[lease.fh for lease in leases],
                        **kwargs,
                    )
                finally:
                    for lease in leases:
                        lease.release()
                outcomes[index] = CoordinatedMigrationResult(
                    index, instance_ids[index], result, took_over
                )
                return
        except BaseException as exc:
            errors[index] = exc
        finally:
            finished[index].set()

    threads = [
        threading.Thread(target=worker, args=(i,), name=f"migrate-{i}")
        for i in range(count)
    ]
    for thread in threads:
        thread.start()
    for index, thread in enumerate(threads):
        thread.join()
    for exc in errors:
        if exc is not None:
            raise exc
    return tuple(outcomes)
