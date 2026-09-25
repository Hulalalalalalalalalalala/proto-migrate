# proto-migrate

Versioned message codec for records written by older releases: readers decode any known version and migrate it forward to the current shape.

## Requirements

Python 3.11 or newer. Standard library only.

## Install

    python3 -m pip install -e .

## Run

    python3 -m proto_migrate --selftest

## Online, resumable log migration

`migrate-log` migrates an append-only JSONL log (mixed v1/v2/v3
records, one object per line) to the current version **while other
processes keep appending to it**.  Appenders never have to stop or wait
for a silent window: every concurrently appended record is migrated in
its original order, with zero loss and zero duplication.  The pass is
streaming and never loads the file into memory:

    python3 -m proto_migrate migrate-log app.log            # strict (default)
    python3 -m proto_migrate migrate-log app.log --skip

Python entry point:

```python
from proto_migrate import migrate_log_file, read_log

result = migrate_log_file("app.log", on_bad="strict")  # or on_bad="skip"
# result.records_migrated / records_skipped / records_salvaged / replaced
# result.post_commit_error is a warning string, never a failure

records = read_log("app.log")   # one consistent view during a migration
```

Options: `--strict` (default) / `--skip` choose bad-record handling;
`--segment-size BYTES` (default 16 MiB) rotates the temp segments;
`--quiesce-ms MS` (default 50 ms) is how long EOF must hold before the
input is considered complete while appenders may still be writing.

### Checkpoints and resume

Migrated output is first written to fsynced, size-bounded segment files
in `<file>.migrate-tmp/`.  Every completed segment is covered by a
durable record in `<file>.migrate-tmp/checkpoint` containing the input
offset already consumed, the migrated and skipped record counts, the
cumulative "needs rewrite" flag, and the segment's durable size (the
record is fsynced together with its directory).

If the process is killed at *any* point, rerunning the same command
continues from the newest valid checkpoint: already migrated bytes are
neither rescanned nor rewritten, and the finished file is byte-for-byte
identical to one uninterrupted run.  Before trusting a checkpoint the
rerun validates it and **rolls back to the previous complete
checkpoint** when it finds:

- a corrupt checkpoint file or a torn half-line at its end;
- a referenced segment that is missing or shorter than recorded;
- a source-inode mismatch (an earlier run already committed) or a
  different `--strict/--skip` policy (strict must re-examine lines a
  prior skip run passed).

Rollback truncates the checkpoint log to its good prefix, truncates
retained segments to their recorded sizes, and deletes unknown
segments; the affected input range is then migrated again.

### Atomic commit and error classification

After the scan and a final quiesced drain, a commit marker
(`<file>.committed`) is published and fsynced, then the assembled file
is atomically `os.replace`d over the original followed by a directory
fsync.  **The atomic rename is the single success/failure border:**

- a failure before the rename (I/O error, or a bad record in strict
  mode) leaves the original file and every completed segment intact;
- a durability or cleanup failure *after* the rename (directory fsync,
  commit marker, temp cleanup) never turns the already-committed
  migration into a failure: the run exits `0` and the fault is reported
  on stderr as a `warning:` (and in `MigrationResult.post_commit_error`).

Exit codes: `0` success (including post-commit durability warnings),
`1` I/O error, `2` usage error, `3` bad record in strict mode.

### Concurrent readers and appenders

- **Readers**: use `read_log(path)`, which returns one complete,
  version-consistent snapshot — either the whole pre-migration content
  decoded as stored, or the whole post-migration content normalized to
  the current version.  Old and new field shapes never mix inside one
  view, not even when a writer reopens the path by name and appends
  during the migrator's wrap-up (the commit marker gates the switch and
  every snapshot pins one inode; a record caught mid-append, without a
  terminating newline, is excluded).  Opening the path directly gets
  the same atomic guarantee at the rename itself (`os.replace`); a
  descriptor opened before the rename keeps reading the old inode.
- **Appenders must write whole records in one `O_APPEND` write.**
  Records appended during the scan are drained into the output.  After
  the commit, a convergence phase drains writers that still hold the old
  descriptor (via the unlinked old inode) and rewrites old-format
  records a path-reopening writer landed on the new inode via further
  atomic replacements.  Every repair rebuilds the post-base suffix in
  rename-generation order, so a record whose single whole-record write
  straddles a rename still lands in its original position.  Convergence
  finishes only after two consecutive fully quiet rounds (the last
  operations are reads, never a rename); a writer that appends
  continuously past the hard backstop (`max(5 s, quiesce*200)`) is not
  blocked on forever — rerun the command, it is idempotent and catches
  up.  The unavoidable boundary is a writer preempted *between* opening
  the path and issuing its one write for longer than convergence runs:
  that record reaches an unlinked inode no path-based tool can see, so
  the append must be one prompt whole-record write (the stated
  contract).

### Bad records

Corrupt lines, JSON parse failures, missing/non-integer/unsupported `v`,
illegal fields, and `NaN`/`Infinity` amounts (including numeric
literals such as `1e999` that overflow to infinity) are bad records.
An amount of `-0.0` is preserved verbatim.

- `--strict`: stop at the first bad line, exit with code **3**, and
  leave the original file untouched.
- `--skip`: skip bad lines, keep migrating, and write one audit entry
  per skipped line to **stderr** as `<lineno>:<first 32 raw bytes>`.

### Idempotency

If every record is already at the current version in the canonical
encoding and there is nothing to skip, the file is left byte-for-byte
untouched (no rename, no mtime change). A second run rewrites no bytes,
and a run that recovered from a crash finishes byte-identical to a run
that was never interrupted.

## Group migration: `migrate-logs`

`migrate-logs` migrates a whole **group** of log files in one shot.
The group is indivisible: either every member ends up at the current
version or every member stays exactly as it was — no partially
migrated group is ever left behind after a handled failure or a kill.
The member list is always given explicitly, and each member may be
appended to by a *different* process while the migration runs:

    python3 -m proto_migrate migrate-logs a.log b.log c.log          # strict
    python3 -m proto_migrate migrate-logs a.log b.log c.log --skip

Python entry points:

```python
from proto_migrate import migrate_log_group, read_log_group

result = migrate_log_group(["a.log", "b.log"], on_bad="strict")  # or "skip"
# result.records_migrated / records_skipped / records_salvaged / replaced
# result.members is a tuple of per-member MigrationResult
# result.post_commit_error is a warning string, never a failure

records = read_log_group(["a.log", "b.log"])  # one consistent group snapshot
```

The member list must be a non-empty sequence without duplicate paths
(a single path string is not a sequence); a non-sequence raises
`TypeError`, an empty or duplicated list raises `ValueError` (CLI exit
`2`).  A member that is missing or unreadable raises
`FileNotFoundError` at both entry points (CLI exit `1`).

### Group work files and the commit marker

Group metadata lives next to the **first** listed member:

- `.migrate-logs.lock` (sibling of the first member) — the group lock;
- `.migrate-logs-tmp/` — the group work directory, holding
  `manifest.json` (the member list plus the bad-record policy) and the
  **group commit marker `group-committed`**;
- `<file>.migrate-group-tmp/` — one directory per member (a sibling of
  that member, so every rename stays within one directory and one
  filesystem), holding that member's segmented output, its local
  `checkpoint`, the assembled `final`, and the `prepared` marker;
- `<file>.migrate-group-staged` and `<file>.migrate-group-backup` —
  short-lived staging/backup names used by the rename protocol;
- `<file>.committed` — the same per-file commit marker the single-file
  entry publishes before its rename.

**Purpose of the commit marker.** `group-committed` is the single
success/failure border of the group.  It is created (temp file +
atomic rename + directory fsync) only after *every* member's rename
has landed and before any original is deleted.  A rerun checks it
first: marker missing ⇒ the whole group is rolled back to the
originals and the migration is retried; marker present ⇒ every member
is completed forward (never rolled back).  The per-file
`<file>.committed` marker gates `read_log`/`read_log_group`, so a
reader can never open a post-rename inode under the pre-commit policy;
like the single-file entry it is intentionally **kept** after a
successful run so readers keep getting normalized views even when a
writer later reopens the path and appends old-format records (an
idempotent rerun rewrites those bytes).

**Cleanup timing.** After the group marker is published, the run
converges racing appenders per member, deletes each
`<file>.migrate-group-backup`, and finally removes the group work
directory `.migrate-logs-tmp/` (taking `group-committed`, the manifest
and member temp state with it) and the per-member
`<file>.migrate-group-tmp/` directories.  If that cleanup fails, or
the process is killed first, nothing is lost: the marker survives, the
next run finishes forward and sweeps the leftovers.  Any such
durability/cleanup failure after the border is reported on stderr as a
`warning:` (and in `post_commit_error`); the run still exits `0`.  The
lock files are empty, reusable, and left in place like the
single-file lock.

### Cross-file two-phase rename and rollback

Each member whose bytes change goes through the recoverable sequence
`final → <file>.migrate-group-staged`, publish `<file>.committed`,
`<file> → <file>.migrate-group-backup`,
`<file>.migrate-group-staged → <file>` (each step directory-fsynced).
Only once all members have completed this sequence is
`group-committed` published.  A kill mid-sequence is resolved
deterministically on the rerun from whichever of `staged`/`backup`/the
live path survive: without the group marker every member is put back
byte-for-byte as it was; with it the remaining renames are completed.

### Checkpoints and group resume

Each member writes the same fsynced segments and local checkpoint as a
single-file run.  Once a member reaches the group commit phase it
records `prepared`; on rerun it is neither rescanned nor rewritten —
its `final` is reused — so an interrupted group finishes
byte-for-byte identical to one uninterrupted run.  A member failure
*before* the group border (including the first bad line in strict
mode) rolls every prepared member back: no member's original file is
modified and no partial intermediate is left in durable state.

### Group snapshots and bad records

`read_log_group([...])` reads the whole group in one call and returns a
flat record list (members in list order, lines in file order) with no
old/new field mix between or within members — including while a
path-reopening appender writes old records during wrap-up.  While the
entire group is still pre-commit and entirely old-format, records are
returned exactly as stored (v1/v2); as soon as any member has crossed
its border, every member is normalized to the current version in
memory, which is exactly the eventual committed content.

Bad-record rules are unchanged.  In strict mode the **first bad line of
the whole group** (members in list order) raises `ValueError` and aborts
before any rename; exit code is `3`.  In skip mode migration continues
and each skipped record is written to **stderr** as

    <filename>:<lineno>:<first 32 raw bytes>

Missing/non-integer/unsupported `v`, wrong field types, and `NaN`,
`Infinity` or values overflowing to infinity (e.g. `1e999`) are bad
records; `-0.0` is preserved verbatim.  The summary counters count
only records the invocation newly migrates; an idempotent finish reports
`migrated=0`.

## Linked migration with cross-group references: `migrate-linked-logs`

`migrate-linked-logs` migrates **several groups** of log files whose
records reference each other across groups — for example detail records
whose `order_id` names an order record in the orders group.  All groups
and all references migrate as one unit: either every group ends at the
current version with every reference resolving to an existing, complete
target record, or every group stays exactly as it was.  Each member may
still be appended to by a different process while the migration runs:

    python3 -m proto_migrate migrate-linked-logs \
        --group orders.log --group details.log \
        --link 1:0:order_id:order_id                # strict (default)
    python3 -m proto_migrate migrate-linked-logs \
        --group orders.log --group details.log \
        --link 1:0:order_id:order_id --skip

`--group` names one member list (repeat per group, in group order);
`--link SRC:DST:SRC_FIELD:DST_FIELD` declares that a record of group
`SRC` whose string field `SRC_FIELD` equals `k` references the record
of group `DST` whose `DST_FIELD` is `k` (repeatable; group indices
follow the `--group` order).

Python entry points:

```python
from proto_migrate import (
    migrate_linked_logs, read_linked_logs, read_linked_logs_stream,
)

result = migrate_linked_logs(
    [["orders.log"], ["details.log"]],
    links=[(1, 0, "order_id", "order_id")],
    on_bad="strict",            # or "skip"
)
# result.records_migrated / records_skipped / references_bad
# result.records_salvaged / replaced / members / post_commit_error

snapshot = read_linked_logs([["orders.log"], ["details.log"]])
# one flat record list per group, version-consistent across all groups

batch, cursor = read_linked_logs_stream([["orders.log"], ["details.log"]],
                                        None, batch_records=1000)
while cursor is not None:
    batch, cursor = read_linked_logs_stream(
        [["orders.log"], ["details.log"]], cursor, batch_records=1000)
# the same snapshot, in cursor-resumable batches
```

The group list must be a non-empty sequence of non-empty member
sequences with no path repeated anywhere (a bare path string is not a
sequence): a non-sequence raises `TypeError`, an empty or duplicated
list raises `ValueError` (CLI exit `2`).  A member that is missing or
unreadable raises `FileNotFoundError` at both entry points (CLI exit
`1`).

### Bad references

Four classes of bad references are recognised: **dangling** (no live
record in the target group has the referenced key), **target skipped**
(the key belongs to a line skipped as a bad record, or to a record
dropped over its own bad references), **cyclic** (the reference sits on
a record-level reference cycle), and **illegal target version** (the
key belongs to a line whose version key is missing, non-integer or
unsupported).

- `--strict`: the first bad line *or* bad reference of the whole run
  (groups in list order, members in list order, lines in file order)
  raises `LinkedBadReferenceError` / `GroupBadRecordError` (both
  `ValueError`) before any rename; every original stays untouched, exit
  code `3`.  Bad lines and bad references are compared in that global
  order and the earliest one decides which exception is raised, so a
  bad reference that sorts before a later member's bad line is reported
  first.
- `--skip`: bad lines are skipped with the usual audit entry, and
  records holding bad references are dropped from the migrated output;
  each dropped record is audited to **stderr** as
  `<filename>:<lineno>:<first 32 raw bytes>`.  The summary counts bad
  records (`skipped`) and bad references (`refs_bad`) separately, and
  both counters only cover records this invocation newly migrates and
  references it newly resolves.

Migration output uses the unchanged encoding, field defaults and drop
rules; the `-0.0` / non-finite amount semantics are exactly the
single-file ones.

### Multi-instance coordination

Several migration instances may run concurrently, over overlapping or
disjoint group sets.  Every member is held under a non-blocking
**lease** — an exclusive `flock` on its `<path>.migrate.lock` file, the
same lock the single-file and single-group migrators wait on — so
overlapping members are mutually exclusive while disjoint group sets
advance in parallel.  **The group work directory layout is**
`.migrate-linked-tmp-<hash>/` next to the first group's first member,
where `<hash>` is a 16-hex-character SHA-1 of the absolute member
lists (this hashed layout is the declared contract: a distinct group
set anchored in the same directory owns a distinct directory, and the
manifest inside names the member lists, the bad-record policy and the
link set).  Each member owns its own ` <path>.migrate-linked-tmp/`
directory.  A lease held by a live instance raises
`MigrationLockedError` (CLI exit `1`) instead of waiting; a holder that
finishes or disappears — however abruptly — releases its leases via the
OS, and the next instance reclaims them and takes over from the durable
checkpoints without rescanning prepared members.  Takeover and resume
leave no partial migration shape: the outcome is byte-for-byte
identical to running the same instances serially.  Lease and index
state lives only in local files and can always be rebuilt.

#### Linked work files

Inside the group work directory `.migrate-linked-tmp-<hash>/`:

- `manifest.json` — the absolute member lists, the bad-record policy
  and the link declarations;
- `linked-committed` — the group commit marker (the single
  success/failure border);
- `refs.sqlite3` — the streaming reference-resolution spill database,
  rebuilt on every uncommitted run;
- `linked-resolved` — the durable reference-resolution outcome written
  by online compaction (see below); absent on an uncompacted run.

Inside each member's `<path>.migrate-linked-tmp/`:

- `seg-NNNNNN` — fsynced output segments, the local `checkpoint` log,
  the assembled `final`, and the `prepared` / `linked-prepared`
  markers (same semantics as a single-group run);
- `lines` — the durable per-line index classifying every consumed
  source line (removed once compaction has published `linked-resolved`
  and the member's `compacted` receipt);
- `compacted` — present when the member's durable state is in the
  compact form (one merged segment, one checkpoint record, no line
  index);
- `linked-final` — output filtered of bad-reference records, used by
  the member's rename.

## Read-only rehearsal: `rehearse-linked-logs`

`rehearse-linked-logs` is the read-only dry run of a linked migration:
it takes the **same group sets and `--link` declarations** as
`migrate-linked-logs` and reports exactly what that migration would do,
without changing a single file.

    python3 -m proto_migrate rehearse-linked-logs \
        --group orders.log --group details.log \
        --link 1:0:order_id:order_id                # strict (default)
    python3 -m proto_migrate rehearse-linked-logs \
        --group orders.log --group details.log \
        --link 1:0:order_id:order_id --skip

Python entry point:

```python
from proto_migrate import rehearse_linked_logs

report = rehearse_linked_logs(
    [["orders.log"], ["details.log"]],
    links=[(1, 0, "order_id", "order_id")],
    on_bad="strict",            # or "skip"
)
# report.members[i]: per-member records_migrated / records_skipped /
#   records_dropped / would_replace / predicted_bytes, with every bad
#   record and dropped record located (group, member, 1-based line)
# report.bad_records / report.bad_references: located in global
#   group/member/line order
# report.discarded: the exact skip-mode discard list (with the exact
#   audit bytes the real migration writes to stderr)
# report.references_bad: the bad-reference edge count
# report.first_error / report.first_error_kind: the strict-mode global
#   first-error attribution (GroupBadRecordError or
#   LinkedBadReferenceError with the identical path/line/message)
```

Guarantees:

- **Strictly read-only.** It takes no member lease, creates no lock or
  work file, writes nothing next to any member (its spill database
  lives in a private temporary directory it removes on exit), and never
  blocks an appender or another instance.  A rehearsal while an active
  instance holds the leases and is migrating the same members proceeds
  normally; it never raises `MigrationLockedError`.
- **Pinned, non-drifting locations.** Each member is pinned at open
  time (one inode, one frozen length) like the streaming snapshot; only
  complete lines in that pinned prefix are considered, so the reported
  group/member/line locations never drift while appenders keep writing.
- **Record-for-record agreement with the real run.** Every verdict
  (kept / skipped bad record / dropped bad-reference record) for a line
  in the pinned prefix matches the real migration's verdict for that
  same line, including the global first-error attribution and the exact
  skip-mode audit bytes.  With the files quiet between the rehearsal
  and the migration, aggregate counters and the per-member rewrite
  counts match the real `LinkedMigrationResult` exactly; the report's
  `predicted_bytes` equal the migrated member bytes.
- Bad-record and bad-reference rules, exception types, audit format
  and validation/exit-code taxonomy are exactly the migration's; the
  rehearsal CLI itself always exits `0` on a successful read-only pass
  (it *reports* the strict first error instead of raising it).

## Online compaction: `compact-linked-logs`

Repeatedly killed/resumed linked migrations accumulate durable state:
one checkpoint record per output segment in every member's `checkpoint`
log and one record per consumed source line in every member's `lines`
index.  `compact-linked-logs` folds that state into a deterministic
compact form whose size grows with the **member count, not the total
line count**:

    python3 -m proto_migrate compact-linked-logs \
        --group orders.log --group details.log \
        --link 1:0:order_id:order_id [--strict|--skip]

Python entry point:

```python
from proto_migrate import compact_linked_logs

result = compact_linked_logs(
    [["orders.log"], ["details.log"]],
    links=[(1, 0, "order_id", "order_id")], on_bad="skip",
)
# result.members_prepared / members_compacted / references_bad;
# result.first_error carries the pending strict-mode first problem.
```

Compaction prepares any not-yet-prepared member (never rescanning a
prepared one), resolves references once into the durable
`linked-resolved` record, then per member concatenates the retained
segments into one, collapses the checkpoint log to header + one record,
publishes the `compacted` receipt and removes the per-line `lines`
index.  The original log files are never modified and no commit marker
is published; a later `migrate-linked-logs` resumes every member from
its receipt **without rescanning it** and finishes byte-for-byte
identically to a run that was never compacted — the migrated output is
byte-identical, resume semantics and the fresh-only summary counters
are unchanged (a run that only finishes compacted state reports zero).
Compaction takes the same non-blocking member leases as the migration
(`MigrationLockedError` while one is held).  It is itself
crash-recoverable: killed at any point, a rerun reuses `compacted`
receipts and the `linked-resolved` record, never rescans a completed
member, and reaches the same compact state.  A strict-mode problem is
not raised by compaction — the files stay untouched — but its
attribution is reported (`result.first_error`) and the subsequent
migration raises the identical error.

### Durability, resume and snapshots

Every member is prepared with the same fsynced segments and durable
checkpoints as a single-group run, plus a durable per-line index
classifying every consumed source line.  Reference resolution is
streaming with bounded memory: keys and edges live in an SQLite spill
file inside the group work directory (`.migrate-linked-tmp-<hash>/`
next to the first group's first member), never loading a whole group
into memory.  The group commit marker `linked-committed` is the single
success/failure border — published only after every member's rename
landed — and a kill at any point reruns deterministically: without the
marker every member is rolled back byte-for-byte (prepared members
resume from their checkpoints and line indexes without rescanning),
with it the remaining renames are completed forward.  A recovery run
that finds the marker **converges appends still living on a member's
backup inode before the backup is removed**, so no record appended
across the crash is lost.  The result is byte-for-byte identical to one
uninterrupted run, and durability or cleanup failures after the marker
are warnings (exit `0`), never failures.

`read_linked_logs` returns one consistent snapshot of all groups in a
single call: old and new field shapes never mix between groups, between
members or inside a member — including while a path-reopening appender
writes old-format records during wrap-up.

### Streaming snapshots: `read_linked_logs_stream`

`read_linked_logs_stream(groups, cursor=None, batch_records=4096)`
serves the same version-consistent snapshot as `read_linked_logs`, but
in cursor-resumable batches.  Each call returns
`(records, next_cursor)`: up to `batch_records` decoded records —
member-aligned, advancing strictly in group, member and line order —
plus an opaque string cursor; `next_cursor` is `None` when the snapshot
is exhausted.  Concatenating all batches reproduces the one-shot read
exactly.  The snapshot is pinned at the first call (every member's
inode is held open and its length frozen), so no batch repeats or loses
records and old/new field shapes never mix, even with appenders writing
throughout.  Members are read line by line — no group is ever loaded
into memory wholesale.  The cursor is a plain string and may be
persisted between calls (including across processes, while the pinned
inodes still resolve by path).  A non-string cursor raises `TypeError`,
corrupt cursor content raises `ValueError`, and a cursor whose current
member is missing or unreadable raises `FileNotFoundError` — batches
already returned are unaffected.

## Tests

    python3 -m unittest discover -s tests -t .

The migration tests cover three-way concurrency (migrator + appender +
reader), resume from checkpoints, checkpoint corruption / missing
segment / torn-tail rollback, real `SIGKILL` crash injection and
recovery, post-commit fault classification, and idempotent reruns.  The
linked-migration tests additionally cover the streaming snapshot cursor
(batched reads equal to the one-shot read, cursor persistence, the
TypeError/ValueError/FileNotFoundError taxonomy), multi-instance leases
(mutual exclusion on overlapping members, parallel disjoint instances,
crash takeover), the strict-mode global first-error ordering, and
post-commit-crash backup convergence.  The rehearsal and compaction
tests cover report/real-run agreement (counts, located verdicts, exact
audit bytes, predicted bytes), read-only behaviour under live leases
and appenders, the deterministic compact form, compaction crash
recovery at every crash point, zero-rescan resume from compacted
state, byte-identical finishing migrations, and descriptor reclaim for
abandoned stream cursors.

## Public interface

`proto_migrate.dumps(message) -> bytes` encodes the current version.
- `proto_migrate.loads(data) -> dict` decodes any known version.
- `proto_migrate.migrate(message, target_version) -> dict` converts a decoded message.
- `proto_migrate.migrate_log_file(path, ...) -> MigrationResult` runs the online, resumable in-place log migration.
- `proto_migrate.read_log(path) -> list[dict]` returns one version-consistent snapshot of a migrating log.
- `proto_migrate.migrate_log_group(paths, ...) -> GroupMigrationResult` migrates an explicit group of logs as one all-or-nothing unit.
- `proto_migrate.read_log_group(paths) -> list[dict]` returns one version-consistent snapshot of a whole group.
- `proto_migrate.migrate_linked_logs(groups, links=..., ...) -> LinkedMigrationResult` migrates several groups with cross-group reference integrity as one all-or-nothing unit; raises `MigrationLockedError` while a needed member lease is held by a live instance.
- `proto_migrate.rehearse_linked_logs(groups, links=..., ...) -> RehearsalReport` performs the read-only rehearsal of a linked migration: per-member rewrite counts, globally ordered bad-record/bad-reference locations, strict-mode first-error attribution and the skip-mode discard list, without writing anything or taking a lease.
- `proto_migrate.compact_linked_logs(groups, links=..., ...) -> CompactionResult` compacts the durable checkpoint/line-index state of an uncommitted linked migration into deterministic, member-count-sized, crash-recoverable form without rescanning completed members.
- `proto_migrate.read_linked_logs(groups) -> list[list[dict]]` returns one version-consistent snapshot across all linked groups.
- `proto_migrate.read_linked_logs_stream(groups, cursor=None, batch_records=...) -> (list[dict], str | None)` returns the same snapshot in cursor-resumable, member-aligned batches.
- `proto_migrate.close_linked_logs_stream(cursor)` releases the descriptors a still-open stream pinned (the cursor stays resumable); idle sessions are also reaped automatically.
- `proto_migrate.VERSIONS -> tuple[int, ...]` supported versions, ascending.
- `proto_migrate.CURRENT_VERSION -> int` the version `dumps` writes.

## Tests

    python3 -m unittest discover -s tests -t .

## Limits

Values must be JSON-compatible scalars, lists and objects.
No schema registry and no network lookup.
Unknown newer versions are rejected rather than guessed.
