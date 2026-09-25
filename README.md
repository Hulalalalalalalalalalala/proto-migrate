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
When the marker is present, a finishing rerun first **converges any
append still landing on a retained backup inode and only then deletes
the backup**, so a writer preempted between opening the old path and
its single whole-record write loses nothing.

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
only records the invocation newly rewrites — the group total is exactly
the sum of the per-member counts (a member left byte-for-byte
untouched, including an already canonical one, contributes zero on
both levels); an idempotent finish reports `migrated=0`.

## Linked multi-group migration: `migrate-linked-logs`

`migrate-linked-logs` migrates **several log groups at once**, keeping
the references *between* them intact.  The caller gives both the groups
(each an explicit member list; group indices are zero based in list
order) and a set of reference declarations `SRC:DST`.  Records carry no
special reference field — for every declared group edge *SRC → DST*,
**every** record of group SRC points at the first record of group DST
with the same `order_id` in member-list order and then line order (when
the two groups are the same, that first match may be the record
itself):

    python3 -m proto_migrate migrate-linked-logs \
        --group orders.log --group details.log --ref 1:0
    python3 -m proto_migrate migrate-linked-logs \
        --group g1/a.log g1/b.log --group g2/c.log --group g3/d.log \
        --ref 0:1 --ref 2:1 --ref 2:0 --skip

`--group FILE [FILE ...]` is repeatable (one option per group, its
members follow it); `--ref SRC:DST` is repeatable and writes one source
group index, a colon, and one target group index.  When SRC and DST name
the same group, the first match may be the record itself, i.e. a
length-one self reference.

Python entry points:

```python
from proto_migrate import migrate_linked_groups, read_linked_logs

result = migrate_linked_groups(
    [["orders.log"], ["d1.log", "d2.log"]], [(1, 0)],
    on_bad="strict")  # or "skip"
# result.records_migrated / records_skipped / references_bad /
#        records_salvaged / replaced / members / post_commit_error

records = read_linked_logs([["orders.log"], ["d1.log", "d2.log"]])
# groups, then members, then lines in order
```

The whole set is indivisible: either **every** member of every group is
migrated to the current version together with every reference, or
**every** member stays exactly as it was — no partially migrated group
is ever left in durable state after a handled failure or a kill.

### Bad references

There are four kinds, all validated after the members quiesce:

- **missing** — the target group has no record with that order id;
- **target-skipped** — the earliest matching target line is a bad line
  the run skips, or a good record itself removed for a bad reference;
- **illegal-target-version** — the earliest matching target line parses
  as JSON but carries a missing/non-integer/unsupported `v`;
- **cycle** — following the declared edges from the source record loops
  back to that same source line (a self loop included).  The check is
  per edge: another declaration of the same record may still resolve.

In strict mode the run stops at the **globally first bad line or bad
reference** (groups in order, members in order, lines in order; edges
of one line in declaration order), raises `LinkedBadReferenceError` /
`GroupBadRecordError` (both `ValueError`, CLI exit `3`) and rolls every
member back before any rename.  In skip mode the offending record (and
anything that only references removed records, transitively) is
skipped; one audit entry per removed record is written to **stderr** as

    <filename>:<lineno>:<first 32 raw bytes>

Bad **lines** (`records_skipped`) and bad **references**
(`references_bad`) are counted separately; like every other counter
both count only records/references this invocation newly processes, so
an idempotent rerun reports all zeros.

### Streaming, work files and crash recovery

Every member streams through the same fsynced, size-bounded segments
and durable local checkpoint as a group run; alongside the segments the
scan appends a small per-record index row to a spill file in the
member's `<file>.migrate-linked-tmp/`, checkpointed and rolled back
together with the output prefix.  After every member quiesces the spill
files are bulk-loaded into a temporary on-disk SQLite database
(`.migrate-linked-tmp/refs.sqlite`, rebuilt from scratch every run and
trivially reconstructable after a crash) where earliest-target
resolution, target existence, the skip-mode transitive drop closure and
cycle detection run — no whole group and no reference graph is ever
held in memory.

Commit is the same recoverable two-phase rename as `migrate-logs`
(`final → staged`, per-file `.committed` marker, `path → backup`,
`staged → path`), with linked debris names
(`.migrate-linked-staged` / `.migrate-linked-backup` /
`.migrate-linked-tmp`) and a single border marker
`.migrate-linked-tmp/linked-committed` next to the first member.  A kill
before the border rolls every rename back and resumes members from
their checkpoints (prepared members are neither rescanned nor
rewritten; the finished bytes are byte-for-byte identical to one
uninterrupted run); a kill after it completes the renames forward, first
converges appends still landing on the backup inode and deletes the
backup only once drained, and reduces any later durability/cleanup
fault to a stderr `warning:` with exit `0`.  Handled pre-border
failures sweep all work and leave every original byte-for-byte
untouched.

`read_linked_logs([[...], ...])` returns one flat, version-consistent
snapshot (groups, members, then lines in order): while the whole set is
still pre-commit it is served exactly as stored (v1/v2 together are the
uniform "old" world); once any member crosses its border the whole
snapshot is normalized to the current version in memory — old/new field
shapes never mix between or within members, even with a path-reopening
appender writing old records during wrap-up, and references never
dangle or point at half a record.

The nested group list and the reference list validate like
`migrate_log_group`: a non-sequence raises `TypeError` (a flat member
list and a bare `(src, dst)` pair are non-sequences of the required
shape and raise `ValueError` instead); an empty/duplicate member list, a
repeated declaration, or a reference index out of range raises
`ValueError` (CLI exit `2`); a missing/unreadable member raises
`FileNotFoundError` at both entry points (CLI exit `1`).  Exit codes
otherwise match the other subcommands: `0` success (post-border
warnings included), `1` I/O error, `3` strict-mode bad
record/reference.  Bad-record rules, the `-0.0` / overflow amount
semantics, field defaults, and the existing single-file, single-group
and selftest entry points are unchanged.

## Tests

    python3 -m unittest discover -s tests -t .

The migration tests cover three-way concurrency (migrator + appender +
reader), resume from checkpoints, checkpoint corruption / missing
segment / torn-tail rollback, real `SIGKILL` crash injection and
recovery, post-commit fault classification, and idempotent reruns.

## Public interface

`proto_migrate.dumps(message) -> bytes` encodes the current version.
- `proto_migrate.loads(data) -> dict` decodes any known version.
- `proto_migrate.migrate(message, target_version) -> dict` converts a decoded message.
- `proto_migrate.migrate_log_file(path, ...) -> MigrationResult` runs the online, resumable in-place log migration.
- `proto_migrate.read_log(path) -> list[dict]` returns one version-consistent snapshot of a migrating log.
- `proto_migrate.migrate_log_group(paths, ...) -> GroupMigrationResult` migrates an explicit group of logs as one all-or-nothing unit.
- `proto_migrate.read_log_group(paths) -> list[dict]` returns one version-consistent snapshot of a whole group.
- `proto_migrate.migrate_linked_groups(groups, refs, ...) -> LinkedMigrationResult` migrates multiple explicit groups with declared cross-group order references as one all-or-nothing unit.
- `proto_migrate.read_linked_logs(groups) -> list[dict]` returns one version-consistent snapshot of every linked group.
- `proto_migrate.VERSIONS -> tuple[int, ...]` supported versions, ascending.
- `proto_migrate.CURRENT_VERSION -> int` the version `dumps` writes.

## Tests

    python3 -m unittest discover -s tests -t .

## Limits

Values must be JSON-compatible scalars, lists and objects.
No schema registry and no network lookup.
Unknown newer versions are rejected rather than guessed.
