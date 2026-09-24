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

### The commit marker (`<file>.committed`)

- **Purpose:** it gates the consistent-read switch.  The marker is
  published and fsynced *before* the atomic rename, so `read_log` (and
  `read_log_group`) can tell whether the path still serves
  pre-migration content (no marker: decode as stored) or post-migration
  content (marker present: normalize every record to the current
  version).  This is what keeps old and new field shapes from mixing
  inside one snapshot, even when a writer reopens the path and appends
  old-format records after wrap-up.
- **Location:** a sibling of each migrated log file, named
  `<file>.committed` (one per member for a group migration).  It is
  written via a temporary file (`<file>.committed.tmp`) plus an atomic
  rename and a directory fsync.
- **Cleanup timing:** never removed after a successful run — it is
  kept so later reads keep serving normalized views and reruns stay
  idempotent.  Only a stale `<file>.committed.tmp` left by a crash
  mid-publication is removed, at the start of the next run.  Delete it
  by hand only if you intentionally want pre-migration read semantics
  back.

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

## Group migration: all-or-nothing across a set of logs

`migrate-logs` migrates an explicitly listed set of log files as **one
atomic group**: either every member ends at the current version, or
every member stays exactly as it was — the persistent state never shows
a partially migrated group.  Each member may be appended to by a
different process while the migration runs:

    python3 -m proto_migrate migrate-logs a.log b.log c.log            # strict (default)
    python3 -m proto_migrate migrate-logs a.log b.log c.log --skip

Python entry points:

```python
from proto_migrate import migrate_log_group, read_log_group

result = migrate_log_group(["a.log", "b.log"], on_bad="strict")
# result.results: one MigrationResult per member, in manifest order
# result.records_migrated / records_skipped / records_salvaged / replaced
#   count only records newly migrated during THIS run
# result.post_commit_error is a warning string, never a failure

views = read_log_group(["a.log", "b.log"])   # one consistent group snapshot
# views[i] is the decoded record list of paths[i]
```

Options are the same as for `migrate-log`.  The manifest must be a
sequence of paths: a non-sequence raises `TypeError`, an empty manifest
or duplicate paths raise `ValueError` (CLI: usage error, exit `2`), and
a missing or unreadable member raises `FileNotFoundError` from both
entry points (CLI: I/O error, exit `1`).  Exit codes are the same as
for `migrate-log`: `0` success, `1` I/O error, `2` usage error, `3` bad
record in strict mode.

### How the group commit works

1. **Prepare (per member, resumable).**  Every member is migrated with
   the single-file machinery: size-bounded segment files and a durable
   checkpoint log in `<file>.migrate-tmp/`, then a final drain and a
   durable `prepared` record.  Up to this point **no original file has
   been modified** and the whole group can roll back without a trace —
   any member failing here (I/O error, or a bad record in strict mode)
   aborts the group and sweeps every member's temporary artifacts.
2. **Commit phase 1: the group decision.**  Once ALL members are
   prepared, a group commit record is written to
   `<first member>.migrate-group/commit` (member list plus source and
   output inode of each member) and fsynced.  This record is the atomic
   decision of the two-phase protocol.
3. **Commit phase 2: the unified renames.**  For each member in turn
   the commit marker is published, the assembled output is atomically
   `os.replace`d over the original, the directory is fsynced, and a
   `done` line is appended to the commit record.

A process killed before the commit record is durable has changed no
original file; the rerun resumes every member from its checkpoints —
fully prepared members are neither rescanned nor rewritten — and
finishes byte-for-byte identical to one uninterrupted run.  A process
killed after the commit record is durable is finished by the rerun
**from the record**: remaining renames are completed (a rename that
already landed is detected by inode and only its `done` line is
rewritten), never rolled back.  After the last rename, each member
converges with racing appenders exactly like the single-file
migration, and a durability or cleanup failure (directory fsync, temp
sweep) never turns the committed group into a failure: the run exits
`0` and reports the fault on stderr as a `warning:`.

### Consistent group snapshots

`read_log_group(paths)` reads the whole group in one call and returns a
list parallel to `paths`.  Old and new field shapes never mix inside
the returned view — neither within a member nor between members: while
no member's commit marker exists every member is decoded exactly as
stored; as soon as any member's marker exists (the group's commit
sequence has begun or finished) every member is normalized to the
current version.  This holds even while a writer reopens a path and
appends old-format records after wrap-up.  Each member's snapshot pins
one inode, and only newline-terminated lines are included, so a record
caught mid-append never appears.

### Bad records in a group

The bad-record rules are identical to the single-file migration
(corrupt lines, missing/non-integer/unsupported `v`, illegal fields,
`NaN`/`Infinity` or overflowing amounts; `-0.0` is preserved):

- `--strict`: stop at the group's first bad line, raise `ValueError`
  (CLI exit `3`), and leave **every** member untouched.
- `--skip`: skip bad lines, keep migrating, and write one audit entry
  per skipped line to **stderr** as `<file>:<lineno>:<first 32 bytes>`.

## Tests

    python3 -m unittest discover -s tests -t .

The migration tests cover three-way concurrency (migrator + appender +
reader), resume from checkpoints, checkpoint corruption / missing
segment / torn-tail rollback, real `SIGKILL` crash injection and
recovery, post-commit fault classification, and idempotent reruns.  The
group tests cover all-or-nothing rollback, the recoverable two-phase
rename protocol (kill before/after the group commit record), resume
without rescanning prepared members, consistent group snapshots under
concurrent appends, manifest validation, and CLI exit codes.

## Public interface

`proto_migrate.dumps(message) -> bytes` encodes the current version.
- `proto_migrate.loads(data) -> dict` decodes any known version.
- `proto_migrate.migrate(message, target_version) -> dict` converts a decoded message.
- `proto_migrate.migrate_log_file(path, ...) -> MigrationResult` runs the online, resumable in-place log migration.
- `proto_migrate.migrate_log_group(paths, ...) -> GroupMigrationResult` migrates a set of logs as one all-or-nothing group.
- `proto_migrate.read_log(path) -> list[dict]` returns one version-consistent snapshot of a migrating log.
- `proto_migrate.read_log_group(paths) -> list[list[dict]]` returns one version-consistent snapshot per member, aligned with `paths`.
- `proto_migrate.VERSIONS -> tuple[int, ...]` supported versions, ascending.
- `proto_migrate.CURRENT_VERSION -> int` the version `dumps` writes.

## Limits

Values must be JSON-compatible scalars, lists and objects.
No schema registry and no network lookup.
Unknown newer versions are rejected rather than guessed.
