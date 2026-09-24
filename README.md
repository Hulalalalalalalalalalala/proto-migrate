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
- `proto_migrate.VERSIONS -> tuple[int, ...]` supported versions, ascending.
- `proto_migrate.CURRENT_VERSION -> int` the version `dumps` writes.

## Tests

    python3 -m unittest discover -s tests -t .

## Limits

Values must be JSON-compatible scalars, lists and objects.
No schema registry and no network lookup.
Unknown newer versions are rejected rather than guessed.
