# proto-migrate

Versioned message codec for records written by older releases: readers decode any known version and migrate it forward to the current shape.

## Requirements

Python 3.11 or newer. Standard library only.

## Install

    python3 -m pip install -e .

## Run

    python3 -m proto_migrate --selftest

## Crash-safe log migration

`migrate-log` rewrites a whole append-only JSONL log (mixed v1/v2/v3
records, one object per line) to the current version in one streaming
pass, without loading the file into memory:

    python3 -m proto_migrate migrate-log app.log            # strict (default)
    python3 -m proto_migrate migrate-log app.log --skip

Python entry point:

```python
from proto_migrate import migrate_log_file

result = migrate_log_file("app.log", on_bad="strict")  # or on_bad="skip"
# result.records_migrated / records_skipped / records_salvaged / replaced
```

Options: `--strict` (default) / `--skip` choose bad-record handling;
`--segment-size BYTES` (default 16 MiB) rotates the temp segments;
`--quiesce-ms MS` (default 50 ms) is how long EOF must hold before the
input is considered complete while appenders may still be writing.

### Crash safety and resume

Output goes to segmented temp files next to the target
(`<file>.migrate-tmp/`), each segment is `fsync`ed, and after every
segment a checkpoint (source inode, consumed source offset, record
counts, segment inventory) is appended to
`<file>.migrate-tmp/checkpoint` and `fsync`ed. The segments are
assembled into one final temp file, that file is `fsync`ed, and only
then is it atomically `os.replace`d over the original, followed by an
fsync of the directory.

If the process is killed at any point and rerun, the run resumes from
the newest checkpoint that still validates: already-migrated records
are not re-scanned and not re-written, and the resumed result is
byte-for-byte identical to a single uninterrupted run. A torn
checkpoint tail, a corrupt checkpoint line, a missing segment file, or
an over-long assembled file (killed mid tail-drain) is detected and
rolled back to the newest checkpoint whose referenced files are all
intact — all the way back to a fresh start if none validates. A source
file whose identity changed (dev/inode) invalidates every checkpoint.
The target path always names either the original file byte-for-byte or
the complete migrated file — never a half-migrated file, and never a
mix of old- and new-version records in one view.

The atomic rename is the commit point: durability or cleanup failures
after it (directory fsync, appender convergence, temp-dir removal) are
reported as warnings on stderr, never as a failed migration. Leftover
temp files from a killed run are reused or swept under an advisory lock
(`<file>.migrate.lock`) before the next run.

### Concurrent readers and appenders

- **Readers**: a reader opening the path at any instant sees the whole
  original file or the whole migrated file, never a partial view
  (`os.replace` is atomic). A reader holding an already-open file
  descriptor keeps reading the old inode (standard rename semantics).
- **Appenders must write whole records in one `O_APPEND` write.**
  Records appended during the scan are drained into the output. After
  the rename, a bounded convergence phase drains writers that still
  hold the old descriptor (via the unlinked old inode) and rewrites
  old-format records that a writer opening the path after the rename
  landed on the new inode, using one more atomic replacement. A writer
  that appends continuously past the convergence deadline (at least
  1 s) is not blocked on forever; rerun the command — it is idempotent
  and catches up.

### Bad records

Corrupt lines, JSON parse failures, missing/non-integer/unsupported `v`,
illegal fields, and `NaN`/`Infinity` amounts (including numeric
literals such as `1e999` that overflow to infinity) are rejected with
`ValueError`. An amount of `-0.0` is preserved verbatim.

- `--strict`: stop at the first bad line, exit with code **3**, and
  leave the original file untouched.
- `--skip`: skip bad lines, keep migrating, and write one audit entry
  per skipped line to **stderr** as `<lineno>:<first 32 raw bytes>`.

Exit codes: `0` success, `1` other I/O error, `2` usage error,
`3` bad record in strict mode.

### Idempotency

If every record is already at the current version in the canonical
encoding and there is nothing to skip, the file is left byte-for-byte
untouched (no rename, no mtime change). A second run rewrites no bytes.

## Tests

    python3 -m unittest discover -s tests -t .

The migration tests cover crash injection at each durability
checkpoint (segment fsync, assembly, rename), checkpointed resume after
real SIGKILLs, checkpoint corruption and missing-segment rollback,
records appended concurrently during the rewrite, post-rename appends,
three-party concurrency (migrator + appender + reader) across a kill
and resume, atomic reader views, post-commit durability-failure
classification, and idempotent reruns.

## Public interface

`proto_migrate.dumps(message) -> bytes` encodes the current version.
- `proto_migrate.loads(data) -> dict` decodes any known version.
- `proto_migrate.migrate(message, target_version) -> dict` converts a decoded message.
- `proto_migrate.VERSIONS -> tuple[int, ...]` supported versions, ascending.
- `proto_migrate.CURRENT_VERSION -> int` the version `dumps` writes.

## Tests

    python3 -m unittest discover -s tests -t .

## Limits

Values must be JSON-compatible scalars, lists and objects.
No schema registry and no network lookup.
Unknown newer versions are rejected rather than guessed.
