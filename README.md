# proto-migrate

Versioned message codec for records written by older releases: readers decode any known version and migrate it forward to the current shape.

## Requirements

Python 3.11 or newer. Standard library only.

## Install

    python3 -m pip install -e .

## Run

    python3 -m proto_migrate --selftest

## Public interface

`proto_migrate.dumps(message) -> bytes` encodes the current version.
- `proto_migrate.loads(data) -> dict` decodes any known version.
- `proto_migrate.migrate(message, target_version) -> dict` converts a decoded message.
- `proto_migrate.VERSIONS -> tuple[int, ...]` supported versions, ascending.
- `proto_migrate.CURRENT_VERSION -> int` the version `dumps` writes.

## Crash-safe log migration

For an append-only JSONL log containing a mix of v1/v2/v3 records, the
whole file can be migrated to the current version in one pass:

    python3 -m proto_migrate migrate-log data.jsonl --mode strict
    python3 -m proto_migrate migrate-log data.jsonl --mode skip

The Python entry point is `proto_migrate.migrate_log(path, *, mode=...,
audit_stream=...)`; modes are `proto_migrate.LOG_STRICT` (default) and
`proto_migrate.LOG_SKIP`. It returns a `MigrationResult`
(`changed`, `total_lines`, `migrated_lines`, `skipped`, `audit`).

Properties:

- **Single pass, constant memory.** The file is streamed line by line
  (never loaded whole); output goes to segmented temporary files
  (`.<name>.migrate.<pid>/part-NNNNNN`, 8 MiB each by default), which
  are merged, fsynced, and only then atomically renamed over the
  original.
- **No half-migrated state.** Replacement is one `rename(2)`. At every
  instant a reader opening the path sees either the untouched original
  file or the complete migrated file — never a missing path and never a
  mix of old-version and new-version fields. A kill at any point, then a
  rerun, converges to one of those two outcomes.
- **Idempotent.** A file already encoded in the canonical current
  version is not rewritten at all (same inode, same mtime); the second
  run changes no bytes.
- **Bad records.** A corrupt line, JSON parse failure, missing/
  non-integer/unsupported `v`, schema violation, `NaN`/`Infinity`, or a
  numeric literal that overflows to `Infinity` (e.g. `1e999`) is a bad
  record:
  - `strict`: stop at the first bad line, leave the original
    byte-for-byte untouched, exit with status **3**
    (`BadRecordError`, a `ValueError` subclass, from Python).
  - `skip`: omit bad lines, migrate the rest, and write one audit entry
    per bad line to **stderr**: `<line number>:<first 32 raw bytes of
    the line>`.
  - An amount of `-0.0` is a valid record and is preserved verbatim.
- Defaults and dropped fields follow the existing `migrate` semantics.

### Concurrency and consistency trade-offs

- Whole-file migrators are serialised by an exclusive `flock` on a
  sidecar file (`<name>.jsonl.migrate.lock`).
- **Readers** get linearisable visibility: because the swap is a single
  atomic rename, any concurrent reader that opens the path sees the old
  file or the new file in full, never a partial result. Readers that
  already hold the old file descriptor keep reading the old inode to
  EOF, as usual after a rename.
- **Appenders** that write while the rewrite runs do not lose records.
  The scan follows file growth until the size is stable for one second
  and folds appended lines into the output before the rename; an
  appender holding a descriptor on the old inode across the rename is
  afterwards drained through a temporary hard link
  (`.<name>.migrate.old`), with a checkpoint marker
  (`.<name>.migrate.state`) making that drain crash-safe and
  exactly-once. Appenders that reopen the path after the rename write
  to the new file directly. Assumes the standard append discipline
  (single `write(2)` per line, e.g. `O_APPEND`) so individual records
  are not torn.
- Exit codes: `0` success, `2` usage or I/O error, `3` first bad record
  in strict mode.

## Tests

    python3 -m unittest discover -s tests -t .

The log-migration tests cover crash injection at every durability
point (followed by a rerun), records appended concurrently during the
rewrite, and byte-level idempotency of repeated runs; acceptance is
checked against the final on-disk file and the stderr audit list.

## Limits

Values must be JSON-compatible scalars, lists and objects.
No schema registry and no network lookup.
Unknown newer versions are rejected rather than guessed.
