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

## Tests

    python3 -m unittest discover -s tests -t .

## Limits

Values must be JSON-compatible scalars, lists and objects.
No schema registry and no network lookup.
Unknown newer versions are rejected rather than guessed.
