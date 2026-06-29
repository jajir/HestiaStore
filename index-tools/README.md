# HestiaStore Index Tools Distribution

This module packages offline export/import tooling for HestiaStore indexes as
a standalone zip distribution.

## Included commands

- `bin/hestia_index export`
- `bin/hestia_index import`
- `bin/hestia_index init-target-config`
- `bin/hestia_index inspect-export`
- `bin/hestia_index verify-export`

Help is available through:

- `hestia_index help`
- `hestia_index help <command>`
- `hestia_index <command> --help`
- `hestia_index <command> -h`

## Build

```bash
mvn -pl index-tools -am package
```

Distribution artifact:

- `index-tools/target/index-tools-<version>.zip`
- `index-tools/target/index-tools-<version>.zip.sha256`

Inspection defaults to metadata-only reads. Use `inspect-export --verify` when
you want the same deep checksum and record-count validation as `verify-export`.

Useful operational features:

- partial export via `--from-key`, `--to-key`, and `--limit`
- editable target config template generation via `init-target-config`
- machine-readable inspection and verification via `--json`
- post-import verification via `import --verify-after-import`

Classpath extensions:

- set `HST_EXTRA_CLASSPATH=/path/to/custom.jar` before launching the tool when
  you need custom datatype descriptors, custom `TextValueCodec`
  implementations, or custom chunk filter providers
