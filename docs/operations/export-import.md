# Export & Import

HestiaStore export/import tooling provides an offline, logical data boundary
above the internal on-disk index layout.

Use it when you need to:

- create a portable backup artifact instead of copying raw index files
- migrate data to a newer HestiaStore version with changed on-disk behavior
- prepare data for datatype or configuration migration into a fresh index
- extract data into a human-readable stream for other systems

## MVP scope

The current CLI supports **offline** export and import only.

That means:

1. stop application writes
2. close the source index cleanly
3. run the export tool against the closed directory

Do not treat the current MVP as an online backup mechanism for actively written
indexes.

## Distribution

The export/import tooling is delivered as the standalone distribution artifacts:

```text
index-tools-<version>.zip
index-tools-<version>.zip.sha256
```

The main launcher inside the distribution is:

```bash
bin/hestia_index
```

## Installation

Operators should work with the released `index-tools-<version>.zip`
distribution, verify the accompanying checksum when your release process
requires it, then unpack it on the host where export/import runs.

Example installation:

```bash
sha256sum -c index-tools-<version>.zip.sha256
unzip index-tools-<version>.zip -d /opt/hestiastore/index-tools
/opt/hestiastore/index-tools/bin/hestia_index --help
```

Concrete example:

```bash
VERSION=1.2.3
RELEASE_DIR=/srv/releases/hestiastore
INSTALL_DIR=/opt/hestiastore/index-tools

cd "$RELEASE_DIR"
sha256sum -c "index-tools-${VERSION}.zip.sha256"
unzip -o "index-tools-${VERSION}.zip" -d "$INSTALL_DIR"
"$INSTALL_DIR/bin/hestia_index" --help
```

If you want the launcher available on your shell `PATH`, add:

```bash
export PATH="/opt/hestiastore/index-tools/bin:$PATH"
```

For custom datatype descriptors, custom `TextValueCodec` implementations, or
custom chunk filter providers, set:

```bash
export HST_EXTRA_CLASSPATH="/opt/hestia/extensions/custom-tools.jar"
```

The current MVP is intentionally a standalone operational CLI. It is separate
from the embedded application library and should be treated as an operations
artifact.

## Supported formats

### `bundle`

`bundle` is the default lossless HestiaStore-to-HestiaStore format.

It writes:

- `manifest.json`
- `source-config.json`
- `checksums.txt`
- `part-*.bin` or `part-*.bin.gz`

Use it for:

- portable restore into a fresh HestiaStore index
- upgrade staging between environments
- low-risk archival of current logical contents

### `jsonl`

`jsonl` writes one JSON object per logical entry plus the same manifest and
configuration metadata.

For built-in scalar types, the tool writes readable text values.
For values that cannot be rendered safely as text, it falls back to base64 of
the encoded descriptor payload.

Use it for:

- datatype migrations where text form is acceptable
- ETL into other systems
- audits and debugging
- minimizing fear of lock-in around opaque internal layouts

## Commands

### Export a portable bundle

```bash
hestia_index export \
  --source-index /srv/hestia/orders \
  --output /srv/backups/orders-2026-04-21 \
  --format bundle
```

Concrete example with explicit launcher path:

```bash
/opt/hestiastore/index-tools/bin/hestia_index export \
  --source-index /srv/hestia/indexes/orders \
  --output /srv/hestia/backups/orders-2026-04-21 \
  --format bundle
```

### Export readable JSONL

```bash
hestia_index export \
  --source-index /srv/hestia/orders \
  --output /srv/exports/orders-jsonl \
  --format jsonl
```

Concrete example:

```bash
/opt/hestiastore/index-tools/bin/hestia_index export \
  --source-index /srv/hestia/indexes/orders \
  --output /srv/hestia/exports/orders-jsonl-2026-04-21 \
  --format jsonl
```

### Export only part of a key range

```bash
hestia_index export \
  --source-index /srv/hestia/orders \
  --output /srv/exports/orders-partial \
  --format jsonl \
  --from-key 100000 \
  --to-key 199999 \
  --limit 10000
```

`from-key` and `to-key` are inclusive. Range export requires a comparable key
type plus a text codec that can parse the supplied key text.

### Verify an export

```bash
hestia_index verify-export \
  --input /srv/backups/orders-2026-04-21
```

Concrete example:

```bash
/opt/hestiastore/index-tools/bin/hestia_index verify-export \
  --input /srv/hestia/backups/orders-2026-04-21
```

This validates:

- manifest presence
- checksum integrity
- record counts
- JSONL syntax or bundle framing

For automation-friendly output:

```bash
hestia_index verify-export \
  --input /srv/backups/orders-2026-04-21 \
  --json
```

### Inspect metadata

```bash
hestia_index inspect-export \
  --input /srv/backups/orders-2026-04-21
```

Concrete example:

```bash
/opt/hestiastore/index-tools/bin/hestia_index inspect-export \
  --input /srv/hestia/backups/orders-2026-04-21 \
  --json
```

This is a fast metadata read by default. It does not scan all records or
checksums unless you ask for full verification.

For deep inspection with validation:

```bash
hestia_index inspect-export \
  --input /srv/backups/orders-2026-04-21 \
  --verify
```

For automation-friendly output:

```bash
hestia_index inspect-export \
  --input /srv/backups/orders-2026-04-21 \
  --json
```

### Import into a fresh index

```bash
hestia_index import \
  --input /srv/backups/orders-2026-04-21 \
  --target-index /srv/hestia-restored/orders
```

Concrete example:

```bash
/opt/hestiastore/index-tools/bin/hestia_index import \
  --input /srv/hestia/backups/orders-2026-04-21 \
  --target-index /srv/hestia-restored/indexes/orders
```

### Import and verify the target index

```bash
hestia_index import \
  --input /srv/backups/orders-2026-04-21 \
  --target-index /srv/hestia-restored/orders \
  --verify-after-import
```

This reopens the imported index, re-reads all logical entries, and compares the
record count plus a logical fingerprint against what was imported.

### Create a target configuration template

```bash
hestia_index init-target-config \
  --input /srv/exports/orders-jsonl \
  --output /srv/exports/orders-jsonl/target-config.json \
  --index-name orders-v2
```

Use this when you want an editable `target-config.json` as the starting point
for datatype or configuration migration.

## Migrating to a different target configuration

Each export writes `source-config.json`.

You can copy and edit that file, then use it as the target configuration for a
new import.

This is useful when you want to migrate:

- to a newer version with different preferred settings
- to a different datatype descriptor
- to a different logical index name

Example workflow:

1. export source data as JSONL
2. copy `source-config.json` to `target-config.json`
3. adjust the target value class and descriptor
4. import using the edited target config

Example:

```bash
hestia_index import \
  --input /srv/exports/orders-jsonl \
  --target-index /srv/hestia/orders-v2 \
  --target-config /srv/exports/orders-jsonl/target-config.json
```

This creates a **new** index. It does not mutate the original index in place.

## Moving data to other systems

When adopters are concerned about data lock-in, prefer `jsonl`.

The export directory contains:

- a readable JSONL data file
- the original HestiaStore configuration metadata

Concrete example:

```bash
/opt/hestiastore/index-tools/bin/hestia_index export \
  --source-index /srv/hestia/indexes/orders \
  --output /srv/integration/orders-jsonl \
  --format jsonl

head -n 3 /srv/integration/orders-jsonl/data.jsonl
```

## End-to-end operator examples

### Offline backup and restore drill

```bash
APP_SERVICE=hestia-orders
INDEX_TOOLS=/opt/hestiastore/index-tools/bin/hestia_index
SOURCE_INDEX=/srv/hestia/indexes/orders
EXPORT_DIR=/srv/hestia/backups/orders-2026-04-21
RESTORE_INDEX=/srv/hestia-restored/indexes/orders

sudo systemctl stop "$APP_SERVICE"
"$INDEX_TOOLS" export \
  --source-index "$SOURCE_INDEX" \
  --output "$EXPORT_DIR" \
  --format bundle
"$INDEX_TOOLS" verify-export --input "$EXPORT_DIR"
"$INDEX_TOOLS" import \
  --input "$EXPORT_DIR" \
  --target-index "$RESTORE_INDEX" \
  --verify-after-import
sudo systemctl start "$APP_SERVICE"
```

### Datatype migration via JSONL

```bash
INDEX_TOOLS=/opt/hestiastore/index-tools/bin/hestia_index
SOURCE_INDEX=/srv/hestia/indexes/orders
EXPORT_DIR=/srv/hestia/migrations/orders-v2-jsonl
TARGET_INDEX=/srv/hestia/indexes/orders-v2
TARGET_CONFIG="$EXPORT_DIR/target-config.json"

"$INDEX_TOOLS" export \
  --source-index "$SOURCE_INDEX" \
  --output "$EXPORT_DIR" \
  --format jsonl

"$INDEX_TOOLS" init-target-config \
  --input "$EXPORT_DIR" \
  --output "$TARGET_CONFIG" \
  --index-name orders-v2

# edit "$TARGET_CONFIG" here

"$INDEX_TOOLS" import \
  --input "$EXPORT_DIR" \
  --target-index "$TARGET_INDEX" \
  --target-config "$TARGET_CONFIG" \
  --verify-after-import
```
- checksums for integrity verification

That combination gives you:

- plain-text access for common built-in types
- a stable machine-readable manifest
- a fallback binary representation for non-text-friendly values

## Custom codecs and classpath extensions

The CLI discovers extension points through Java `ServiceLoader`.

For custom human-readable JSONL rendering and parsing, provide an implementation
of `org.hestiastore.indextools.TextValueCodec` and register it in:

```text
META-INF/services/org.hestiastore.indextools.TextValueCodec
```

For custom chunk filter providers, register:

```text
META-INF/services/org.hestiastore.index.chunkstore.ChunkFilterProvider
```

If your export/import flow uses custom datatype descriptors, those descriptor
classes must also be present on the launcher classpath. The simplest
operational path is to package the required classes into one jar and expose it
through `HST_EXTRA_CLASSPATH`.

## Relationship to Backup & Restore

Use filesystem-level backup when you want a cold copy of the exact index
directory as described in [Backup & Restore](backup-restore.md).

Use export/import when you want a portable logical backup or a migration
boundary that is less coupled to the internal storage layout.

## Limitations

- offline only in the current MVP
- the source index must not be receiving writes during export
- imports create a new target index and expect an empty target directory
- custom datatype descriptors must be available on the CLI classpath
- custom chunk filter providers must be available on the CLI classpath

## Help and discoverability

The CLI supports both top-level and command-level help:

- `hestia_index help`
- `hestia_index help export`
- `hestia_index export --help`
- `hestia_index export -h`

## Why this matters for upgrades

Export/import gives HestiaStore a practical compatibility escape hatch:

- raw storage layout can evolve more freely
- adopters keep a supported path for extracting logical data
- upgrades with potentially incompatible persistence changes remain manageable
- downstream systems can consume JSONL without understanding internal segment
  files

The repository also keeps compatibility fixtures for the v1 export format in
`index-tools/src/test/resources/compatibility/` so future releases can validate
`old export -> new import` behavior in CI.
