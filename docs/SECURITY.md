# HestiaStore Security

Security and quality are important considerations in the HestiaStore project. While HestiaStore is a library (not a network-exposed service), several tools are in place to monitor and improve code and dependency safety.

## Dependency Scanning

HestiaStore uses the [OWASP Dependency-Check](https://owasp.org/www-project-dependency-check/) Maven plugin to automatically scan project dependencies for known vulnerabilities. The scan is performed during the Maven `verify` phase. This helps detect issues in third-party libraries such as outdated or vulnerable versions of common libraries.

The OWASP dependency report is also included in the Maven Site documentation.

## Data Storage Security

HestiaStore stores data in the local file system or memory, depending on the
`Directory` implementation (for example `FsDirectory` or `MemDirectory`). It
does not provide a remote storage backend, and it does not manage keys or
enable encryption automatically. Integrators that need encrypted payloads can
wire the provider-backed `ChunkFilterAesGcmEncrypt` and
`ChunkFilterAesGcmDecrypt` filters with application-managed keys.

## Static Code Analysis

HestiaStore uses the following tools to enforce code quality and detect potential bugs:

- **PMD**: Checks for common coding errors, best practices violations, and potential bugs.
- **SpotBugs** (formerly FindBugs): Performs bytecode-level bug detection for possible concurrency issues, null pointer dereferences, etc.

Both reports are available through the Maven Site (`mvn site`).

## Testing and Coverage

The project includes a comprehensive suite of unit tests. Test coverage is measured using **JaCoCo**, and the coverage report is also published as part of the Maven Site.

```bash
mvn clean verify site
```

This will generate the full set of reports under `target/site/`.

## Threat Model

HestiaStore is designed to run as a component within a trusted local application. It does not expose network interfaces or provide internal access control mechanisms. As such, it assumes that:

- The host operating environment is trusted.
- Filesystem access is managed by the application or OS.
- Inputs to the library are trusted or validated upstream.

### Known Risks

| Threat                      | Mitigated? | Notes |
|----------------------------|------------|-------|
| Malicious input data       | ❌         | No input sanitization is performed |
| Unauthorized file access   | ❌         | No access control; relies on OS permissions |
| File corruption            | ⚠️         | Optional WAL (disabled by default) can replay and truncate invalid tail; non-WAL files rely on atomic renames and consistency checks |
| Memory data leakage        | ❌         | JVM memory is not encrypted or zeroed |
| SegmentIndex inconsistency        | ⚠️         | Recovery possible using `checkAndRepairConsistency()` |

## Trust Boundaries

HestiaStore does not define security boundaries within its API. Instead, it assumes that:

- The file system used by `FsDirectory` is controlled by the same principal as the application.
- Memory content is considered volatile and not protected against memory inspection.
- The user is responsible for isolating the library appropriately in containerized or multi-tenant environments.

## Data Integrity

HestiaStore provides limited protections:

- Optional Write-Ahead Logging (WAL) with CRC-protected records, recovery replay, and invalid-tail handling when enabled.
- Manual compaction and `checkAndRepairConsistency()` assist in recovery from logical inconsistencies.
- No global cryptographic MAC/signature layer is enabled by default. Optional
  AES-GCM chunk filters can provide authenticated encryption for chunk payloads
  when explicitly configured.

## Encryption

HestiaStore does not implement:

- Automatic key management or KMS integration
- Encryption in memory
- Encryption by default for segment files

Users requiring data confidentiality should either configure the provider-backed
AES-GCM chunk filters with application-managed keys or enable full-disk
encryption and isolate the storage backend appropriately.

## Denial of Service Considerations

While HestiaStore is efficient, certain usage patterns may degrade system performance:

- Inserting excessive data without flushing may exhaust memory.
- Large segment files may incur slow read or compaction times.
- Thread-safe operations may incur additional locking overhead under heavy concurrency.

## Security Responsibilities of Integrators

Users embedding HestiaStore must take responsibility for:

- Validating inputs
- Managing access to the directory path
- Applying memory and disk usage quotas externally
- Protecting against unauthorized runtime access

## Future Work

Planned or considered improvements include:

- Built-in key management and turnkey encrypted segment configuration
- Checksumming of stored values
- Sandboxed key/value type descriptors

## Summary

- ✅ Vulnerability scanning via OWASP Dependency Check
- ✅ Static analysis via PMD and SpotBugs
- ✅ Unit tests with coverage reporting via JaCoCo
- ⚠️ Payload encryption is opt-in and requires application-managed key wiring
- ✅ Basic threat model documented
- ✅ Optional WAL-based local crash recovery is available
- ⚠️ Assumes trusted host environment unless integrators add their own access
  control and optional payload encryption
- 🚧 Future improvements under consideration (checksums, encryption)

If you encounter any problems, discover vulnerabilities, or have questions, please report them by opening an [issue in the project's GitHub repository](https://github.com/jajir/HestiaStore/issues).
