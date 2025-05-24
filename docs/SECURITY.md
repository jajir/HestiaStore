# HestiaStore Security

Security and quality are important considerations in the HestiaStore project. While HestiaStore is a library (not a network-exposed service), several tools are in place to monitor and improve code and dependency safety.

## Dependency Scanning

HestiaStore uses the [OWASP Dependency-Check](https://owasp.org/www-project-dependency-check/) Maven plugin to automatically scan project dependencies for known vulnerabilities. The scan is performed during the Maven `verify` phase. This helps detect issues in third-party libraries such as outdated or vulnerable versions of common libraries.

The OWASP dependency report is also included in the Maven Site documentation.

## Data Storage Security

Currently, HestiaStore does **not** support a persistent, remote or encrypted storage backend. All data is stored in the local file system or memory, depending on the `Directory` implementation (e.g. `FsDirectory` or `MemDirectory`). Support for more advanced persistent stores with security features like encryption may be added in the future.

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
| File corruption            | 🚫         | Partial protection through optional WAL |
| Memory data leakage        | ❌         | JVM memory is not encrypted or zeroed |
| Index inconsistency        | ⚠️         | Recovery possible using `checkAndRepairConsistency()` |

## Trust Boundaries

HestiaStore does not define security boundaries within its API. Instead, it assumes that:

- The file system used by `FsDirectory` is controlled by the same principal as the application.
- Memory content is considered volatile and not protected against memory inspection.
- The user is responsible for isolating the library appropriately in containerized or multi-tenant environments.

## Data Integrity

HestiaStore provides limited protections:

- Optional Write-Ahead Logging (WAL) ensures durability between flushes.
- Manual compaction and `checkAndRepairConsistency()` assist in recovery from logical inconsistencies.
- No built-in checksums or MACs are currently used.

## Encryption

HestiaStore does not implement:

- Encryption at rest
- Encryption in memory
- Encrypted WAL or segment files

Users requiring data confidentiality should enable full-disk encryption or isolate the storage backend appropriately.

## Denial of Service Considerations

While HestiaStore is efficient, certain usage patterns may degrade system performance:

- Inserting excessive data without flushing may exhaust memory.
- Large segment files may incur slow read or compaction times.
- `withThreadSafe(true)` may incur additional locking overhead under heavy concurrency.

## Security Responsibilities of Integrators

Users embedding HestiaStore must take responsibility for:

- Validating inputs
- Managing access to the directory path
- Applying memory and disk usage quotas externally
- Protecting against unauthorized runtime access

## Future Work

Planned or considered improvements include:

- Optional encryption of segment data
- Checksumming of stored values
- Sandboxed key/value type descriptors

## Summary

- ✅ Vulnerability scanning via OWASP Dependency Check
- ✅ Static analysis via PMD and SpotBugs
- ✅ Unit tests with coverage reporting via JaCoCo
- ⏳ Persistent encrypted storage is not yet supported
- ✅ Basic threat model documented
- ⚠️ Assumes trusted host environment (no access control or encryption)
- 🚧 Future improvements under consideration (checksums, encryption)
