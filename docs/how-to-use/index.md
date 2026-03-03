# 🚀 Getting Started

> Note: HestiaStore is a library, not a standalone application. It is designed to be integrated into a larger system to provide efficient storage and retrieval of large volumes of key‑value entries.

HestiaStore is a Java library distributed as a JAR. To get started quickly:

- [Install](install.md) — add the dependency via Maven/Gradle.
- [Quick Start](quick-start.md) — minimal in‑memory and filesystem examples.
- [Troubleshooting](troubleshooting.md) — common issues, .lock files, and how to get help.
- [WAL Operations](../operations/wal.md) — optional write-ahead logging is implemented and can be enabled per index.

## 💡 Use Cases

HestiaStore is especially effective when you need to:

- Store billions of key‑value entries on local disks
- Perform efficient point lookups with bounded memory
- Persist values to disk without external databases
- Avoid cloud storage or network‑attached stores

When not to use HestiaStore:

- If all key‑value entries fit in RAM, prefer an in‑memory map (e.g., `HashMap` or `ConcurrentHashMap`) for speed and simplicity.
- For small datasets with relational queries, a traditional RDBMS may be simpler to operate.
