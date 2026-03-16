# Troubleshooting

Use this page for the common integration failures seen during first adoption and
filesystem-backed operation.

## A `.lock` file prevents opening

Cause:

- another process still has the index open
- a previous process exited without cleaning up the lock

What to do:

1. Confirm no running process is still using the index directory.
2. If the process is gone, remove the stale `.lock` file.
3. Reopen the index.

## Consistency checks fail after a crash

Symptoms:

- `IndexException`
- unexpected read behavior after an unclean shutdown

What to do:

1. Open the index.
2. Run `checkAndRepairConsistency()`.
3. If repair fails, restore from backup.
4. If WAL is enabled, inspect the WAL directory with the WAL tooling before
   discarding evidence.

## `Attempt to insert the same key as previous`

This usually means a custom key type or comparator does not provide a stable,
strict ordering.

```text
Exception in thread "main" java.lang.IllegalArgumentException:
Attempt to insert the same key as previous
```

Check:

- your custom type ordering is deterministic
- `compare(a, b) == 0` only for keys that should be equal
- serialization and comparator behavior match

See [Data Types](../configuration/data-types.md#custom-data-types).

## Dependency resolution fails

For Maven:

```bash
mvn dependency:tree
```

For Gradle:

```bash
./gradlew dependencyInsight --dependency org.hestiastore:engine
```

Confirm `org.hestiastore:engine` is present and the version comes from
Maven Central.

## Java version mismatch

HestiaStore requires Java 17 or newer.

```bash
java -version
```

Align your local shell, IDE, CI runner, and container base image to the same
JDK line.

## Permission or path errors

- confirm the process has read and write permission on the index directory
- prefer absolute paths in services and containers
- verify mounted volumes are writable before opening the index

## Streaming does not show recent writes

- call `flush()` before starting the stream when recent writes must be visible
- avoid mutating the same index while consuming a stream if you need a stable
  view

## Still blocked

- Search existing tickets: <https://github.com/jajir/HestiaStore/issues?q=is%3Aissue>
- Open a new ticket: <https://github.com/jajir/HestiaStore/issues/new/choose>

Include:

- Java version
- HestiaStore version
- build tool and dependency declaration
- minimal reproduction
- relevant logs, stack traces, and OS details
