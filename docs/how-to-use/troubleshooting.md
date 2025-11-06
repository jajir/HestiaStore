# 🛠️ Troubleshooting

Common issues and quick fixes.

## 🔒 ".lock" File Prevents Opening

- Cause: Another process holds the index open, or a previous run did not close cleanly.
- Fix: Ensure the process using the index has terminated and closed the index. If safe, remove the stale .lock file after verifying no process uses the directory.

## 🧩 Consistency Errors (IndexException)

- Symptom: Consistency checks fail or reads behave unexpectedly after a crash.
- Fix: Run checkAndRepairConsistency(). If it still fails, restore from a backup.

## 📦 Dependency Resolution Fails

- Maven: run the command below and confirm org.hestiastore.index:core is present.

    mvn dependency:tree

- Gradle: run the command below for dependency insight.

    ./gradlew dependencyInsight --dependency org.hestiastore.index:core

- Also verify you used the latest version from Maven Central.

## ☕ Java Version Mismatch

- Ensure Java 11+ is used (Java 17 recommended). Check with java -version and align your IDE or CI JDK.

## 📁 Permission or Path Errors

- Ensure your process has read/write permissions to the target directory.
- Use absolute paths for clarity in services or containers.

## 🔄 Stale Streaming Results

- Call flush() before streaming if you require latest writes.
- Avoid put() or delete() while iterating a stream.

## ❓ Need More Help?

- Search existing tickets: <https://github.com/jajir/HestiaStore/issues?q=is%3Aissue>
- Open a new ticket: <https://github.com/jajir/HestiaStore/issues/new/choose>

When opening a ticket, please include:

- Your Java version (run `java -version`)
- HestiaStore version and build tool (Maven/Gradle)
- Minimal code snippet or steps to reproduce
- Relevant logs/stack traces and OS info
