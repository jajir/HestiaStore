# 🚀 Getting Started

> **Note:** HestiaStore is a library, not a standalone application. It is designed to be integrated into a larger system to provide efficient storage and retrieval of large volumes of key-value pairs.

HestiaStore is a Java library distributed as a JAR file. It can be directly and easily used in any Java application. To get started, first refer to the installation guide and then explore usage examples.

## 💡 Use Cases

The library is suitable for scenarios requiring storage of large amounts of key-value pairs. HestiaStore is especially effective when:

* Store billions of key-value pairs
* Cloud storage is not an option
* You need to search key-value pairs efficiently
* Values must be persisted to disk

If all key-value pairs can be held in memory, it's usually better and faster to use an in-memory structure like `java.util.HashMap`. For smaller datasets, a traditional relational database may be more appropriate.
