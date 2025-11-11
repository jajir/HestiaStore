# 🧩 Data types

HestiaStore supports a variety of data types for storing keys and values in a binary-efficient and consistent manner. Each data type is associated with a `TypeDescriptor`, which handles serialization, deserialization, comparison, and hashing logic.

Below is a list of the supported data types and their characteristics.

| Java Class           | TypeDescriptor Class              | Max Length (Bytes) | Notes |
|----------------------|-----------------------------------|---------------------|-------|
| `java.lang.Byte`     | `TypeDescriptorByte`              | 1                   | Two's complement representation |
| `java.lang.Integer`  | `TypeDescriptorInteger`           | 4                   | Big-endian encoding |
| `java.lang.Long`     | `TypeDescriptorLong`              | 8                   | Big-endian encoding |
| `java.lang.Float`    | `TypeDescriptorFloat`             | 4                   | IEEE 754 format |
| `java.lang.Double`   | `TypeDescriptorDouble`            | 8                   | IEEE 754 format |
| `java.lang.String`   | `TypeDescriptorShortString`       | 128                 | UTF-8 encoding, prefixed with 1-byte length, it's default string type descriptor |
| `java.lang.String`   | `TypeDescriptorString`            | 2 GB                | UTF-8 encoding, prefixed with 4-byte length |
| `org.hestiastore.index.datatype.ByteArray` | `TypeDescriptorByteArray`   | n                   | Raw bytes, length determined by actual data |
| `org.hestiastore.index.datatype.NullValue` | `TypeDescriptorNullValue`   | 0                   | Usefulll when value is not needed. Doesn't occupy any space. |
| `org.hestiastore.index.datatype.CompositeValue` | `TypeDescriptorCompositeValue`   | n                   | Represents multiple values.  |

## 🧰 Custom Data Types

HestiaStore allows advanced users to define custom `TypeDescriptor` implementations for handling specialized serialization strategies or complex types. Main usecases:

* Allows to store new data type.
* Introduce some specific encoding. it could lead to space saving.
* Limit data to some exact size

To create a new data type:

1. Implement the `TypeDescriptor<T>` interface. It's just a collection of simple interfaces which allows store data type to bytes and restore it from byte array
2. Optionallly register it using `org.hestiastore.index.sst.DataTypeDescriptorRegistry.addTypeDescriptor(Class, descriptor)`.

### 💡 Why Register Your Custom Type Descriptor

Registering your `TypeDescriptor` with `DataTypeDescriptorRegistry` is not mandatory, but it brings benefits:

* Simpler configuration defaults: When building an `IndexConfiguration` you can specify only `keyClass`/`valueClass`. The manager auto-fills `keyTypeDescriptor`/`valueTypeDescriptor` from the registry, so you don’t have to wire descriptors everywhere.
* Consistency and validation: A single registry reduces mismatches between classes and descriptors. The configuration manager can validate and prevent accidental overriding of fixed properties because the descriptor identity is explicit.
* Future-proofing: Sharing an index between services or running offline maintenance works seamlessly as long as your descriptor type is on the classpath.

Important: If you register using a `TypeDescriptor` instance, its class will be saved. That class must have a public no-args constructor (the system instantiates it reflectively). If this is not possible, you can register using the explicit class name overload `addTypeDescriptor(Class, String)`.

### 🧪 How to use new Data Type

During Index configuration new data type descriptor can by directly used:

```java
IndexConfiguration<Integer, Integer> conf = IndexConfiguration
    .<Integer, Integer>builder()//
    .withKeyClass(Integer.class)//
    .withValueClass(MySuperDataType.class)//
        ...
    .withValueTypeDescriptor(new TypeDescriptorMySuperDataType()) //
        ...
    .build();

Index<Integer, Integer> index = Index.<Integer, Integer>create(directory, conf);
```

## 📝 Notes

* All numeric types use big-endian byte order for consistent sorting and comparison.
* `ByteArray` is a wrapper class designed to support `equals()`, `hashCode()`, and lexicographic comparison. It can be used for binary blobs or hash digests.

## ⚖️ Equality, Hashing, and Sorting Contracts

When you define custom key types (or use low-level binary types), it is critical that equality, hashing, and ordering behave consistently. HestiaStore relies on these properties to deduplicate updates in memory, to merge data from multiple sources, and to safely write strictly-increasing keys to disk.

Why this matters

* In-memory caches coalesce multiple updates to the same key before flush. If the cache cannot recognize “the same key”, duplicate keys can slip through.
* During flushing and compaction, keys are iterated in sorted order and written by `SortedDataFileWriter`. That writer requires strictly increasing keys. Two adjacent keys that are equal under the comparator will cause a hard failure.
* Merging/lookup structures assume a total and stable order; a non-transitive or unstable comparator leads to undefined behavior.

Typical failure mode when the contract is broken

* Using raw `byte[]` as a key: `byte[]` uses reference equality and identity-based `hashCode()`. If your comparator compares by content (e.g., lexicographically), then two different `byte[]` instances with the same content are NOT equal to `HashMap`, but ARE equal to the comparator. The cache keeps both, sorts them, and the writer sees two adjacent equal keys and throws an exception like:
  * "Attempt to insert the same key as previous. Key(Base64)='…'"

Contracts to follow

* equals/hashCode consistency: If `a.equals(b)` is `true`, then `a.hashCode() == b.hashCode()` must also be `true`.
* Comparator total order: The comparator returned by your `TypeDescriptor#getComparator()` must be total (anti-symmetric, transitive, and consistent), and must define the uniqueness of keys.
* Comparator consistency with equality: If the system uses hash-based maps anywhere for keys of type `K`, ensure that `compare(a, b) == 0` implies `a.equals(b)`. This avoids having one part of the system see two objects as the same key while another part sees them as different. If you cannot satisfy this for a given JVM type (e.g., raw `byte[]`), wrap it in a type that does (e.g., `ByteArray`).
* Stable order vs. encoding: Keep the comparator consistent with the intended semantics of your keys and their encoding. For numbers encoded big-endian, natural numeric order is the correct comparator.

Practical guidance

* Do NOT use raw `byte[]` as keys. Use `org.hestiastore.index.datatype.ByteArray` or another wrapper that implements content-based `equals()`, `hashCode()`, and a lexicographic `compareTo`.
* For fixed-length binary keys, prefer a wrapper or a fixed-length `String` (`TypeDescriptorFixedLengthString`) if your domain allows it.
* Avoid lossy comparators (e.g., comparing only a prefix) unless that prefix truly defines key identity. Otherwise, distinct keys will be treated as equal under the comparator and collapse unintentionally.
* Ensure your comparator never depends on mutable or external state; it must be stable for the lifetime of the index.

How to self-check

* Property checks you can add to tests when introducing a new key type `K` and comparator `cmp`:
  * For many random pairs `(a, b)`: if `cmp.compare(a, b) == 0`, then assert `a.equals(b)` and `a.hashCode() == b.hashCode()`.
  * For many triples `(a, b, c)`: assert transitivity: if `cmp.compare(a, b) <= 0` and `cmp.compare(b, c) <= 0` then `cmp.compare(a, c) <= 0`.
  * Round-trip through caches: insert duplicates under the comparator, ensure only the latest value is kept, and iteration returns strictly increasing unique keys.

Example: byte[] vs. ByteArray

* Bad (will fail): using `byte[]` as keys with a content comparator. Two different arrays with the same content will be considered different by a hash-based map but equal by the comparator, leading to duplicate adjacent keys during write.
* Good: using `ByteArray` as keys. `ByteArray` implements content-based `equals()`, `hashCode()`, and lexicographic `compareTo`, so all parts of the system agree on key identity and ordering.

By following these contracts, you ensure that updates are correctly deduplicated, merges are deterministic, and on-disk files maintain the strict key ordering required for safe reads and compactions.
