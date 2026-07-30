# Data Types

HestiaStore supports a variety of data types for storing keys and values in a binary-efficient and consistent manner. Each data type is associated with a `TypeDescriptor`, which handles serialization, deserialization, comparison, and hashing logic.

Below is a list of the supported data types and their characteristics.

| Java Class           | TypeDescriptor Class              | Maximum Payload Bytes | Notes |
|----------------------|-----------------------------------|-----------------------|-------|
| `java.lang.Byte`     | `TypeDescriptorByte`              | 1                   | Two's complement representation |
| `java.lang.Integer`  | `TypeDescriptorInteger`           | 4                   | Big-endian encoding |
| `java.lang.Long`     | `TypeDescriptorLong`              | 8                   | Big-endian encoding |
| `java.lang.Float`    | `TypeDescriptorFloat`             | 4                   | IEEE 754 format |
| `java.lang.Double`   | `TypeDescriptorDouble`            | 8                   | IEEE 754 format |
| `java.lang.String`   | `TypeDescriptorShortString`       | 127                 | ISO-8859-1, one-byte length; default string descriptor |
| `java.lang.String`   | `TypeDescriptorString`            | JVM array limit     | ISO-8859-1, four-byte signed length |
| `java.lang.String`   | `TypeDescriptorTinyUtf8String`    | 255                 | Strict UTF-8, one-byte unsigned length |
| `java.lang.String`   | `TypeDescriptorUtf8String`        | 2,147,483,642       | Strict UTF-8, canonical unsigned LEB128 length |
| `org.hestiastore.index.datatype.ByteArray` | `TypeDescriptorByteArray`   | n                   | Raw bytes, length determined by actual data |
| `org.hestiastore.index.datatype.NullValue` | `TypeDescriptorNull`   | 0                   | Useful when a value is not needed; occupies no payload space |
| `org.hestiastore.index.datatype.CompositeValue` | `TypeDescriptorCompositeValue`   | n                   | Represents multiple values.  |

## UTF-8 String Descriptors

Both UTF-8 descriptors use standard UTF-8 and store the encoded payload byte
length. The limit does not count Java UTF-16 code units, Unicode code points,
or displayed grapheme clusters.

`TypeDescriptorTinyUtf8String` stores:

```text
[one-byte unsigned payload length][UTF-8 payload]
```

Its payload can contain 0 through 255 bytes. For example, it can hold 255 ASCII
characters or 63 four-byte emoji, but not 64 four-byte emoji.

`TypeDescriptorUtf8String` stores:

```text
[canonical unsigned LEB128 payload length][UTF-8 payload]
```

The prefix size is:

| Payload Length | Prefix Bytes |
|----------------|--------------|
| 0–127 | 1 |
| 128–16,383 | 2 |
| 16,384–2,097,151 | 3 |
| 2,097,152–268,435,455 | 4 |
| 268,435,456–2,147,483,642 | 5 |

The general descriptor's maximum preserves the `TypeWriter` contract that the
total prefix and payload byte count fits in a non-negative Java `int`. Actual
usable values can be lower because a payload must also fit in a JVM byte array
and available heap.

Both descriptors:

- reject unpaired Java UTF-16 surrogates during encoding
- reject malformed, truncated, overlong, or surrogate UTF-8 byte sequences
  during decoding
- preserve the original string without automatic Unicode normalization
- use Java `String.compareTo` ordering

Application character limits must account for variable-width encoding:

| Application Limit | Worst-Case UTF-8 Payload | Suitable Descriptor |
|-------------------|---------------------------|---------------------|
| 128 encoded bytes | 128 bytes | `TypeDescriptorTinyUtf8String` |
| 128 Unicode code points | 512 bytes | `TypeDescriptorUtf8String` |
| 64,000 encoded bytes | 64,000 bytes | `TypeDescriptorUtf8String` |
| 64,000 Unicode code points | 256,000 bytes | `TypeDescriptorUtf8String` |

Displayed characters can contain multiple code points, so a grapheme-count
limit alone does not provide a strict serialized byte limit.

### Configuration

Wire UTF-8 descriptors explicitly in the identity configuration:

```java
IndexConfiguration<String, String> configuration = IndexConfiguration
    .<String, String>builder()
    .identity(identity -> identity
        .name("localized-text")
        .keyClass(String.class)
        .valueClass(String.class)
        .keyTypeDescriptor(new TypeDescriptorTinyUtf8String())
        .valueTypeDescriptor(new TypeDescriptorUtf8String()))
    .build();
```

The default registry mapping for `String` remains
`TypeDescriptorShortString` for on-disk compatibility. Applications can use
either UTF-8 descriptor without changing that global default by configuring it
explicitly as shown above.

### Migration from ISO-8859-1

Do not replace `TypeDescriptorShortString` or `TypeDescriptorString` with a
UTF-8 descriptor when opening an existing index. The formats use different
payload encodings and different length framing. Create a new index with the
UTF-8 descriptor and migrate or rebuild the data.

The legacy ISO-8859-1 encoder replaces unsupported characters with `?`.
Choose a UTF-8 descriptor before accepting emoji or characters outside
ISO-8859-1 to prevent that data loss.

## Custom Data Types

HestiaStore allows advanced users to define custom `TypeDescriptor` implementations for handling specialized serialization strategies or complex types. Main usecases:

* Allows to store new data type.
* Introduce some specific encoding. it could lead to space saving.
* Limit data to some exact size

To create a new data type:

1. Implement the `TypeDescriptor<T>` interface. It groups encoder/decoder/reader/writer/comparator contracts for your type.
2. Optionallly register it using `org.hestiastore.index.segmentindex.configuration.DataTypeDescriptorRegistry.addTypeDescriptor(Class, descriptor)`.

### TypeEncoder Contract

`TypeEncoder` uses a single method:

```java
EncodedBytes encode(T value, byte[] reusableBuffer)
```

Notes:

* `reusableBuffer` is provided by the caller to reduce allocations.
* Encoder should reuse it when possible, otherwise allocate a larger buffer.
* Returned `EncodedBytes` contains:
  * `bytes`: buffer holding encoded payload
  * `length`: effective encoded byte count (can be smaller than `bytes.length`)

Example skeleton:

```java
public final class TypeDescriptorMyType implements TypeDescriptor<MyType> {
    @Override
    public TypeEncoder<MyType> getTypeEncoder() {
        return (value, reusableBuffer) -> {
            byte[] out = reusableBuffer;
            int required = /* compute max/required length */;
            if (out.length < required) {
                out = new byte[required];
            }
            int written = /* encode value to out */;
            return new EncodedBytes(out, written);
        };
    }
}
```

### Why Register Your Custom Type Descriptor

Registering your `TypeDescriptor` with `DataTypeDescriptorRegistry` is not mandatory, but it brings benefits:

* Simpler configuration defaults: When building an `IndexConfiguration` you can specify only `keyClass`/`valueClass`. The manager auto-fills `keyTypeDescriptor`/`valueTypeDescriptor` from the registry, so you don’t have to wire descriptors everywhere.
* Consistency and validation: A single registry reduces mismatches between classes and descriptors. The configuration manager can validate and prevent accidental overriding of fixed properties because the descriptor identity is explicit.
* Future-proofing: Sharing an index between services or running offline maintenance works seamlessly as long as your descriptor type is on the classpath.

Important: If you register using a `TypeDescriptor` instance, its class will be saved. That class must have a public no-args constructor (the system instantiates it reflectively). If this is not possible, you can register using the explicit class name overload `addTypeDescriptor(Class, String)`.

### How to use new Data Type

During `SegmentIndex` configuration, pass the descriptor through the grouped
identity section:

```java
IndexConfiguration<Integer, MySuperDataType> conf = IndexConfiguration
    .<Integer, MySuperDataType>builder()
    .identity(identity -> identity
        .name("custom-data")
        .keyClass(Integer.class)
        .valueClass(MySuperDataType.class)
        .valueTypeDescriptor(new TypeDescriptorMySuperDataType()))
    .build();

SegmentIndex<Integer, MySuperDataType> index = SegmentIndex.create(directory, conf);
```

## Notes

* All numeric types use big-endian byte order for consistent sorting and comparison.
* `ByteArray` is a wrapper class designed to support `equals()`, `hashCode()`, and lexicographic comparison. It can be used for binary blobs or hash digests.

## Equality, Hashing, and Sorting Contracts

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
