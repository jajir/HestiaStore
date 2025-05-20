# Configuration

The index is configured using the builder pattern. All important index properties should be configurable through the builder. See following example:

```java
final Index<Integer, String> index = Index.<Integer, String>builder()
        .withDirectory(directory)
        .withKeyClass(Integer.class)
        .withValueClass(String.class)
        .build();
```

All property are required with following meaning:

### Directory

Place where all data are stored. There are two already prepared types:

#### In Memory

All data are stored in memory. It's created like this:

```java
Directory directory = new MemDirectory();
```

#### File system

In this case all data are stored in file system. It's main purpose of index to store data at directory:

```java
Directory directory = new FsDirectory(new File('my directory'));
```

### Key class

key class can be set with method `withKeyClass()`. A class object representing the type of keys is required. Only instances of this class can be inserted into the index. While any class can be used, it is advisable to choose a reasonably small class for efficiency. Predefined classes include:

* Integer
* Long
* String
* Byte

If a different class is used, the key type descriptor must be set using the `withKeyTypeDescriptor()` method from the builder. When new class should be used than `com.coroptis.index.datatype.TypeDescriptor` interface should be implemented.

### Value class

Similar to key class, it used method `withValueClass()` and `withValueTypeDescriptor()`.

## Bloom filter configuration

A Bloom filter is a probabilistic data structure that efficiently tests whether an element is part of a set. You can find a detailed explanation on [Wikipedia](https://en.wikipedia.org/wiki/Bloom_filter). In this context, each segment has its own Bloom filter. The settings for the Bloom filter can be adjusted using the following methods:

* `withBloomFilterIndexSizeInBytes()` - Sets the size of the Bloom filter in bytes. A value of 0 disables the use of the Bloom filter.
* `withBloomFilterNumberOfHashFunctions()` -  Sets the number of hash functions used in the Bloom filter.
* `withBloomFilterProbabilityOfFalsePositive()` -  Sets the probability of false positives. When `get(someKey)` is called on a segment, the Bloom filter is checked to determine if the value is not in the segment. It can return `true`, indicating that the key **could be** in the segment. If the Bloom filter indicates the key is in the segment but it's not found, that's a false positive. The probability of this occurring is a value between 0 and 1.
* `withMaxNumberOfKeysInSegment()` - Sets the maximum number of keys in a segment. When this limit is exceeded, the segment is split into two.

Usually, it's not necessary to adjust the Bloom filter settings.

## Examples

more comples example could look like:

```java
Index<Integer, Integer> index = Index.<Integer, Integer>builder()
    .withDirectory(directory)
    .withKeyClass(Integer.class)
    .withValueClass(Integer.class)
    .withKeyTypeDescriptor(tdi)
    .withValueTypeDescriptor(tdi)
    .withCustomConf()
    .withMaxNumberOfKeysInSegment(4)
    .withMaxNumberOfKeysInSegmentCache(10000)
    .withMaxNumberOfKeysInSegmentIndexPage(1000)
    .withMaxNumberOfKeysInCache(2)
    .withBloomFilterIndexSizeInBytes(1000)
    .withBloomFilterNumberOfHashFunctions(4)
    .withUseFullLog(false)
    .build();
```

#### Index Library – Design Requirements (User-Friendly Version)

1. **User clarity**  
   * The public API must be self-explanatory.  
   * Constructors, builders, and method names should read like plain English.

2. **JUnit friendliness**  
   * All stateful components must be injectable or mockable.  
   * Constructors should avoid side effects (I/O, threads) so unit tests can instantiate quickly.

3. **Existence check**  
   * Provide an explicit `Index.exists(Path dir)` (or similar) that answers *“Is there an index here?”* without opening it.

4. **Separate create vs. open**  
   * Creation and opening must be two distinct operations (e.g., `create()` vs. `open()`), never bundled in one call.

5. **Parameter integrity**  
   * Persisted metadata (key/value classes, segment sizes, etc.) is immutable once the index is created.  
   * Attempting to reopen with conflicting settings must throw a clear exception.

6. **Type safety**  
   * The API is fully generic (`Index<K,V>`).  
   * Only matching key/value types compile; no unchecked casts are exposed to users.

7. **Runtime tuning**  
   * Non-persistent knobs (e.g., cache sizes, buffer lengths) can be adjusted at runtime through dedicated setters or a `RuntimeConfig` object, without recreating the index.
