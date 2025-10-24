# Configuration

Don’t be afraid to experiment—if a configuration is missing or invalid, the Index will fail fast, helping you catch issues early.

The index is configured using the `IndexConfiguration` class. All essential index properties are configurable through the builder. See the example below:

```java
IndexConfiguration<Integer, Integer> conf = IndexConfiguration
    .<Integer, Integer>builder()//
    .withKeyClass(Integer.class)//
    .withValueClass(Integer.class)//
    .withKeyTypeDescriptor(tdi) //
    .withValueTypeDescriptor(tdi) //
    .withMaxNumberOfKeysInSegment(4) //
    .withMaxNumberOfKeysInSegmentCache(10L) //
    .withMaxNumberOfKeysInSegmentCacheDuringFlushing(12L)//
    .withMaxNumberOfKeysInSegmentIndexPage(2) //
    .withMaxNumberOfKeysInCache(3) //
    .withBloomFilterIndexSizeInBytes(0) //
    .withBloomFilterNumberOfHashFunctions(4) //
    .withLogEnabled(false) //
    .withName("test_index") //
    .build();

Index<Integer, Integer> index = Index.<Integer, Integer>create(directory, conf);
```

Now let's look at particular parameters.

# Index Directory

Place where all data are stored. There are two already prepared types:

## In Memory

All data are stored in memory. It's created like this:

```java
Directory directory = new MemDirectory();
```

It's usefull for testing purposes.

## File system

Its main purpose is to store index data in the file system. Create a file-system-based directory like this:

```java
Directory directory = new FsDirectory(new File('my directory'));
```

# Properties of `IndexConfiguration` class

All properties are required and have the following meanings:

## Index related configuration

### Key class - `withKeyClass()`

A `Class` object that represents the type of keys used in the index. Only instances of this class may be inserted. While any Java class is technically supported, it's recommended to use simple, compact types for performance reasons. Predefined classes are:

* Integer
* Long
* String
* Byte

If a different class is used, the key type descriptor must be set using the `withKeyTypeDescriptor()` method from the builder. If you use a custom class, you must implement the `com.hestiastore.index.datatype.TypeDescriptor` interface to describe how the type is serialized and compared.

### Value class - `withValueClass()`

Required. Specifies the Java class used for values. The same rules that apply to the key class also apply to the value class.

### Index name - `withName()`

Required. Assigns a logical name to the index. This can be useful in diagnostics and logging.

### Key type descriptor - `withKeyTypeDescriptor()`

Type descriptor for the key class. Required for non-default types.

### Value type descriptor - `withValueTypeDescriptor()`

Type descriptor for the value class. Required for non-default types.

### Max number of keys in cache - `withMaxNumberOfKeysInCache()`

Sets the maximum number of key-value pairs allowed in the in-memory cache before flushing.

### Max number of segments in cache - `withMaxNumberOfSegmentsInCache()`

Limits the number of segments stored in memory. Useful for controlling memory usage.

### Thread safe - `withThreadSafe()`

Whether the index instance is safe for concurrent access by multiple threads. When it's set to `code` true than index will be synchronized between threads.

### Log enabled - `withLogEnabled()`

Enables or disables write-ahead logging. Currently it's experimental feature.

## Segment related configuration

### Max number of keys in segment - `withMaxNumberOfKeysInSegment()`

Sets the maximum number of keys allowed in a single segment. Exceeding this splits the segment.

### Max number of keys in segment cache - `withMaxNumberOfKeysInSegmentCache()`

Defines how many keys can be cached from a segment during regular operation.

### Max number of keys in segment cache during flushing - `withMaxNumberOfKeysInSegmentCacheDuringFlushing()`

Specifies the maximum number of keys that can be temporarily cached from a segment during flushing.

### Max number of keys in segment index page - `withMaxNumberOfKeysInSegmentIndexPage()`

Defines the number of keys in the index page for a segment. This impacts lookup efficiency.

## Bloom filter configuration

A Bloom filter is a probabilistic data structure that efficiently tests whether an element is part of a set. You can find a detailed explanation on [Wikipedia](https://en.wikipedia.org/wiki/Bloom_filter). In this context, each segment has its own Bloom filter.

To **disable** bloom filter completle set:

```java
 .withBloomFilterIndexSizeInBytes(0)
```

The settings for the Bloom filter can be adjusted using the following methods:

#### Bloom filter size - `withBloomFilterIndexSizeInBytes()`

Sets the size of the Bloom filter in bytes. A value of 0 disables the use of the Bloom filter.

#### Number of hash functions - `withBloomFilterNumberOfHashFunctions()`

Sets the number of hash functions used in the Bloom filter.

#### Probability of false positive - `withBloomFilterProbabilityOfFalsePositive()`

Sets the probability of false positives. When `get(someKey)` is called on a segment, the Bloom filter is checked to determine if the value is not in the segment. It can return `true`, indicating that the key **could be** in the segment. If the Bloom filter indicates the key is in the segment but it's not found, that's a false positive. The probability of this occurring is a value between 0 and 1.

Usually, it's not necessary to adjust the Bloom filter settings.

## Changing Index propertise

Some parameters can be redefined when the index is opened.

```java
Index<String, String> index = Index.<String, String>open(directory, conf);
```

At allows to pass `IndexConfiguration` object and this way change configuration parameters. Fllowing table shou parameters that can be changed.  

| Name                                        | Meaning                                              | Can be changed | Applies to            |
| ------------------------------------------- | ---------------------------------------------------- | -------------- | -------------------- |
| indexName                                   | Logical name of the index                            | 🟩             | index                |
| keyClass                                    | Key class                                            | 🟥             | index                |
| valueClass                                  | Value class                                          | 🟥             | index                |
| keyTypeDescriptor                           | Key class type descriptor                            | 🟥             | index                |
| valueTypeDescriptor                         | Value class type descriptor                          | 🟥             | index                |
| maxNumberOfKeysInSegmentIndexPage           | Maximum keys in segment index page                   | 🟥             | segment              |
| maxNumberOfKeysInSegmentCache               | Maximum number of keys in segment cache              | 🟩             | segment              |
| maxNumberOfKeysInSegmentCacheDuringFlushing | Maximum keys in cache during flushing                | 🟩             | segment              |
| maxNumberOfKeysInCache                      | Maximum keys in the index cache                      | 🟩             | index                |
| maxNumberOfKeysInSegment                    | Maximum keys in a segment                            | 🟥             | segment              |
| maxNumberOfSegmentsInCache                  | Maximum number of segments in cache                  | 🟩             | index                |
| bloomFilterNumberOfHashFunctions            | Bloom filter - number of hash functions used         | 🟥             | segment bloom filter |
| bloomFilterIndexSizeInBytes                 | Bloom filter - index size in bytes                   | 🟥             | segment bloom filter |
| bloomFilterProbabilityOfFalsePositive       | Bloom filter - probability of false positives        | 🟥             | segment bloom filter |
| diskIoBufferSize                            | Size of the disk I/O buffer                          | 🟩             | Disk IO              |
| threadSafe                                  | If index is thread-safe                              | 🟩             | index                |
| logEnabled                                  | If full logging is enabled                           | 🟩             | index                |

# Add custom data type

HestiaStore have to know how to work with new data type. So first is create implementatio of `com.hestiastore.index.datatype.TypeDescriptor`. Than during index creation set let index know about your implementation by `withKeyTypeDescriptor`. And it's done.
