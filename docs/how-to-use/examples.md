# 📚 Examples of HestiaStore Usage

## 👋 Hello World Example

```java
import com.hestiastore.index.Index;
import com.hestiastore.index.IndexFactory;

public class Example {
  public static void main(String[] args) {
        // Create an in-memory file system abstraction
        final Directory directory = new MemDirectory();

        // Prepare index configuration
        final IndexConfiguration<String, String> conf = IndexConfiguration
                .<String, String>builder()//
                .withKeyClass(String.class)//
                .withValueClass(String.class)//
                .withName("test_index") //
                .build();

        // create new index
        Index<String, String> index = Index.<String, String>create(directory,
                conf);

        // Perform basic operations with the index
        index.put("Hello", "World");

        String value = index.get("Hello");
        System.out.println("Value for 'Hello': " + value);

        index.close();
  }
}
```

This creates a simple in-memory index and stores a key-value entry.

When you have first example you can dive into [more advanced configuration](../configuration/index.md). There are explained details about `Directory` object and using custom Key/Value classes

## 💾 Using HestiaStore with the File System

This is the most common scenario for storing data on persistent storage. Creating an index in a given directory can be done as follows:

```java
Directory directory = System.getProperty(new File("./index-directory"));

// Prepare index configuration
final IndexConfiguration<String, String> conf = IndexConfiguration
        .<String, String>builder()//
        .withKeyClass(String.class)//
        .withValueClass(String.class)//
        .withName("test_index") //
        .build();

// create new index
Index<String, String> index = Index.<String, String>create(directory,
        conf);

```

This immediately creates the initial index files and makes it ready to use.

> **Note** When the index starts working with a directory, it locks it with a `.lock` file. When the index is closed, the `.lock` file is removed. This prevents other applications from simultaneously modifying the index data.


## 📂 Opening an Existing Index

Please note that Index uses separate methods for creating an index and for opening an already existing index. To open an already existing index, use:

```java
IndexConfiguration<String, String> conf = IndexConfiguration
        .<String, String>builder()//
        .withKeyClass(String.class)//
        .withValueClass(String.class)//
        .withName("test_index") //
        .build();

Index<String, String> index = Index.<String, String>open(directory, conf);
```

## ✍️ Data Manipulation

There are two methods `put` and `get` using them is straightforward:

```java
index.put("Hello", "World");

String value = index.get("Hello");
```

Stored values are immediately available. Command ordering could be random.

## 📈 Sequential Data Reading

Reading from index could be done like this:

```java
index.getStream(null).forEach(entry -> {

  // Do what have to be done
    System.out.println("Entry: " + entry);
    
});
```

Data are returned in ascending ordering. This ordering can't be changed. Index stores data in segments. In some cases could be usefull to sequentially read just some segments. Segment could be selected by object `SegmentWindow`

```java
SegmentWindow window = SegmentWindow.of(1000, 10);

index.getStream(window).forEach(entry -> {
    System.out.println("Entry: " + entry);
});
```

## 🧹 Data Maintenance

In some cases could be useful to perform maintenance with data. There are following operations with `Index`:

- `flush()` It flush all data from memory to disk to ensure that all data is safely stored. It make sure that data are stored. Could be called before index iterating and when user want to be sure, that all data are stored.
- `checkAndRepairConsistency()` It verify that meta data about data in index are consistent. Some problems coudl repair. When index is beyond repair it fails.
- `compact();` Goes through all segments add compact main segment data with temporal files. It can save disk space.

## ⚠️ Limitations

### 🌀 Stale Results from `index.getStream()`

Data from `index.getStream()` method could be stale or invalid. It's corner case when next readed key value entry is changed. Index data streaming is splited internally into steps `hasNextElement()` and `getNextElement()`. Following example will show why it's no possible to use index cache:

```java
index.hasNextElement(); // --> true
```

Now next element has to be known to be sure that exists. Let's suppose that in index is just one element `<k1,v1>`.

```java
index.delete("k1");
index.nextElement(); // --> fail
```

last operation will fail because there is not possible to find next element because `<k1,v1>` was deleted. To prevent this problem index cache is not used during index streaming. If all index content should be streamed than before streaming should be `compact()` method and during streaming data shouldn't be changed.

To be sure that all data is read than before reading perform `Index.flush()` and during iterating avoid using of `Index.put()` and `Index.delete()` operations.

### 🔒 Thread Safety

> **Note:** Index is not thread-safe by default. Use `.withThreadSafe(true)` in the configuration to enable thread safety.

## 🧨 Exception Handling

Here are exceptions that could be throws from HestiaStore:

- `NullPointerException` -  When something fails really badly. For example when disk reading fails or when user delete part of configuration file.
- `IndexException` - Usually indicated internal HestiaStore problem with data consistency.
- `IllegalArgumentException` - validation error, for example when key type is not specified. It's also thrown when some object is not initialized correctly.
- `IllegalStateException` - When HestiaStore is in inconsistent state and is unable to recover.

All exceptions are runtime exceptions and doesn't have to be explicitly handled.
