> **Note:** HestiaStore is a library, not a standalone application. It is designed to be integrated into a larger system to provide efficient storage and retrieval of large volumes of key-value pairs.

## Table of Contents

- [Prerequisites](#prerequisites)
- [Maven](#maven)
- [Gradle](#gradle)
- [Creating an index](#creating-an-index)
- [Opening an existing index](#opening-an-existing-index)
- [Data manipulation](#data-manipulation)
- [Sequential data reading](#sequential-data-reading)
- [Data maintenance](#data-maintenance)
- [Limitations](#limitations)
- [Thread Safety](#threadsafety)
- [Exception handling](#exception-handling)

# How to Use HestiaStore

## Prerequisites

- Java 11 or higher (recommended)
- Maven 3.6+ or Gradle 6+
- Access to GitHub Packages (requires authentication)

HestiaStore is available via GitHub Packages. To use it in your project, you need to add the GitHub repository and the dependency.

## Maven

Add the following to your `pom.xml`:

```xml
<repositories>
  <repository>
    <id>github</id>
    <url>https://maven.pkg.github.com/jajir/HestiaStore</url>
  </repository>
</repositories>

<dependencies>
  <dependency>
    <groupId>com.hestiastore</groupId>
    <artifactId>hestia-store</artifactId>
    <version>1.0.0</version> <!-- Replace with the actual version -->
  </dependency>
</dependencies>
```

> **Note:** You must authenticate to GitHub to access the package. Refer to [GitHub's documentation](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-apache-maven-registry) for details.

## Gradle

In your `build.gradle`:

```groovy
repositories {
  maven {
    url = uri("https://maven.pkg.github.com/jajir/HestiaStore")
    credentials {
      username = project.findProperty("gpr.user") ?: System.getenv("USERNAME")
      password = project.findProperty("gpr.key") ?: System.getenv("TOKEN")
    }
  }
}

dependencies {
  implementation "com.hestiastore:hestia-store:1.0.0" // Replace with the actual version
}
```

# Some examples

## Creating an index

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

This creates a simple in-memory index and stores a key-value pair.

When you have first example you can dive into [more advanced configuration](./configuration.md). There are explained details about `Directory` object and using custom Key/Value classes

## Opening an existing index

Please note that Index uses separate methods for creating index and for opening already existing index. So open already existing index use:

```java
IndexConfiguration<String, String> conf = IndexConfiguration
        .<String, String>builder()//
        .withKeyClass(String.class)//
        .withValueClass(String.class)//
        .withName("test_index") //
        .build();

Index<String, String> index = Index.<String, String>open(directory, conf);
```

## Data manipulation

There are two methods `put` and `get` using them is straightforward:

```java
index.put("Hello", "World");

String value = index.get("Hello");
```

Stored values are immediately available. Command ordering could be random.

## Sequential data reading

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

## Data maintenance

In some cases could be useful to perform maintenance with data. There are following operations with `Index`:

- `flush()` It flush all data from memory to disk to ensure that all data is safely stored. It make sure that data are stored. Could be called befor index iterating and when user want to be sure, that all data are stored.
- `checkAndRepairConsistency()` It verify that meta data about data in index are consistent. Some problems coudl repair. When index is beyond repair it fails.
- `compact();` Goes through all segments add compact main segment data with temporal files. It can save disk space.

## Limitations

### Staled result from index.getStream() method

Data from `index.getStream()` method could be staled or invalid. It's corner case when next readed key value pair is changed. Index data streaming is splited internally into steps `hasNextElement()` and `getNextElement()`. Following example will show why it's no possible to use index cache:

```java
index.hasNextElement(); // --> true
```

Now next element has to be known to be sure that exists. Let's suppose that in index is just one element `<k1,v1>`.

```java
index.delete("k1");
index.nextElement(); // --> fail
```

last operation will fail because there is not possible to find next element because `<k1,v1>` was deleted. To prevent this problem index cache is not used during index streaming. If all index content should be streamed than before streaming should be `compact()` method and during streaming data shouldn't be changed.

To be sure that all data is read than befor reading perform `Index.flush()` and during iterating avoid using of `Index.put()` and `Index.delete()` operations.

### ThreadSafe

> **Note:** Index is not thread-safe by default. Use `.withThreadSafe(true)` in the configuration to enable thread safety.

## Exception handling

Here are exceptions that could be throws from HestiaStore:

- `NullPointerException` -  When something fails really badly. For example when disk reading fails or when user delete part of configuration file.
- `IndexException` - Usually indicated internal HestiaStore problem with data consistency.
- `IllegalArgumentException` - validation error, for example when key type is not specified. It's also thrown when some object is not initialized correctly.
- `IllegalStateException` - When HestiaStore is in inconsistent state and is unable to recover.

All exceptions are runtime exceptions and doesn't have to be explicitly handled.
