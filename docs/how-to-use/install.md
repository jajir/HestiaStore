# Installation Guide


## ⚙️ Prerequisites

- Java 11 or higher
- Maven 3.6+ or Gradle 6+

HestiaStore is distributed via Maven Central.

## 🛠️ Maven

In your `pom.xml`, add the following to the `dependencies` section:

```xml
<dependencies>
  <dependency>
    <groupId>org.hestiastore.index</groupId>
    <artifactId>core</artifactId>
    <version>0.0.5</version> <!-- Replace with the actual version -->
  </dependency>
</dependencies>
```

### ✅ (Optionaly) Verify Installation

To verify that HestiaStore was installed successfully and is accessible from your project, try compiling and running a minimal example or run:

```bash
mvn dependency:tree
```

to confirm the dependency was resolved correctly.

## 🛠️ Gradle

For Gradle, add the following to your `build.gradle`:

```groovy
dependencies {
  implementation "org.hestiastore.index:core:0.0.5" // Replace with the actual version
}
```

## 🧱 Build from Sources

Source code for each release can be downloaded from [GitHub HestiaStore releases](https://github.com/jajir/HestiaStore/releases). Then, build the desired version using `mvn install`.
