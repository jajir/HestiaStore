# Installation

HestiaStore is published to Maven Central as `org.hestiastore:engine`.

## Prerequisites

- Java 17 or newer
- Maven 3.6+ or Gradle 6+

Latest artifact:
[org.hestiastore:engine](https://central.sonatype.com/artifact/org.hestiastore/engine)

## Maven

```xml
<dependencies>
  <dependency>
    <groupId>org.hestiastore</groupId>
    <artifactId>engine</artifactId>
    <version><!-- latest version --></version>
  </dependency>
</dependencies>
```

Verify the dependency is resolved:

```bash
mvn dependency:tree
```

## Gradle (Groovy DSL)

```groovy
repositories {
  mavenCentral()
}

dependencies {
  implementation "org.hestiastore:engine:<latest>"
}
```

Verify the dependency is resolved:

```bash
./gradlew dependencyInsight --dependency org.hestiastore:engine
```

## Gradle (Kotlin DSL)

```kotlin
repositories {
  mavenCentral()
}

dependencies {
  implementation("org.hestiastore:engine:<latest>")
}
```

## Build from source

If you need a local build instead of a published release:

```bash
mvn install
```

Then continue with [Quick Start](quick-start.md).
