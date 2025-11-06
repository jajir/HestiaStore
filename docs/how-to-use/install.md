# Installation Guide

## ⚙️ Prerequisites

- Java 11 or higher (Java 17 recommended)
- Maven 3.6+ or Gradle 6+

HestiaStore is distributed via Maven Central: <https://central.sonatype.com/artifact/org.hestiastore.index/core>

## 🛠️ Maven

Add the dependency to your pom.xml (use the latest version from Maven Central):

```xml
<dependencies>
  <dependency>
    <groupId>org.hestiastore.index</groupId>
    <artifactId>core</artifactId>
    <version><!-- latest --></version>
  </dependency>
</dependencies>
```

### ✅ Verify Installation (Maven)

```bash
mvn dependency:tree
```

Confirm org.hestiastore.index:core is present in the dependency tree.

## 🛠️ Gradle

Add the dependency to your Gradle build (use the latest version from Maven Central). Ensure mavenCentral() is in repositories.

Groovy DSL (build.gradle):

```groovy
repositories {
  mavenCentral()
}

dependencies {
  implementation "org.hestiastore.index:core:<latest>"
}
```

### ✅ Verify Installation (Gradle)

```bash
./gradlew dependencyInsight --dependency org.hestiastore.index:core
```

## 🛠️ Kotlin

Kotlin DSL (build.gradle.kts):

```kotlin
repositories {
  mavenCentral()
}

dependencies {
  implementation("org.hestiastore.index:core:<latest>")
}
```

## 🧱 Build from Sources

Source code for each release can be downloaded from GitHub releases:
<https://github.com/jajir/HestiaStore/releases>

Build the desired version:

```bash
mvn install
```
