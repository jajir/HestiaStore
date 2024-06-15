# Versioning of the project

Project use traditional versioning pattern. Version number consist of three numbers separated by dots. For example:

```
0.3.6
```

Meaning of number is:
* `0` - Major project version, project API could be incompatible between two major versions
* `3` - Minor project version contains changes in features, performance optimizations and small improvement. Minor versions should be compatible.
* `6` - Bug fixing project release

There are also released snapshot versions with version number `0.3.6-SNAPSHOT`.

# Release repository

As release repository is used github packages. Released packages could be easily used in your project. In case of maven:

```
<dependency>
  <groupId>com.coroptis</groupId>
  <artifactId>jbindex</artifactId>
  <version>1.0.0-SNAPSHOT</version>
</dependency>
```

# How to deploy new version

Following steps describes how to publish package to github packages.

* Make sure that code compile and all test are passing.
* Prepare personal github access token. With right to deploy.
* Replace `TOKEN` in file `TOKEN` with your personal github token.
* Execute: 
```
mvn --settings ./src/main/settings.xml clean deploy
```

it's done.
