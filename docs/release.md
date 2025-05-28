# Releasing new version

Simple guide how to make new release.

## Versioning of the project

Project use traditional versioning pattern. Version number consist of three numbers separated by dots. For example:

```
0.3.6
```

Meaning of number is:

* `0` - Major project version, project API could be incompatible between two major versions
* `3` - Minor project version contains changes in features, performance optimizations and small improvement. Minor versions should be compatible.
* `6` - Bug fixing project release

There are also snapshot versions with version number `0.3.6-SNAPSHOT`. Snapshot versions should not by sotred into maven repository.

## Branching strategy

![project branching](./images/branching.png)

We use a simplified GitHub Flow:

* `main`: the primary development and release branch. Small changes may be committed directly to `main`, while larger or experimental features must be developed in a separate branch and merged via pull request.
* Feature branches: created from `main` for larger or isolated changes. Use descriptive names like `feature/compression`, `fix/index-scan`, etc.

The previous `devel` branch is no longer used and has been removed.

## How to release new version

### Prerequisites

 Adjust settings.xml in `~/.m2/settings.xml` like this described at [github official documentation how to work with github maven repository](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-apache-maven-registry). Get correct token and it's done.

## How to make release

Release will appear in maven central.

### Release prerequisities:

1. File `~/m2/settings.xml` should contains:

```xml
<settings>
    ...
	<profile>
	  <id>release</id>
	  <properties>
	    <gpg.executable>gpg</gpg.executable>
	    <gpg.passphrase>--pgp-password--</gpg.passphrase>
	  </properties>      
	</profile>
    ...
</settings>
```
2. At [central.sonatype.com](https://central.sonatype.com/) heve to be account with verified namespace `org.hestiastore`. From section `Acount` key and password have to be generated. both should be placed at 

```xml
<settings>
    ...
    <servers>
        <server>
            <id>central</id>
	        <username>------</username>
	        <password>---------------token---------------</password>
	    </server>
    </servers>
    ...
</settings>
```

### Perform release 

Perform the following steps to create a new release:

1. Checkout the `main` branch:

   ```
   git checkout main
   ```

2. Set the release version:

   ```
   mvn versions:set -DnewVersion=0.0.12
   git commit -am "release: version 0.0.12"
   ```

3. Tag and push the release:

   ```
   git tag v0.0.12
   git push --follow-tags
   ```

4. Deploy the release (can be automated via GitHub Actions or done manually):

   ```
   mvn deploy -P release
   ```

5. Bump to next snapshot version:

   ```
   mvn versions:set -DnewVersion=0.0.13-SNAPSHOT
   git commit -am "post-release: bumped to 0.0.13-SNAPSHOT"
   git push
   ```

That's it — the release is live and development can continue.

## Helpfull commands

### How to use custom settings.xml file

```
mvn --settings ./src/main/settings.xml clean deploy
```

### How to use set maven project version

```
mvn versions:set -DnewVersion=1.0.1-SNAPSHOT
```

### Check dependencies

try to update dependencies. Check them with:

```
mvn versions:display-dependency-updates
```
