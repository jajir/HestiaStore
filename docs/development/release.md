# Release Process

This is the source-of-truth runbook for making a new HestiaStore release.

When you ask Codex to use the `release-maven-library` skill, it performs the
full local release workflow described on this page: prerequisite checks,
pre-release verification, release version bump, release commit, release tag,
Maven Central deployment, next snapshot bump, next snapshot commit, and Git
pushes.

The only manual step is publishing the GitHub release on the repository
homepage at [https://github.com/jajir/HestiaStore/releases](https://github.com/jajir/HestiaStore/releases).
Codex can prepare the release title and body, but it cannot click `Publish
release` in the GitHub web UI.

If you are using Codex in this repository, use the `release-maven-library` skill. The skill follows the same workflow documented here and uses the helper scripts under `.agents/skills/release-maven-library/scripts/`.

## Release Principles

- Keep release work focused on release and versioning changes only.
- Do not mix unrelated refactors, dependency upgrades, or API changes into a release commit.
- Stop immediately if the working tree is dirty, the current version is inconsistent, a forbidden snapshot dependency is found, or verification fails.

## Versioning

Hestia Store uses Semantic Versioning:

```text
X.Y.Z
```

- `X`: major version for incompatible API changes
- `Y`: minor version for backward-compatible feature work
- `Z`: patch version for backward-compatible fixes

Development versions use the `-SNAPSHOT` suffix:

```text
X.Y.Z-SNAPSHOT
```

Release preparation should start from a snapshot version and convert it to the matching release version.

The release will be published to Maven Central. Release configuration secrets are placed at the Maven settings file `~/.m2/settings.xml`.

Releases must be prepared from a clean `main` branch worktree with push access
to the repository and tags.

- Java 17 and Maven are installed.
- You are in the repository root.
- The branch and working tree are clean.
- Maven Central credentials are configured in `~/.m2/settings.xml`.
- GPG signing is configured for the Maven `release` profile.

Example `settings.xml` fragments:

```xml
<settings>
  <servers>
    <server>
      <id>central</id>
      <username>YOUR_USERNAME</username>
      <password>YOUR_TOKEN</password>
    </server>
  </servers>

  <profiles>
    <profile>
      <id>release</id>
      <properties>
        <gpg.executable>gpg</gpg.executable>
        <gpg.passphrase>YOUR_GPG_PASSPHRASE</gpg.passphrase>
      </properties>
    </profile>
  </profiles>
</settings>
```

### Setup Maven Central account secrets

This provides `org.sonatype.central:central-publishing-maven-plugin` plugin secrets to enable login to the Maven Central account where release data will be placed.
You must have an account with a verified namespace `org.hestiastore` at [central.sonatype.com](https://central.sonatype.com/). From the `Account` section, generate a key and password. These should be added to:

## Release Checklist

- Verify the working tree is clean.
- Detect the current Maven version.
- Confirm the current version is a snapshot.
- Run `mvn clean verify`.
- Confirm there are no forbidden snapshot dependencies or plugins.
- Bump from `X.Y.Z-SNAPSHOT` to `X.Y.Z`.
- Run verification again after the release version change.
- Commit and tag the release.
- Deploy the release artifact.
- Bump to the next snapshot version.
- Verify again after the next snapshot bump.

## Step-by-Step Procedure

### 1. Inspect branch and working tree

Run the standard release verification script:

```bash
./.agents/skills/release-maven-library/scripts/verify-release.sh
```

That script runs the release verification used by the skill:

```bash
mvn -N install
mvn -pl engine,wal-tools,monitoring-micrometer,monitoring-prometheus,monitoring-rest-json-api,monitoring-rest-json,monitoring-console-web -DskipTests package
mvn -pl wal-tools,monitoring-micrometer,monitoring-prometheus,monitoring-rest-json-api,monitoring-rest-json,monitoring-console-web test
mvn -pl engine test -Dtest=IntegrationSegmentIndexMetricsSnapshotConcurrencyTest
mvn -pl monitoring-prometheus test -Dtest=HestiaStorePrometheusExporterTest
mvn clean verify
```

### 1. Checkout and update the `main` branch

```bash
git checkout main
git pull --ff-only
```

Expected result:

```bash
./.agents/skills/release-maven-library/scripts/bump-version.sh 0.0.12
git commit -am "release: version 0.0.12"
```

### 3. Validate the release profile

Run the root release profile so the parent POM and all release modules are
validated together:

```bash
mvn -P release -DskipTests verify
```

Do not deploy `engine` alone. The release must be run from the repository root
so the parent POM and all publishable modules stay aligned.

### 4. Create the release tag

```bash
git tag release-0.0.12
```

### 5. Deploy the release

Deploy the release to Maven Central from the repository root:

```bash
mvn -P release -DskipTests deploy
```

### 6. Push the release commit and tag

Push both `main` and the release tag after deployment succeeds:

```bash
git push origin main release-0.0.12
```

### 7. Bump to the next snapshot version

```bash
./.agents/skills/release-maven-library/scripts/prepare-next-snapshot.sh 0.0.13-SNAPSHOT
git commit -am "post-release: bumped to 0.0.13-SNAPSHOT"
git push origin main
```

### 8. Publish the release on GitHub

This step is manual and must be completed on the GitHub repository homepage,
not in the generated documentation site:

1. Go to [https://github.com/jajir/HestiaStore/releases](https://github.com/jajir/HestiaStore/releases) and choose `Draft a new release`.
2. Select the existing tag `release-0.0.12`.
3. Keep `main` as the target branch.
4. Set the release title to `Release 0.0.12`.
5. In the `Write` field, use the text generated from the template below.
6. If the release contains breaking changes, add a dedicated `Breaking changes` section with migration steps.
7. Press `Publish release`.

```bash
./.agents/skills/release-maven-library/scripts/bump-version.sh 0.0.6
```

Then verify again:

```xml
<dependencies>
  <dependency>
    <groupId>org.hestiastore</groupId>
    <artifactId>engine</artifactId>
    <version>0.0.12</version> <!-- Replace with the actual version -->
  </dependency>
</dependencies>
```

After the release bump, there should be no remaining forbidden snapshot versions in the Maven project files.

### 6. Commit and tag the release

Review the changed files and create a focused release commit:

- `org.hestiastore:hestiastore-parent` (POM)
- `org.hestiastore:engine`
- `org.hestiastore:wal-tools`
- `org.hestiastore:monitoring-rest-json-api`
- `org.hestiastore:monitoring-micrometer`
- `org.hestiastore:monitoring-prometheus`
- `org.hestiastore:monitoring-rest-json`
- `org.hestiastore:monitoring-console-web`

The `benchmarks` module participates in the build but is not deployed because
its POM sets `maven.deploy.skip=true`.

Compatibility and staged upgrade guidance:

### 7. Deploy the release

### 9. Finish the release

```bash
mvn -pl engine -am -P release deploy
```

This matches the current repository configuration, where the `release` profile is defined in `engine/pom.xml`.

### 8. Prepare the next development snapshot

After the release is deployed, bump to the next snapshot version:

```bash
./.agents/skills/release-maven-library/scripts/prepare-next-snapshot.sh 0.0.7-SNAPSHOT
./.agents/skills/release-maven-library/scripts/verify-release.sh
git add pom.xml */pom.xml
git commit -m "post-release: bumped to 0.0.7-SNAPSHOT"
```

### 9. Push commits and tags

Push both the branch and the release tag:

```bash
git push origin main
git push origin release-0.0.6
```

### 10. Publish release notes

Create the GitHub release from tag `release-0.0.6` and summarize:

- released version
- notable changes
- any migration or compatibility notes
- Maven dependency coordinates consumers should use

## Helper Commands

Display dependency updates:

```bash
mvn versions:display-dependency-updates
```

Set a version manually without backup POMs:

```bash
mvn versions:set -DnewVersion=1.0.1-SNAPSHOT -DgenerateBackupPoms=false -DprocessAllModules=true
```

## Related Files

- `.agents/skills/release-maven-library/SKILL.md`
- `.agents/skills/release-maven-library/references/release-checklist.md`
- `AGENTS.md`
