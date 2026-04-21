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

If you are using Codex in this repository, use the `release-maven-library`
skill. The skill follows the same workflow documented here and uses the helper
scripts under `.agents/skills/release-maven-library/scripts/`.

## Release Principles

- Keep release work focused on release and versioning changes only.
- Do not mix unrelated refactors, dependency upgrades, or API changes into a
  release commit.
- Stop immediately if the working tree is dirty, the current version is
  inconsistent, a forbidden snapshot dependency is found, or verification
  fails.

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

Release preparation should start from a snapshot version and convert it to the
matching release version.

Releases are published to Maven Central. Release credentials and signing
configuration live in `~/.m2/settings.xml`.

Standalone operational ZIP distributions are also part of the release build.
For export/import, the official CLI artifact is
`index-tools/target/index-tools-<version>.zip` with
`index-tools/target/index-tools-<version>.zip.sha256`. For WAL inspection, the
official CLI artifact is `wal-tools/target/wal-tools-<version>.zip` with
`wal-tools/target/wal-tools-<version>.zip.sha256`.

Releases must be prepared from a clean `main` branch worktree with push access
to the repository and tags.

## Prerequisites

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

Generate the Maven Central credentials from your account at
[central.sonatype.com](https://central.sonatype.com/), and make sure a usable
GPG secret key exists locally before starting a release.

## Release Checklist

- Verify the working tree is clean and the release is being prepared from
  `main`.
- Detect the current Maven version.
- Confirm the current version is a snapshot.
- Run `./.agents/skills/release-maven-library/scripts/verify-release.sh`.
- Confirm there are no forbidden snapshot dependencies or plugins.
- Bump from `X.Y.Z-SNAPSHOT` to `X.Y.Z`.
- Run verification again after the release version change.
- Commit and tag the release.
- Run `mvn -P release -DskipTests verify`.
- Deploy the release artifact from the repository root.
- Confirm the standalone CLI ZIP and SHA-256 artifacts were produced for
  operational tools.
- Bump to the next snapshot version.
- Verify again after the next snapshot bump.
- Publish the GitHub release manually.

## Step-by-Step Procedure

### 1. Inspect branch and working tree

Use the intended release branch, usually `main`, and make sure it is clean:

```bash
git checkout main
git pull --ff-only
git status --short --branch
```

If the working tree is not clean, stop and resolve that first.

### 2. Detect the current project version

Run:

```bash
mvn help:evaluate -Dexpression=project.version -q -DforceStdout
```

Expected result:

```text
X.Y.Z-SNAPSHOT
```

If the version does not end with `-SNAPSHOT`, do not continue with release
preparation until the intended state is clear.

### 3. Run pre-release verification

Use the helper script:

```bash
./.agents/skills/release-maven-library/scripts/verify-release.sh
```

If verification fails, stop and fix the problem before changing versions.

### 4. Check for forbidden snapshot dependencies or plugins

Search all Maven project files for snapshot versions:

```bash
rg -n --glob 'pom.xml' 'SNAPSHOT'
```

Before the release bump, the current project version is expected to appear as a
snapshot. Any unrelated snapshot dependency, plugin, or version property is a
release blocker and must be removed or intentionally approved before
continuing.

### 5. Prepare the release version

Convert `X.Y.Z-SNAPSHOT` to `X.Y.Z` using the helper script:

```bash
./.agents/skills/release-maven-library/scripts/bump-version.sh X.Y.Z
```

Then verify again:

```bash
./.agents/skills/release-maven-library/scripts/verify-release.sh
rg -n --glob 'pom.xml' 'SNAPSHOT'
git status --short
```

After the release bump, there should be no remaining forbidden snapshot
versions in the Maven project files.

### 6. Commit and tag the release

Review the changed files and create a focused release commit:

```bash
git add pom.xml */pom.xml
git commit -m "release: version X.Y.Z"
git tag release-X.Y.Z
```

Use the `release-X.Y.Z` tag format. Create the tag on the release commit, not
on the later post-release snapshot commit.

The `benchmarks` module participates in the build but is not deployed because
its POM sets `maven.deploy.skip=true`.

### 7. Validate the release profile

Run the root release profile so the parent POM and all release modules are
validated together:

```bash
mvn -P release -DskipTests verify
```

Do not deploy `engine` alone. The release must be run from the repository root
so the parent POM and all publishable modules stay aligned.

### 8. Deploy the release

Deploy the release to Maven Central from the repository root:

```bash
mvn -P release -DskipTests deploy
```

After the release build, confirm these standalone tool distributions exist:

```text
index-tools/target/index-tools-X.Y.Z.zip
index-tools/target/index-tools-X.Y.Z.zip.sha256
wal-tools/target/wal-tools-X.Y.Z.zip
wal-tools/target/wal-tools-X.Y.Z.zip.sha256
```

Treat them as the official downloadable distributions for operational CLI use.

### 9. Prepare the next development snapshot

After the release is deployed, bump to the next snapshot version:

```bash
./.agents/skills/release-maven-library/scripts/prepare-next-snapshot.sh X.Y.(Z+1)-SNAPSHOT
./.agents/skills/release-maven-library/scripts/verify-release.sh
git add pom.xml */pom.xml
git commit -m "post-release: bumped to X.Y.(Z+1)-SNAPSHOT"
```

### 10. Push commits and tags

Push both the branch and the release tag:

```bash
git push origin main
git push origin release-X.Y.Z
```

### 11. Publish release notes

This step is manual and must be completed on the GitHub repository homepage,
not in the generated documentation site:

1. Go to [https://github.com/jajir/HestiaStore/releases](https://github.com/jajir/HestiaStore/releases) and choose `Draft a new release`.
2. Select the existing tag `release-X.Y.Z`.
3. Keep `main` as the target branch.
4. Set the release title to `Release X.Y.Z`.
5. In the description, include notable changes, any migration or compatibility
   notes, the dependency snippet below, and mention the standalone
   `index-tools-X.Y.Z.zip` operational distribution plus its SHA-256 file when
   export/import changed.
6. If the release contains breaking changes, add a dedicated `Breaking changes`
   section with migration steps.
7. Press `Publish release`.

Suggested dependency snippet:

```xml
<dependencies>
  <dependency>
    <groupId>org.hestiastore</groupId>
    <artifactId>engine</artifactId>
    <version>X.Y.Z</version>
  </dependency>
</dependencies>
```

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
