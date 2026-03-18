# Release Process

This document is the canonical release procedure for Hestia Store. It covers versioning, verification, tagging, deployment, and the next snapshot bump for this Maven-based Java library repository.

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

The current repository configuration defines the Maven `release` profile in `engine/pom.xml`, so the deploy command below targets `engine` and builds required modules with `-am`.

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

If the version does not end with `-SNAPSHOT`, do not continue with release preparation until the intended state is clear.

### 3. Run pre-release verification

Use the helper script:

```bash
./.agents/skills/release-maven-library/scripts/verify-release.sh
```

This prints git status and runs:

```bash
mvn clean verify
```

If verification fails, stop and fix the problem before changing versions.

### 4. Check for forbidden snapshot dependencies or plugins

Search all Maven project files for snapshot versions:

```bash
rg -n --glob 'pom.xml' 'SNAPSHOT'
```

Before the release bump, the current project version is expected to appear as a snapshot. Any unrelated snapshot dependency, plugin, or version property is a release blocker and must be removed or intentionally approved before continuing.

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

After the release bump, there should be no remaining forbidden snapshot versions in the Maven project files.

### 6. Commit and tag the release

Review the changed files and create a focused release commit:

```bash
git add pom.xml */pom.xml
git commit -m "release: version X.Y.Z"
git tag release-X.Y.Z
```

Use the `release-X.Y.Z` tag format. Create the tag on the release commit, not on the later post-release snapshot commit.

### 7. Deploy the release

Deploy from the repository root:

```bash
mvn -pl engine -am -P release deploy
```

This matches the current repository configuration, where the `release` profile is defined in `engine/pom.xml`.

### 8. Prepare the next development snapshot

After the release is deployed, bump to the next snapshot version:

```bash
./.agents/skills/release-maven-library/scripts/prepare-next-snapshot.sh X.Y.(Z+1)-SNAPSHOT
./.agents/skills/release-maven-library/scripts/verify-release.sh
git add pom.xml */pom.xml
git commit -m "post-release: bumped to X.Y.(Z+1)-SNAPSHOT"
```

### 9. Push commits and tags

Push both the branch and the release tag:

```bash
git push origin main
git push origin release-X.Y.Z
```

### 10. Publish release notes

Create the GitHub release from tag `release-X.Y.Z` and summarize:

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
