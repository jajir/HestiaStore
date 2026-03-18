---
name: release-maven-library
description: Use this skill for Maven release and versioning tasks in the Hestia Store repository, including Central publishing prerequisites, parent-POM-aware release validation, release version preparation, and the next development snapshot.
---

# Release Maven Library

Use this skill when the task is to prepare, verify, or advance a Maven library release in Hestia Store.

This skill performs the full local release flow described in
`docs/development/release.md`: prerequisite checks, pre-release verification,
release version bump, release commit, release tag, Maven Central deployment,
next snapshot bump, next snapshot commit, and Git pushes.

The final GitHub release publication is manual and must be completed on the
repository homepage at `https://github.com/jajir/HestiaStore/releases`. This
skill can prepare the release title/body and confirm the pushed tag, but it
cannot click `Publish release` in the GitHub web UI.

## Rules

- Keep the work limited to release/versioning changes.
- Do not perform unrelated refactors, cleanup, or API changes.
- Stop immediately and report clearly if the repository is dirty, Maven metadata is inconsistent, snapshot checks fail, signing or Central credentials are missing, or tests fail.

## Workflow

1. Inspect the current git branch and working tree status before changing anything.
   - Run `git status --short --branch`.
   - For an actual release, require the `main` branch.
   - If the working tree is not clean, or the branch is wrong for a real release, stop and report it.
2. Update the local `main` branch before version changes.
   - Run `git pull --ff-only`.
   - If the pull cannot fast-forward cleanly, stop and report it.
3. Detect the current Maven project version from the repository root.
   - Run `mvn help:evaluate -Dexpression=project.version -q -DforceStdout`.
   - If Maven cannot resolve the version, stop and report it.
4. Verify whether the current version is a `-SNAPSHOT`.
   - A release preparation should start from `X.Y.Z-SNAPSHOT`.
   - If the version is already a release version and the user asked for release preparation, report that state instead of forcing changes.
5. Confirm local release prerequisites before changing versions.
   - Check `~/.m2/settings.xml` for the `central` server entry and a `release` profile with GPG properties.
   - Check that a GPG secret key exists with `gpg --list-secret-keys --keyid-format LONG`.
   - If credentials, signing configuration, or a usable secret key are missing, stop and report the blocker.
6. Run the standard pre-release verification from the release document.
   - Use `./.agents/skills/release-maven-library/scripts/verify-release.sh`.
   - If verification fails, stop and report the failure.
7. Check for forbidden snapshot dependencies or plugins before release.
   - Search `pom.xml` files for `SNAPSHOT`.
   - Allow the current project version while it is still the development snapshot being released.
   - Any other snapshot dependency, plugin, or property must be treated as a release blocker and reported clearly.
8. Validate the release deployment shape before tagging or deploying.
   - Hestia Store publishes the parent POM together with release modules because child modules inherit dependency management from `hestiastore-parent`.
   - Do not deploy `engine` alone with `mvn -pl engine -P release deploy`.
   - Run release validation and deployment from the repository root with `mvn -P release ...` so the parent POM and all publishable modules stay aligned.
   - If parent POM metadata, SCM, licenses, developers, or release plugin configuration are missing or inconsistent, stop and report it.
9. Prepare the release version by removing `-SNAPSHOT`.
   - Convert `X.Y.Z-SNAPSHOT` to `X.Y.Z`.
   - Run `./.agents/skills/release-maven-library/scripts/bump-version.sh X.Y.Z`.
10. Commit the release version change.
   - Review the modified `pom.xml` files and any release-related files.
   - Commit with `git commit -am "release: version X.Y.Z"`.
11. Run Maven verification again after the version change.
   - Use `./.agents/skills/release-maven-library/scripts/verify-release.sh`.
   - If verification fails, stop and report the failure.
12. Run release-profile verification before deployment.
   - Run `mvn -P release -DskipTests verify`.
   - This must confirm signing and release metadata before attempting `deploy`.
   - If this step fails, stop and report the exact blocker instead of proceeding.
13. Create the local release tag once release verification is clean.
   - Use the release tag format `release-X.Y.Z`.
   - Run `git tag release-X.Y.Z`.
14. Deploy the release once the local release tag exists.
   - Deploy with `mvn -P release -DskipTests deploy`.
   - If Sonatype accepts the upload but reports `PUBLISHING`, treat that as an in-progress publish state rather than an immediate failure.
   - If Sonatype reports missing signatures, missing parent metadata, unresolved dependency versions, or coordinate validation errors, stop and report them as release blockers.
15. Push the release commit and tag after deployment succeeds.
   - Run `git push origin main release-X.Y.Z`.
16. Prepare the next development version after the release.
   - Choose the next version requested by the user or infer the next patch snapshot if the task explicitly asks for it.
   - Run `./.agents/skills/release-maven-library/scripts/prepare-next-snapshot.sh X.Y.(Z+1)-SNAPSHOT`.
   - Run `./.agents/skills/release-maven-library/scripts/verify-release.sh` again after the next snapshot bump when the task includes the full post-release flow.
   - Commit with `git commit -am "post-release: bumped to X.Y.(Z+1)-SNAPSHOT"`.
   - Push with `git push origin main`.
17. Prepare the GitHub release publication details.
   - The GitHub release must be published manually on `https://github.com/jajir/HestiaStore/releases`.
   - Reuse the pushed tag `release-X.Y.Z`; do not create a different tag in the GitHub UI.
   - Suggested title: `Release X.Y.Z`.
   - Include the dependency snippet from the release document and a `Breaking changes` section when needed.
   - State clearly that the skill cannot complete the web UI click path itself.
18. End with a concise release summary.
   - Include branch name, starting version, release version, deployment command used, Sonatype status if known, next snapshot version if prepared, verification results, and changed files.

## Notes

- Keep [`references/release-checklist.md`](references/release-checklist.md) as the quick human/Codex checklist.
- Run all commands from the repository root so multi-module version changes stay consistent.
