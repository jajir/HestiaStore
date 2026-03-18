# Hestia Store Repository Rules

- This repository is a Maven-based Java library project with a multi-module parent build.
- Prefer focused changes that stay within the requested scope.
- Avoid unnecessary API-breaking changes.
- Run `mvn clean verify` after non-trivial changes.
- Use the `release-maven-library` skill for release preparation, versioning, and post-release snapshot tasks.
- Use the `sonar-smell-cleanup` skill for small, safe SonarCloud code smell cleanup batches.
