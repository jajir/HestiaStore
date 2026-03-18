# Hestia Store Repository Rules

- This repository is a Maven-based Java library project with a multi-module parent build.
- Prefer focused changes that stay within the requested scope.
- Avoid unnecessary API-breaking changes.
- Run `mvn clean verify` after non-trivial changes.
- Use the `fix-ci-failure` skill for failing CI jobs and broken local verification runs.
- Use the `update-dependencies` skill for Maven dependency and plugin refresh work.
- Use the `java-code-review` skill for reviewing diffs, branches, and pull requests.
- Use the `investigate-test-flake` skill for intermittent or nondeterministic test failures.
- Use the `benchmark-regression-check` skill for benchmark drift and performance regression investigation.
