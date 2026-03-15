# Quality Reporting

This page describes the report-only static analysis workflow used on pull
requests to `main`.

The initial goal is visibility, not enforcement. Findings are collected and
published as CI artifacts before any hard fail threshold is introduced.

## Current Scope

The reporting workflow currently runs:

- PMD
- CPD (copy/paste detection)
- SpotBugs
- Dependency Review
- CodeQL

These reports are generated through Maven plugins so they can be reproduced
locally and in GitHub Actions with the same command line.

## Local Command

Run the same report-only analysis locally from the repository root:

```sh
mvn -B -ntp -DskipTests -Ddependency-check.skip=true test-compile
mvn -B -ntp -DskipTests -Ddependency-check.skip=true \
  pmd:pmd \
  pmd:cpd \
  spotbugs:spotbugs
```

## CI Behavior

- The workflow runs on pull requests targeting `main`.
- Findings are uploaded as artifacts and summarized in the workflow summary.
- Findings do not fail the PR yet.
- Build errors in the tools themselves still fail the workflow.

## Why Maven First

Maven is the execution engine for Java static analysis in this project:

- local and CI commands stay identical
- plugin configuration lives close to the code
- reports can later move from report-only to gated checks without changing the
  core toolchain

GitHub-native features still matter, but they sit around Maven:

- Dependabot for update drift
- Dependency Review for new and changed dependencies in pull requests
- CodeQL for deeper security and dataflow scanning

## Next Steps

Once report quality is stable and noise is understood, the next additions should
be:

- Checkstyle with a project-owned ruleset
- Maven Enforcer for environment and dependency hygiene
- PR annotations or SARIF-backed summaries for better report visibility
- selective hard thresholds on agreed rules only
