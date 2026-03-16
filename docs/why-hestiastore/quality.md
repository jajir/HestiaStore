# Quality and Testing

HestiaStore treats quality signals as part of the product surface, not as
maintenance-only detail. This page summarizes the checks that help validate
correctness, compatibility, and operational safety.

## Continuous integration

- Main and pull-request builds run Maven verification.
- Unit, integration, and packaging checks run in CI before changes land.
- Selected WAL hardening and stress workflows run separately for durability and
  failure-path coverage.

## Quality signals

- Test execution and published test summaries
- JaCoCo line coverage reporting
- Static analysis and quality gates through SonarCloud
- Dependency vulnerability scanning through OWASP Dependency Check
- GitHub dependency review and CodeQL workflows

## What gets validated

- Core storage behavior: read, write, iteration, and persistence flows
- Segment and SegmentIndex concurrency-sensitive behavior
- WAL verification, tooling, and recovery paths
- Monitoring and operational integration surfaces
- Benchmark profile contracts for performance-sensitive workflows

## Where to go deeper

- [Code Quality Charter](../development/code-quality-charter.md)
- [Quality Reporting](../development/quality-reporting.md)
- [Rollout Quality Gates](../development/rollout-quality-gates.md)
- [Release Process](../development/release.md)

## What this means for adopters

These checks do not replace workload-specific validation in your environment,
but they do show that HestiaStore tracks correctness, coverage, and operational
regressions as first-class concerns rather than afterthoughts.
