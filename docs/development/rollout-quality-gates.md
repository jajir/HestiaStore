# Rollout Stages and Quality Gates

This document defines releasable stages for the monitoring/management rollout.

## Stage A: Core metrics snapshot only

- Scope: `engine` module, immutable snapshot API, no external exporters.
- Required gates:
  - Concurrency correctness test: `IntegrationSegmentIndexMetricsSnapshotConcurrencyTest`
  - Snapshot contract tests pass in `engine`.
  - Perf budget: no more than +3% overhead for get/put baseline in internal
    benchmark profile.

## Stage B: Monitoring bridge (Prometheus/Micrometer)

- Scope: `monitoring-micrometer`, `monitoring-prometheus`.
- Required gates:
  - Prometheus scrape contract test:
    `HestiaStorePrometheusExporterTest`.
  - Metric naming/tag compatibility stays stable (`hestiastore_*`, `index` tag).
  - Perf budget: exporter disabled path adds effectively zero overhead in core.

## Stage C: Node management agent

- Scope: `monitoring-rest-json-api`, `monitoring-rest-json`.
- Required gates:
  - API behavior tests: `ManagementAgentServerTest`.
  - Security tests: `ManagementAgentServerSecurityTest`.
  - Audit trail coverage for all mutating endpoints.
  - Failure mode checks for unauthorized/forbidden/rate-limited requests.

## Stage D: Direct web console

- Scope: `monitoring-console-web` in direct mode (`/api/v1/*` to nodes).
- Required gates:
  - Node dashboard render for reachable/unreachable nodes.
  - Action flow tests: flush/compact actions and user feedback.
  - Failure mode tests: node down, timeout, auth failure.

## Gate execution command

Run all stage gates:

```bash
mvn -pl engine test -Dtest=IntegrationSegmentIndexMetricsSnapshotConcurrencyTest
mvn -pl monitoring-prometheus test -Dtest=HestiaStorePrometheusExporterTest
mvn -pl monitoring-rest-json test -Dtest=ManagementAgentServerTest,ManagementAgentServerSecurityTest
mvn -pl monitoring-console-web test
```

## Release and rollback readiness

- Each stage is releasable independently.
- If a stage fails in production, rollback can stop at previous stage without
  breaking core index operation.
- Detailed rollback steps are in:
  [Rollout Rollback Runbook](rollout-rollback-runbook.md).
