# Rollout Rollback Runbook

This runbook defines rollback actions for stages A-D.

## Preconditions

- Keep previous release artifacts available for all modules.
- Keep Prometheus scrape config versioned with release tags.

## Rollback strategy

Rollback is stage-local. Do not roll back `engine` unless stage A itself fails.

## Stage A rollback (core snapshot API)

1. Re-deploy previous `engine` artifact.
1. Re-run smoke tests:
   - open index,
   - put/get/delete,
   - close/reopen.
1. Confirm no dependency on monitoring modules is required.

## Stage B rollback (monitoring bridge)

1. Remove or downgrade:
   - `monitoring-api`
   - `monitoring-micrometer`
   - `monitoring-prometheus`
1. Keep `engine` unchanged.
1. Disable scrape target temporarily if exporter endpoint fails.

## Stage C rollback (management agent)

1. Revert `management-agent` and `management-api` to previous release pair.
1. Keep index runtime alive; management endpoints can be temporarily disabled.
1. Validate:
   - `/api/v1/report`
   - secured action paths reject unauthorized requests.

## Stage D rollback (direct web console)

1. Revert `monitoring-console-web` to previous version.
1. Keep agents running; web console is control-plane and can be redeployed
   independently.
1. Validate dashboard polling and action submission after rollback.

## Rollback verification command

After rollback deployment, run:

```bash
mvn -pl engine test -Dtest=IntegrationSegmentIndexMetricsSnapshotConcurrencyTest
mvn -pl monitoring-prometheus test -Dtest=HestiaStorePrometheusExporterTest
mvn -pl management-agent test -Dtest=ManagementAgentServerTest,ManagementAgentServerSecurityTest
mvn -pl monitoring-console-web test
```

## Incident notes template

- stage:
- release version:
- failure symptom:
- rollback version:
- verification result:
- follow-up action:
