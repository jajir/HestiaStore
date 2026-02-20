# Upgrade Notes: Multi-Module Line

These notes apply when upgrading from older single-jar usage to the current
multi-module distribution.

## What changed

- Build moved to a parent multi-module Maven project.
- Monitoring and management integrations are now separate artifacts.
- Core `engine` remains free of Micrometer/Prometheus/console dependencies.

## Artifact mapping

- Old usage: one runtime jar with mixed concerns.
- New usage:
  - core only: `org.hestiastore:engine`
  - metrics bridge: add `monitoring-micrometer` and/or `monitoring-prometheus`
  - management control plane: add `monitoring-rest-json-api`,
    `monitoring-rest-json`, and
    `monitoring-console-web` (direct mode)

## Breaking-change expectation

- Package names for core APIs are preserved.
- Management contracts are versioned under `/api/v1/...`.
- DTO compatibility is guarded by contract tests.

## Recommended upgrade sequence

1. Upgrade dependency coordinates to new module artifacts.
1. Keep versions aligned for all HestiaStore modules.
1. Deploy `monitoring-rest-json` next to engine JVM if control endpoints are needed.
1. Configure `monitoring-console-web` with direct node endpoints and validate
   dashboard/action paths.
1. Enable auth/TLS policy and rate limits before production rollout.

## Post-upgrade smoke checks

```bash
curl -s http://<agent-host>:<port>/api/v1/report
```

For secured deployments, include `Authorization: Bearer <token>` and HTTPS.
