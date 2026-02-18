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
  - metrics bridge: add `monitoring-api` + `monitoring-micrometer` and/or
    `monitoring-prometheus`
  - management control plane: add `management-agent` and optionally
    `monitoring-console` / `monitoring-console-web`

## Breaking-change expectation

- Package names for core APIs are preserved.
- Management contracts are versioned under `/api/v1/...`.
- DTO compatibility is guarded by contract tests.

## Recommended upgrade sequence

1. Upgrade dependency coordinates to new module artifacts.
1. Keep versions aligned for all HestiaStore modules.
1. Deploy `management-agent` next to engine JVM if control endpoints are needed.
1. Register nodes in `monitoring-console` and validate dashboard/action paths.
1. Enable auth/TLS policy and rate limits before production rollout.

## Post-upgrade smoke checks

```bash
curl -s http://<agent-host>:<port>/api/v1/state
curl -s http://<agent-host>:<port>/api/v1/metrics
curl -s http://<console-host>:<port>/console/v1/dashboard
```

For secured deployments, include `Authorization: Bearer <token>` and HTTPS.
