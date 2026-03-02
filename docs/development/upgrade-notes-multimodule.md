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
- `TypeEncoder` API changed in `0.0.6`:
  - removed: `bytesLength(T)` and `toBytes(T, byte[])`
  - required: `EncodedBytes encode(T value, byte[] reusableBuffer)`

### TypeEncoder migration checklist (0.0.6+)

1. Replace old two-step encoding with `encode(...)` implementation.
1. Reuse caller-provided buffer when possible; allocate only when too small.
1. Return exact written byte count in `EncodedBytes.length`.
1. Update tests and custom integrations to call `encode(...)` only.

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
