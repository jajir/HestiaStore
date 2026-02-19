# Compatibility Matrix

This matrix defines supported same-version combinations for the rollout modules.

## Version alignment policy

All runtime modules are released with the same version number (for example
`0.2.x` for all modules in one release line).

## Supported combinations

| Module | Required same-version modules | Notes |
|---|---|---|
| `engine` | none | Core runtime without monitoring dependencies. |
| `monitoring-api` | `engine` | Shared names/contracts for monitoring adapters. |
| `monitoring-micrometer` | `engine`, `monitoring-api` | Micrometer bridge from core snapshots. |
| `monitoring-prometheus` | `engine`, `monitoring-api`, `monitoring-micrometer` (or direct use) | Prometheus scrape support. |
| `management-api` | none | Shared DTO contracts for agent and web console direct mode. |
| `management-agent` | `engine`, `management-api` | Node-local management endpoints. |
| `monitoring-console-web` | `management-api`, `management-agent` (remote HTTP) | Spring MVC/Thymeleaf operator UI (direct node mode). |

## Runtime compatibility rule

- Supported: all modules on the same version.
- Not guaranteed: mixing different minor versions across modules.
- Forbidden for production: older `management-api` with newer
  `management-agent` if contract tests fail.

## Upgrade notes summary

1. Upgrade the full module set in one step when possible.
1. If staged rollout is required:
   - upgrade `management-api` first,
   - then `management-agent`,
   - then `monitoring-console-web`.
1. Validate endpoints and scrapes after upgrade:
   - agent `/api/v1/report`,
   - Prometheus scrape output.
