# Compatibility Matrix

This matrix defines supported same-version combinations for the rollout modules.

## Version alignment policy

All runtime modules are released with the same version number (for example
`0.2.x` for all modules in one release line).

## Supported combinations

| Module | Required same-version modules | Notes |
|---|---|---|
| `engine` | none | Core runtime without monitoring dependencies. |
| `monitoring-rest-json-api` | none | Shared monitoring/management DTO contracts for REST/JSON communication. |
| `monitoring-micrometer` | `engine` | Micrometer bridge from core snapshots. |
| `monitoring-prometheus` | `engine`, `monitoring-micrometer` | Prometheus scrape support. |
| `monitoring-rest-json` | `engine`, `monitoring-rest-json-api` | Node-local management endpoints. |
| `monitoring-console-web` | `engine` | Spring MVC/Thymeleaf operator UI; calls `monitoring-rest-json` over HTTP in direct mode. |

## Runtime compatibility rule

- Supported: all modules on the same version.
- Not guaranteed: mixing different minor versions across modules.
- Forbidden for production: older `monitoring-rest-json-api` with newer
  `monitoring-rest-json` if contract tests fail.

## Upgrade notes summary

1. Upgrade the full module set in one step when possible.
1. If staged rollout is required:
   - upgrade `monitoring-rest-json-api` first,
   - then `monitoring-rest-json`,
   - then `monitoring-console-web`.
1. Validate endpoints and scrapes after upgrade:
   - agent `/api/v1/report`,
   - Prometheus scrape output.
