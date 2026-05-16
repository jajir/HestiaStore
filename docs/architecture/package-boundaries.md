# Package Layout

This document defines dependency direction between HestiaStore packages so the
core remains lightweight and integration layers can evolve independently.

## Current module layout

This page also captures the current module-to-package alignment (previously
documented as "Module Current State").

| Domain                                  | Current module directory   | Current artifactId         | Current package root                    |
| --------------------------------------- | -------------------------- | -------------------------- | --------------------------------------- |
| Parent build                            | `.` (repository root)      | `hestiastore-parent`       | N/A (parent POM only)                   |
| Index core                              | `engine`                   | `engine`                   | `org.hestiastore.index`                 |
| Monitoring and management API contracts | `monitoring-rest-json-api` | `monitoring-rest-json-api` | `org.hestiastore.monitoring.json.api`   |
| Monitoring bridge (Micrometer)          | `monitoring-micrometer`    | `monitoring-micrometer`    | `org.hestiastore.monitoring.micrometer` |
| Monitoring bridge (Prometheus)          | `monitoring-prometheus`    | `monitoring-prometheus`    | `org.hestiastore.monitoring.prometheus` |
| Node monitoring/management REST bridge  | `monitoring-rest-json`     | `monitoring-rest-json`     | `org.hestiastore.management.restjson`   |
| Monitoring console web UI               | `monitoring-console-web`   | `monitoring-console-web`   | `org.hestiastore.console.web`           |

## Target package roles

- `org.hestiastore.index.*`
  Core storage/index engine and public data APIs.
- `org.hestiastore.monitoring.json.api.*`
  Shared monitoring/management REST JSON contracts.
- `org.hestiastore.monitoring.micrometer.*`
  Micrometer integration layer.
- `org.hestiastore.monitoring.prometheus.*`
  Prometheus integration layer.
- `org.hestiastore.management.restjson.*`
  Node-local management endpoints running in index JVM.
- `org.hestiastore.console.web.*`
  Central web console/control-plane UI.

## Allowed dependency direction

```text
index <- monitoring bridges
index + monitoring REST/JSON API contracts <- management REST/JSON bridge
management REST/JSON bridge <- console web (HTTP)
```

## Package dependency diagram

![Package dependency boundaries](images/packages.png)

Source: [packages.plantuml](images/packages.plantuml)

Rules:

- Core must not import:
  - `org.hestiastore.monitoring.*`
  - `org.hestiastore.management.*`
  - `org.hestiastore.console.*`
- Monitoring must use only public core APIs (no package-private internals).
- Management/console REST JSON communication must reuse contracts from
  `org.hestiastore.monitoring.json.api.*` and avoid duplicate DTO definitions.

SegmentIndex internal rules:

- `core.topology` owns runtime route state and must not depend on session or
  segment lease packages.
- `core.segmentlease` is the only package that combines `KeyToSegmentMap`,
  `SegmentTopology`, and `SegmentRegistry` for point-operation leases and
  split drains.
- `core.split` may depend on `SegmentRegistry` for child materialization and
  retired/prepared segment cleanup, but it must not depend on `core.topology`.
  Split code obtains route-drain access through `SegmentLeaseService`.
- `mapping` must not depend on split orchestration packages.

## Enforcement

The `PackageDependencyBoundaryTest` classes enforce these rules with ArchUnit.
The SegmentIndex-specific test includes the `core.split` to `core.topology`
rule so future split changes keep topology/registry coordination inside
`core.segmentlease`.
