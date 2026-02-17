# Package Boundaries

This document defines dependency direction between HestiaStore packages so the
core remains lightweight and integration layers can evolve independently.

## Target package roles

- `org.hestiastore.index.*`  
  Core storage/index engine and public data APIs.
- `org.hestiastore.index.monitoring.*`  
  Metrics adapters/exporters (Micrometer, Prometheus, JMX bridge code).
- `org.hestiastore.index.management.api.*`  
  Management DTO/contracts shared by agent and console.
- `org.hestiastore.index.management.agent.*`  
  Node-local management endpoints running in index JVM.
- `org.hestiastore.console.*`  
  Central web console/control-plane UI.

## Allowed dependency direction

```text
core <- monitoring <- management.agent <- console
                   \- management.api shared by agent/console
```

Rules:

- Core must not import:
  - `org.hestiastore.index.monitoring.*`
  - `org.hestiastore.index.management.*`
  - `org.hestiastore.console.*`
- Monitoring must use only public core APIs (no package-private internals).
- Management agent and console should depend on contracts in
  `management.api` and avoid duplicate DTO definitions.

## Enforcement

The test `PackageDependencyBoundaryTest` enforces that core source files do not
import monitoring, management, or console packages.

This test is intentionally source-level and lightweight so it can run without
additional architecture tooling.
