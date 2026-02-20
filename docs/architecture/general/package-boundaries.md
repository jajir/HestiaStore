# Package Boundaries

This document defines dependency direction between HestiaStore packages so the
core remains lightweight and integration layers can evolve independently.

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

Rules:

- Core must not import:
  - `org.hestiastore.monitoring.*`
  - `org.hestiastore.management.*`
  - `org.hestiastore.console.*`
- Monitoring must use only public core APIs (no package-private internals).
- Management/console REST JSON communication must reuse contracts from
  `org.hestiastore.monitoring.json.api.*` and avoid duplicate DTO definitions.

## Enforcement

The test `PackageDependencyBoundaryTest` enforces that core source files do not
import monitoring, management, or console packages.

This test is intentionally source-level and lightweight so it can run without
additional architecture tooling.
