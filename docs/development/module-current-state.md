# Module Current State

This document defines the current naming and package alignment for Maven subprojects.

## Current layout

| Domain | Current module directory | Current artifactId | Current package root |
|---|---|---|---|
| Parent build | `.` (repository root) | `hestiastore-parent` | N/A (parent POM only) |
| Index core | `engine` | `engine` | `org.hestiastore.index` |
| Monitoring and management API contracts | `monitoring-rest-json-api` | `monitoring-rest-json-api` | `org.hestiastore.monitoring.json.api` |
| Monitoring bridge (Micrometer) | `monitoring-micrometer` | `monitoring-micrometer` | `org.hestiastore.monitoring.micrometer` |
| Monitoring bridge (Prometheus) | `monitoring-prometheus` | `monitoring-prometheus` | `org.hestiastore.monitoring.prometheus` |
| Node monitoring/management REST bridge | `monitoring-rest-json` | `monitoring-rest-json` | `org.hestiastore.management.restjson` |
| Monitoring console web UI | `monitoring-console-web` | `monitoring-console-web` | `org.hestiastore.console.web` |
