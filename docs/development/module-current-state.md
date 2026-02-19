# Module Current State

This document defines the current naming and package alignment for Maven subprojects.

## Current layout

| Domain | Current module directory | Current artifactId | Current package root |
|---|---|---|---|
| Parent build | `.` (repository root) | `hestiastore-parent` | N/A (parent POM only) |
| Index core | `engine` | `engine` | `org.hestiastore.index` |
| Monitoring contracts | `monitoring-api` | `monitoring-api` | `org.hestiastore.monitoring.api` |
| Monitoring exporter (Micrometer) | `monitoring-micrometer` | `monitoring-micrometer` | `org.hestiastore.monitoring.micrometer` |
| Monitoring exporter (Prometheus) | `monitoring-prometheus` | `monitoring-prometheus` | `org.hestiastore.monitoring.prometheus` |
| Management contracts | `management-api` | `management-api` | `org.hestiastore.management.api` |
| Management runtime agent | `management-agent` | `management-agent` | `org.hestiastore.management.agent` |
| Monitoring console web UI | `monitoring-console-web` | `monitoring-console-web` | `org.hestiastore.console.web` |
