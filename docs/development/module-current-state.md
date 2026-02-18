# Module Current State

This document defines the current naming and package alignment for Maven subprojects.

## Current layout

| Domain | Current module directory | Current artifactId | Current package root |
|---|---|---|---|
| Parent build | `.` (repository root) | `hestiastore-parent` | N/A (parent POM only) |
| Index core | `engine` | `engine` | `org.hestiastore.index.core` |
| Monitoring contracts | `monitoring-api` | `monitoring-api` | `org.hestiastore.monitoring.api` |
| Monitoring exporter (Micrometer) | `monitoring-exporter-micrometer` | `monitoring-exporter-micrometer` | `org.hestiastore.monitoring.exporter.micrometer` |
| Monitoring exporter (Prometheus) | `monitoring-exporter-prometheus` | `monitoring-exporter-prometheus` | `org.hestiastore.monitoring.exporter.prometheus` |
| Management contracts | `management-api` | `management-api` | `org.hestiastore.management.api` |
| Management runtime agent | `management-agent` | `management-agent` | `org.hestiastore.management.agent` |
| Monitoring console backend | `monitoring-console-backend` | `monitoring-console-backend` | `org.hestiastore.monitoring.console.backend` |
| Monitoring console web UI | `monitoring-console-frontend` | `monitoring-console-frontend` | `org.hestiastore.monitoring.console.web` |
