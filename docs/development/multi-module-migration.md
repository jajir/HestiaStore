# Multi-Module Migration Guide

This document describes migration from legacy single-module builds to the
current multi-module layout.

## Target module layout

The repository now builds these artifacts from one parent:

- `org.hestiastore:index`
- `org.hestiastore:monitoring-api`
- `org.hestiastore:monitoring-micrometer`
- `org.hestiastore:monitoring-prometheus`
- `org.hestiastore:management-api`
- `org.hestiastore:management-agent`
- `org.hestiastore:monitoring-console`

## Dependency direction

Use this direction to avoid accidental coupling:

- `index` has no dependency on monitoring/management/console modules.
- `monitoring-*` depend on `index` and `monitoring-api`.
- `management-agent` depends on `index` and `management-api`.
- `monitoring-console` depends on `management-api` and can use
  `management-agent` contracts through HTTP.

## Migration steps

1. Keep Java packages stable while moving files under module-specific
   `src/main/java` and `src/test/java`.
1. Move API contracts used by multiple runtimes into dedicated contract modules
   (`management-api`, `monitoring-api`).
1. Keep runtime adapters (Micrometer/Prometheus/agent/console) out of `index`.
1. Convert root `pom.xml` to parent packaging `pom` and list all modules.
1. Add boundary tests to guard imports from `index` to higher layers.

## Verification commands

Run from repository root:

```bash
mvn -N install
mvn -pl index,monitoring-api,monitoring-micrometer,monitoring-prometheus,management-api,management-agent,monitoring-console -DskipTests package
mvn -pl management-agent,monitoring-console,monitoring-prometheus test
./scripts/verify-release-artifacts.sh
```

## Compatibility rule

- Module versions stay aligned per release line.
- Adding new fields to DTOs/metrics is allowed.
- Renaming/removing existing DTO fields or changing semantics requires a new API
  version.
