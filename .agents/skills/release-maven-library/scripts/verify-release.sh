#!/usr/bin/env bash
set -euo pipefail

git status --short --branch
mvn -N install
mvn -pl engine,wal-tools,monitoring-micrometer,monitoring-prometheus,monitoring-rest-json-api,monitoring-rest-json,monitoring-console-web -DskipTests package
mvn -pl wal-tools,monitoring-micrometer,monitoring-prometheus,monitoring-rest-json-api,monitoring-rest-json,monitoring-console-web test
mvn -pl engine test -Dtest=IntegrationSegmentIndexMetricsSnapshotConcurrencyTest
mvn -pl monitoring-prometheus test -Dtest=HestiaStorePrometheusExporterTest
mvn clean verify
