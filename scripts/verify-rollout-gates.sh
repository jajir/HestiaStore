#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

echo "[78.9] Running staged quality gates..."

echo "[Stage A] Core snapshot/concurrency"
mvn -pl index test -Dtest=IntegrationSegmentIndexMetricsSnapshotConcurrencyTest

echo "[Stage B] Monitoring bridge contracts"
mvn -pl monitoring-prometheus test -Dtest=HestiaStorePrometheusExporterTest

echo "[Stage C] Management agent behavior/security"
mvn -pl management-agent test -Dtest=ManagementAgentServerTest,ManagementAgentServerSecurityTest

echo "[Stage D] Console multi-node and failure modes"
mvn -pl monitoring-console test -Dtest=MonitoringConsoleServerTest

echo "[78.9] All rollout gates passed."
