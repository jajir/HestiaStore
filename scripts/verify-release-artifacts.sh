#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

MODULES=(
  "index"
  "monitoring-api"
  "monitoring-micrometer"
  "monitoring-prometheus"
  "management-api"
  "management-agent"
  "monitoring-console"
)

echo "Verifying release artifacts..."
for module in "${MODULES[@]}"; do
  jar_count="$(find "${module}/target" -maxdepth 1 -type f -name "*.jar" | wc -l | tr -d ' ')"
  if [[ "${jar_count}" -eq 0 ]]; then
    echo "ERROR: No jar artifact found in ${module}/target"
    exit 1
  fi
  echo "OK: ${module} (${jar_count} jar files)"
done

echo "All module artifacts are present."
