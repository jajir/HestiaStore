#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ROOT_DIR}"

WEB_PORT="${WEB_PORT:-8090}"

usage() {
  echo "Usage: ./monitoring-console-web/scripts/start.sh <monitoring-api-base-url> [monitoring-api-base-url ...]" >&2
  echo "Example: ./monitoring-console-web/scripts/start.sh http://localhost:8091" >&2
  echo "Set WEB_PORT to override the web console port. Default: 8090." >&2
}

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Required command not found: $1" >&2
    exit 1
  fi
}

check_port_available() {
  local port="$1"
  if lsof -iTCP:"${port}" -sTCP:LISTEN >/dev/null 2>&1; then
    echo "Port ${port} is already in use. Set WEB_PORT to another value." >&2
    exit 1
  fi
}

if [[ "$#" -lt 1 ]]; then
  usage
  exit 1
fi

spring_jvm_args=("-Dserver.port=${WEB_PORT}")
endpoints=()
node_index=0
for endpoint in "$@"; do
  if [[ ! "${endpoint}" =~ ^https?:// ]]; then
    echo "Monitoring API endpoint must start with http:// or https://: ${endpoint}" >&2
    exit 1
  fi

  endpoint="${endpoint%/}"
  endpoints+=("${endpoint}")
  node_number=$((node_index + 1))
  spring_jvm_args+=(
    "-Dhestia.console.web.nodes[${node_index}].node-id=node-${node_number}"
    "-Dhestia.console.web.nodes[${node_index}].node-name=node-${node_number}"
    "-Dhestia.console.web.nodes[${node_index}].base-url=${endpoint}"
  )
  node_index=$((node_index + 1))
done

for command in lsof mvn; do
  require_command "${command}"
done

check_port_available "${WEB_PORT}"

echo "Starting HestiaStore monitoring console on http://127.0.0.1:${WEB_PORT}/ ..."
for i in "${!endpoints[@]}"; do
  echo "  node-$((i + 1)): ${endpoints[$i]}"
done

mvn -q -pl monitoring-console-web -am -DskipTests install

exec mvn -q \
  -pl monitoring-console-web \
  -Dspring-boot.run.jvmArguments="${spring_jvm_args[*]}" \
  spring-boot:run
