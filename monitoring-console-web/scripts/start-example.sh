#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ROOT_DIR}"

AGENT_BASE_PORT="${AGENT_BASE_PORT:-9001}"
WEB_PORT="${WEB_PORT:-8090}"
AGENT_REPORT_PATH="${AGENT_REPORT_PATH:-/api/v1/report}"
readonly NODE_COUNT=3

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Required command not found: $1"
    exit 1
  fi
}

check_port_available() {
  local port="$1"
  if lsof -iTCP:"${port}" -sTCP:LISTEN >/dev/null 2>&1; then
    echo "Port ${port} is already in use. Set AGENT_BASE_PORT or WEB_PORT to another value."
    exit 1
  fi
}

for command in curl lsof mvn; do
  require_command "${command}"
done

check_port_available "${WEB_PORT}"
for ((node_index = 0; node_index < NODE_COUNT; node_index += 1)); do
  check_port_available "$((AGENT_BASE_PORT + node_index))"
done

echo "Starting HestiaStore example (3 agents + direct web UI)..."
echo "Press Ctrl+C to stop."

cleanup() {
  if [[ -n "${DEMO_PID:-}" ]]; then
    kill "${DEMO_PID}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT INT TERM

mvn -q -pl monitoring-console-web -am -DskipTests install

mvn -q \
  -pl monitoring-console-web \
  test-compile \
  -Dexec.classpathScope=test \
  -Dexec.args="${AGENT_BASE_PORT}" \
  -Dexec.mainClass=org.hestiastore.console.web.MonitoringConsoleWebDemoMain \
  org.codehaus.mojo:exec-maven-plugin:3.5.0:java &
DEMO_PID=$!

for _ in {1..60}; do
  if curl -sf "http://127.0.0.1:${AGENT_BASE_PORT}${AGENT_REPORT_PATH}" >/dev/null; then
    break
  fi
  sleep 0.5
done

if ! curl -sf "http://127.0.0.1:${AGENT_BASE_PORT}${AGENT_REPORT_PATH}" >/dev/null; then
  echo "Management-agent node did not become ready on port ${AGENT_BASE_PORT}."
  exit 1
fi

echo "Node ready: http://127.0.0.1:${AGENT_BASE_PORT}${AGENT_REPORT_PATH}"
echo "Starting direct web UI on http://127.0.0.1:${WEB_PORT}/ ..."

spring_jvm_args=("-Dserver.port=${WEB_PORT}")
for ((node_index = 0; node_index < NODE_COUNT; node_index += 1)); do
  node_number=$((node_index + 1))
  node_port=$((AGENT_BASE_PORT + node_index))
  spring_jvm_args+=(
    "-Dhestia.console.web.nodes[${node_index}].node-id=node-${node_number}"
    "-Dhestia.console.web.nodes[${node_index}].node-name=index-${node_number}"
    "-Dhestia.console.web.nodes[${node_index}].base-url=http://127.0.0.1:${node_port}"
  )
done

mvn -q \
  -pl monitoring-console-web \
  -Dspring-boot.run.jvmArguments="${spring_jvm_args[*]}" \
  spring-boot:run
