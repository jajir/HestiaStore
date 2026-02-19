#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ROOT_DIR}"

AGENT_BASE_PORT="${AGENT_BASE_PORT:-9001}"
WEB_PORT="${WEB_PORT:-8090}"
AGENT_REPORT_PATH="${AGENT_REPORT_PATH:-/api/v1/report}"

if lsof -iTCP:"${WEB_PORT}" -sTCP:LISTEN >/dev/null 2>&1; then
  echo "Port ${WEB_PORT} is already in use. Set WEB_PORT to another value."
  exit 1
fi

for port in "${AGENT_BASE_PORT}" "$((AGENT_BASE_PORT + 1))" "$((AGENT_BASE_PORT + 2))"; do
  if lsof -iTCP:"${port}" -sTCP:LISTEN >/dev/null 2>&1; then
    echo "Port ${port} is already in use. Set AGENT_BASE_PORT to another value."
    exit 1
  fi
done

echo "Starting HestiaStore example (3 agents + direct web UI)..."
echo "Press Ctrl+C to stop."

cleanup() {
  if [[ -n "${DEMO_PID:-}" ]]; then
    kill "${DEMO_PID}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT INT TERM

mvn -q -pl monitoring-console-web -am -DskipTests -Ddependency-check.skip=true install

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

mvn -q \
  -pl monitoring-console-web \
  -Dspring-boot.run.jvmArguments="-Dserver.port=${WEB_PORT} -Dhestia.console.web.nodes[0].node-id=node-1 -Dhestia.console.web.nodes[0].node-name=index-1 -Dhestia.console.web.nodes[0].base-url=http://127.0.0.1:${AGENT_BASE_PORT} -Dhestia.console.web.nodes[1].node-id=node-2 -Dhestia.console.web.nodes[1].node-name=index-2 -Dhestia.console.web.nodes[1].base-url=http://127.0.0.1:$((AGENT_BASE_PORT + 1)) -Dhestia.console.web.nodes[2].node-id=node-3 -Dhestia.console.web.nodes[2].node-name=index-3 -Dhestia.console.web.nodes[2].base-url=http://127.0.0.1:$((AGENT_BASE_PORT + 2))" \
  spring-boot:run
