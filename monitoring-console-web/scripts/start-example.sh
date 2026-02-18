#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ROOT_DIR}"

BACKEND_PORT="${BACKEND_PORT:-8085}"
WEB_PORT="${WEB_PORT:-8090}"
WRITE_TOKEN="demo-token"

if lsof -iTCP:"${BACKEND_PORT}" -sTCP:LISTEN >/dev/null 2>&1; then
  echo "Port ${BACKEND_PORT} is already in use. Set BACKEND_PORT to another value."
  exit 1
fi

if lsof -iTCP:"${WEB_PORT}" -sTCP:LISTEN >/dev/null 2>&1; then
  echo "Port ${WEB_PORT} is already in use. Set WEB_PORT to another value."
  exit 1
fi

echo "Starting HestiaStore example (console backend + 3 agents + web UI)..."
echo "Press Ctrl+C to stop."

cleanup() {
  if [[ -n "${DEMO_PID:-}" ]]; then
    kill "${DEMO_PID}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT INT TERM

mvn -q -pl monitoring-console,monitoring-console-web -am -DskipTests -Ddependency-check.skip=true install

mvn -q \
  -pl monitoring-console \
  test-compile \
  -Dexec.classpathScope=test \
  -Dexec.args="${BACKEND_PORT}" \
  -Dexec.mainClass=org.hestiastore.console.MonitoringConsoleDemoMain \
  org.codehaus.mojo:exec-maven-plugin:3.5.0:java &
DEMO_PID=$!

for _ in {1..60}; do
  if curl -sf "http://127.0.0.1:${BACKEND_PORT}/console/v1/dashboard" >/dev/null; then
    break
  fi
  sleep 0.5
done

if ! curl -sf "http://127.0.0.1:${BACKEND_PORT}/console/v1/dashboard" >/dev/null; then
  echo "Backend did not become ready on port ${BACKEND_PORT}."
  exit 1
fi

echo "Backend ready: http://127.0.0.1:${BACKEND_PORT}/console/v1/dashboard"
echo "Starting web UI on http://127.0.0.1:${WEB_PORT}/ ..."

mvn -q \
  -pl monitoring-console-web \
  -Dspring-boot.run.jvmArguments="-Dserver.port=${WEB_PORT} -Dhestia.console.web.backend-base-url=http://127.0.0.1:${BACKEND_PORT} -Dhestia.console.web.write-token=${WRITE_TOKEN}" \
  spring-boot:run
