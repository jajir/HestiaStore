#!/usr/bin/env bash
set -euo pipefail

# Functionality:
# - Shared launcher for WAL tooling commands (`verify` and `dump`).
# - Ensures engine classes are compiled before invoking WalTool.
# - Delegates to `org.hestiastore.index.segmentindex.wal.WalTool` with
#   passthrough CLI arguments.

readonly SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
readonly REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
readonly ENGINE_DIR="${REPO_ROOT}/engine"
readonly TOOL_CLASS_FILE="${ENGINE_DIR}/target/classes/org/hestiastore/index/segmentindex/wal/WalTool.class"
readonly TOOL_MAIN_CLASS="org.hestiastore.index.segmentindex.wal.WalTool"

print_usage() {
  cat <<'EOF'
Usage: wal_tool.sh <verify|dump> <walDirectoryPath> [--json]
EOF
}

if [[ $# -lt 2 ]]; then
  print_usage
  exit 1
fi

readonly COMMAND="$1"
shift

if [[ "${COMMAND}" != "verify" && "${COMMAND}" != "dump" ]]; then
  print_usage
  exit 1
fi

if [[ ! -f "${TOOL_CLASS_FILE}" ]]; then
  echo "Compiling engine classes for WalTool..." >&2
  mvn -q -pl engine -DskipTests compile --file "${REPO_ROOT}/pom.xml"
fi

exec java -cp "${ENGINE_DIR}/target/classes" "${TOOL_MAIN_CLASS}" "${COMMAND}" "$@"
