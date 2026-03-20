#!/usr/bin/env bash
set -euo pipefail

# Functionality:
# - Shared launcher for WAL tooling commands (`verify` and `dump`).
# - Uses libraries from the packaged distribution (`../lib/*`).
# - Delegates to `org.hestiastore.index.segmentindex.wal.WalTool` with
#   passthrough CLI arguments.

readonly SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
readonly DIST_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
readonly LIB_DIR="${DIST_ROOT}/lib"
readonly TOOL_MAIN_CLASS="org.hestiastore.index.segmentindex.wal.WalTool"
readonly JAVA_BIN="${JAVA_BIN:-java}"

print_usage() {
  cat <<'USAGE'
Usage: wal_tool.sh <verify|dump> <walDirectoryPath> [--json]
USAGE
  return 0
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

if ! command -v "${JAVA_BIN}" >/dev/null 2>&1; then
  echo "WAL tool failed: Java binary not found: ${JAVA_BIN}" >&2
  exit 1
fi

if [[ ! -d "${LIB_DIR}" ]]; then
  echo "WAL tool failed: Missing distribution lib directory: ${LIB_DIR}" >&2
  exit 1
fi

if ! compgen -G "${LIB_DIR}/engine-*.jar" >/dev/null; then
  echo "WAL tool failed: Missing engine jar in distribution lib directory: ${LIB_DIR}" >&2
  exit 1
fi

readonly CLASSPATH="${LIB_DIR}/*"
exec "${JAVA_BIN}" -cp "${CLASSPATH}" "${TOOL_MAIN_CLASS}" "${COMMAND}" "$@"
