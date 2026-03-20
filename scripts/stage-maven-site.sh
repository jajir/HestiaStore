#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd -- "${SCRIPT_DIR}/.." && pwd)
ROOT_SITE="${REPO_ROOT}/target/site"
STAGING_DIR="${REPO_ROOT}/target/staging"

cd "${REPO_ROOT}"

if [[ "$#" -gt 0 ]]; then
  printf 'This script stages already-generated Maven sites. Run mvn with extra flags before it.\n' >&2
  exit 1
fi

if [[ ! -f "${ROOT_SITE}/index.html" ]]; then
  printf "Root Maven site not found at %s. Run 'mvn site' first.\n" "${ROOT_SITE}" >&2
  exit 1
fi

mkdir -p "${STAGING_DIR}"
cp -R "${ROOT_SITE}/." "${STAGING_DIR}/"

found_module_site=0
for module_site in "${REPO_ROOT}"/*/target/site; do
  if [[ ! -d "${module_site}" ]]; then
    continue
  fi

  module_relative_path=${module_site#"${REPO_ROOT}/"}
  module_name=${module_relative_path%%/*}
  mkdir -p "${STAGING_DIR}/${module_name}"
  cp -R "${module_site}/." "${STAGING_DIR}/${module_name}/"
  found_module_site=1
done

if [[ "${found_module_site}" -eq 0 ]]; then
  printf "No module Maven sites were found under %s. Run 'mvn site' first.\n" "${REPO_ROOT}" >&2
  exit 1
fi

printf 'Staged Maven site: %s\n' "${STAGING_DIR}/index.html"
