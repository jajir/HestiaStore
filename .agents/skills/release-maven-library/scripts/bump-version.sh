#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 1 ]; then
  echo "usage: $0 <new-version>" >&2
  exit 1
fi

mvn versions:set -DnewVersion="$1" -DgenerateBackupPoms=false -DprocessAllModules=true
