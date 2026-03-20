#!/usr/bin/env bash
set -euo pipefail

usage() {
    cat <<'EOF' >&2
usage: git-worktree-start.sh <branch-type> <topic-slug> [base-branch]

Supported branch types:
  feature | fix | docs | chore | hotfix | release | codex

Examples:
  git-worktree-start.sh feature hs-123-segment-cache
  git-worktree-start.sh docs git-workflow
  git-worktree-start.sh hotfix 0.0.13-npe-on-open
  git-worktree-start.sh release 0.0.13 main
EOF
}

sanitize_slug() {
    printf '%s' "$1" \
        | tr '[:upper:]' '[:lower:]' \
        | sed -E 's/[[:space:]_]+/-/g; s/[^a-z0-9.-]+/-/g; s/-+/-/g; s/^-//; s/-$//'
}

resolve_base_ref() {
    local candidate=$1

    if git show-ref --verify --quiet "refs/remotes/origin/$candidate"; then
        printf 'origin/%s' "$candidate"
        return 0
    fi

    if git show-ref --verify --quiet "refs/heads/$candidate"; then
        printf '%s' "$candidate"
        return 0
    fi

    echo "base branch not found: $candidate" >&2
    exit 1
}

if [ "$#" -lt 2 ] || [ "$#" -gt 3 ]; then
    usage
    exit 1
fi

branch_type=$1
topic_slug=$(sanitize_slug "$2")

if [ -z "$topic_slug" ]; then
    echo "topic slug is empty after sanitization" >&2
    exit 1
fi

case "$branch_type" in
    feature | fix | docs | chore | codex)
        default_base_branch=devel
        ;;
    hotfix | release)
        default_base_branch=main
        ;;
    *)
        echo "unsupported branch type: $branch_type" >&2
        usage
        exit 1
        ;;
esac

base_branch=${3:-$default_base_branch}
branch_name="$branch_type/$topic_slug"

repo_root=$(git rev-parse --show-toplevel)
worktree_root=${HESTIASTORE_WORKTREE_ROOT:-$(cd "$repo_root/.." && pwd)/worktrees}
worktree_leaf=${branch_name//\//-}
worktree_path="$worktree_root/$worktree_leaf"

git fetch origin --prune
base_ref=$(resolve_base_ref "$base_branch")

if git show-ref --verify --quiet "refs/heads/$branch_name"; then
    echo "local branch already exists: $branch_name" >&2
    exit 1
fi

if git ls-remote --exit-code --heads origin "$branch_name" >/dev/null 2>&1; then
    echo "remote branch already exists on origin: $branch_name" >&2
    exit 1
fi

if [ -e "$worktree_path" ]; then
    echo "worktree path already exists: $worktree_path" >&2
    exit 1
fi

mkdir -p "$worktree_root"
git worktree add -b "$branch_name" "$worktree_path" "$base_ref"

cat <<EOF
created worktree
branch: $branch_name
base: $base_ref
path: $worktree_path

next:
  cd "$worktree_path"
  git status --short --branch
EOF
