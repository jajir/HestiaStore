#!/usr/bin/env bash
set -euo pipefail

usage() {
    cat <<'EOF' >&2
usage: git-worktree-clean.sh <branch-name>

Examples:
  git-worktree-clean.sh feature/hs-123-segment-cache
  git-worktree-clean.sh hotfix/0.0.13-npe-on-open
EOF
}

is_merged_into() {
    local branch_name=$1
    local target_ref=$2

    if ! git rev-parse --verify --quiet "$target_ref" >/dev/null; then
        return 1
    fi

    git merge-base --is-ancestor "$branch_name" "$target_ref"
}

if [ "$#" -ne 1 ]; then
    usage
    exit 1
fi

branch_name=$1
repo_root=$(cd "$(git rev-parse --show-toplevel)" && pwd -P)
current_dir=$(pwd -P)

git fetch origin --prune >/dev/null 2>&1 || true

worktree_path=$(
    git worktree list --porcelain | awk -v branch_ref="refs/heads/$branch_name" '
        $1 == "worktree" { current_path = $2 }
        $1 == "branch" && $2 == branch_ref { print current_path; exit }
    '
)

if [ -n "${worktree_path:-}" ]; then
    resolved_worktree_path=$(cd "$worktree_path" && pwd -P)
    if [ "$resolved_worktree_path" = "$repo_root" ]; then
        echo "refusing to remove the primary repository checkout: $resolved_worktree_path" >&2
        exit 1
    fi

    case "$current_dir/" in
        "$resolved_worktree_path/"*)
            echo "refusing to remove the current worktree: $resolved_worktree_path" >&2
            exit 1
            ;;
    esac

    if [ "$resolved_worktree_path" = "$current_dir" ]; then
        echo "refusing to remove the current worktree: $resolved_worktree_path" >&2
        exit 1
    fi

    git -C "$repo_root" worktree remove "$worktree_path"
    echo "removed worktree: $worktree_path"
else
    echo "no active worktree found for branch: $branch_name"
fi

if git show-ref --verify --quiet "refs/heads/$branch_name"; then
    if is_merged_into "$branch_name" "origin/devel" \
        || is_merged_into "$branch_name" "devel" \
        || is_merged_into "$branch_name" "origin/main" \
        || is_merged_into "$branch_name" "main"; then
        git branch -d "$branch_name"
        echo "deleted local branch: $branch_name"
    else
        echo "kept local branch because it is not merged into main or devel: $branch_name"
    fi
fi

git worktree prune
