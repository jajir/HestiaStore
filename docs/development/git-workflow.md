---
title: Git and Worktree Workflow
audience: contributor
doc_type: how-to
owner: docs
---

# Git and Worktree Workflow

This page defines the daily git standard for contributors and maintainers. Use
it to start task worktrees, target pull requests, and clean up branches without
mixing unrelated changes.

## Branch roles

1. `main` stays release-ready. Use it for release work and urgent hotfixes
   only.
2. `devel` is the default integration base for normal feature, fix, docs, and
   chore work.
3. Topic branches are short-lived and map to exactly one pull request.
4. Release work is the main exception. Prepare it in a clean `main` worktree
   and follow [Release Process](release.md).

## Pull request targeting

1. Start normal work from `devel` and open the PR back to `devel`.
2. Start hotfix work from `main` and open the PR back to `main`.
3. After a hotfix lands on `main`, sync it back into `devel` before continuing
   normal development.
4. Do not open the same branch as separate PRs to both `devel` and `main`.
5. Promote accumulated work to `main` through the release flow instead of
   reviewing the same change twice.

## Branch naming

Use lowercase, hyphenated branch names that describe one concern. Prefer an
issue identifier first when one exists.

- `feature/hs-123-segment-cache`
- `fix/hs-141-release-tag-cleanup`
- `docs/git-workflow`
- `chore/dependency-check-tuning`
- `hotfix/0.0.13-npe-on-open`
- `release/0.0.13`
- `codex/segment-metrics-cleanup` for Codex-created branches when automation
  owns the branch

## Worktree standard

1. Treat the repository root as a control checkout. Keep it clean and use it
   for coordination, not long-running edits.
2. Do active development in dedicated worktrees under a single sibling
   directory such as `../worktrees`.
3. Use one worktree per active task, one branch per worktree, and one PR per
   branch.
4. Do not switch an existing task worktree to a different branch.
5. Do not develop in a detached `HEAD` worktree.
6. Remove merged worktrees and delete merged local branches as soon as the PR
   is finished.

## Recommended helper commands

The repository includes helper scripts:

- `scripts/git-worktree-start.sh`
- `scripts/git-worktree-clean.sh`

It also includes a reusable git alias snippet at
`config/git/aliases.gitconfig`.

Install the aliases once:

```bash
git config --global include.path /absolute/path/to/HestiaStore/config/git/aliases.gitconfig
```

The helpers use `../worktrees` by default. Override that location with
`HESTIASTORE_WORKTREE_ROOT` if you want a different parent directory.

## Daily flows

### Start a normal task from `devel`

```bash
git wt-start feature hs-123-segment-cache
```

This fetches the latest refs, creates `feature/hs-123-segment-cache` from
`origin/devel`, and creates a worktree at
`../worktrees/feature-hs-123-segment-cache`.

### Start a hotfix from `main`

```bash
git wt-hotfix 0.0.13-npe-on-open
```

This creates `hotfix/0.0.13-npe-on-open` from `origin/main`.

### Start a docs or chore task

```bash
git wt-start docs git-workflow
git wt-start chore build-cache-tuning
```

### Clean up after merge

```bash
git wt-clean feature/hs-123-segment-cache
```

The cleanup helper removes the matching worktree, prunes stale worktree
metadata, and deletes the local branch when it is already merged into `main`,
`devel`, `origin/main`, or `origin/devel`.

## Rules that prevent merge noise

1. Never start a new task by committing on whatever branch is currently open.
2. Never mix unrelated fixes into an existing task branch because it is already
   checked out.
3. If a second task appears while the first is in review, create a second
   worktree instead of rebasing both tasks together.
4. If a task must change target from `devel` to `main`, create a new hotfix
   branch from `main` and cherry-pick the minimal commits instead of retargeting
   a large mixed branch.
5. Before opening the PR, verify that `git diff --stat origin/<target>...HEAD`
   shows only the intended concern.

## Related docs

- [Release Process](release.md)
- [Contributing](../community/contributing.md)
- [Developer Guides](guides.md)
