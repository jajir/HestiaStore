---
name: sonar-smell-cleanup
description: Use when the user wants to reduce SonarCloud code smells in Hestia Store. Focus on small, safe, behavior-preserving maintainability fixes for this Maven-based Java library. Do not use for security hotspots, vulnerabilities, or major architectural refactors unless explicitly requested.
---

# Sonar Smell Cleanup

Use this skill when the task is to reduce SonarCloud code smells in Hestia Store without changing behavior or expanding scope.

## Rules

- Keep the batch small, safe, and reviewable.
- Treat this as maintainability work, not feature work.
- Prefer behavior-preserving fixes.
- Do not make unrelated refactors.
- Do not change public API behavior unless required and explicitly justified.
- Do not suppress warnings unless a true code fix would be materially worse and the reason is explained.
- Stop if the issue source is missing, the next fixes require architecture changes, or verification fails.

## Branching Guidance

- Default base branch: `devel`.
- Create a dedicated cleanup branch from `devel` for each batch. Prefer names like `codex/engine-sonar-s1192-batch-1` or `codex/sonar-smell-cleanup-<module>-<rule>`.
- Do not continue Sonar cleanup work on unrelated docs, release, or feature branches, even if the working tree is clean.
- Do not commit directly to `devel` unless the user explicitly asks for that workflow.
- Keep one small cleanup batch per branch when practical. If the next batch is unrelated, start a new branch from `devel`.
- Before editing, check `git branch --show-current`, `git status -sb`, and whether the current branch is actually meant for Sonar cleanup.
- If local cleanup edits are already on the wrong branch, stash or commit them, switch to `devel`, create the dedicated branch, and then re-apply the changes there.
- If the current branch is already a dedicated Sonar cleanup branch based on `devel`, continue there instead of creating another branch unnecessarily.

## Workflow

0. Confirm branch placement before changing code.
   - If the current branch is unrelated to Sonar cleanup, move the work to a dedicated branch from `devel` first.
   - If the branch is already dedicated to the same cleanup effort and based on `devel`, continue on it.
1. Gather the current issue source.
   - Accept SonarCloud API output, an exported issue file, or issue text provided by the user.
   - If no issue source is available, stop and ask for it instead of guessing.
2. Filter to code smells only.
   - Exclude bugs, vulnerabilities, and security hotspots unless the user explicitly asked for them.
3. Triage and prioritize the backlog.
   - Prefer issues on new code or files already being touched.
   - Prefer high-confidence, local, behavior-preserving fixes first.
   - Prefer repeated rule keys in one module over scattered one-off issues.
   - Prefer readability and maintainability fixes over cosmetic cleanup.
4. Select a small batch only.
   - Default to 3 to 7 issues.
   - Default to one module and one or two Sonar rule keys.
   - Avoid large cross-cutting edits unless explicitly requested.
5. Before editing, summarize the batch.
   - List the selected issues or rule keys.
   - Explain why this batch was chosen.
   - Name the files expected to change.
6. Apply the fixes.
   - Prefer explicit code improvements over comments.
   - Remove dead code when safe.
   - Simplify conditionals when behavior remains the same.
   - Extract methods or constants only when it clearly improves readability.
   - Replace duplicated logic carefully and only when the resulting diff stays easy to review.
7. Verify the batch.
   - Run targeted verification first when the affected module or tests are obvious.
   - If the batch touches the `benchmarks` module, remember that it sets `skipTests=true` by default; pass `-DskipTests=false` when targeted benchmark tests or script smoke tests are meant to execute.
   - Run `mvn clean verify` before finishing the batch.
   - If verification fails, stop and report the failure instead of widening the cleanup.
8. Summarize the result.
   - List the fixed issues or rule keys.
   - List the files changed.
   - Call out any risks, follow-up items, or intentionally skipped issues.
   - If more issues remain, propose the next small batch.

## Stop Conditions

- Stop if tests fail.
- Stop if the issue source is unavailable.
- Stop if the next fixes require public API changes, concurrency changes, persistence format changes, or architectural rewrites.
- Stop and explain when an issue looks like a false positive or needs a product-level decision.

## Output

- Batch selected
- Changes made
- Verification result
- Remaining recommended next steps
