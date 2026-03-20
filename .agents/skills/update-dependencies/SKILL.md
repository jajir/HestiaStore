---
name: update-dependencies
description: Use when the user wants to update Maven dependencies or plugins in Hestia Store in small, safe batches with clear verification and compatibility notes.
---

# Update Dependencies

Use this skill when the task is to refresh Maven dependencies or plugins in Hestia Store without turning the update into a large migration.

## Rules

- Keep updates batched and reviewable.
- Prefer one dependency family or one risk level per batch.
- Avoid broad multi-major upgrades unless explicitly requested.
- Do not mix dependency updates with unrelated cleanup.
- Stop and report when an update requires product decisions, public API changes, or large compatibility work.

## Workflow

1. Gather the update source.
   - Accept Dependabot PRs, `mvn versions:display-dependency-updates`, security reports, or a user-provided list.
2. Prioritize updates.
   - Prefer security-relevant and build-stability updates first.
   - Prefer patch and minor updates before majors.
   - Group related artifacts together when they must stay aligned.
3. Select a small batch.
   - Default to one library family or one plugin family.
   - Keep the change easy to review and revert.
4. Apply the version changes.
   - Touch only the required `pom.xml` files.
   - Preserve existing repository and module structure.
5. Verify the batch.
   - Run targeted module verification first when appropriate.
   - Run `mvn clean verify` before finishing non-trivial updates.
   - Check that `mvn site` still works before finishing so reporting and site generation remain healthy.
6. Summarize the result.
   - List updated dependencies or plugins.
   - Note compatibility risk.
   - State the verification result.
   - Propose the next update batch if more remain.

## Stop Conditions

- Stop if the update source is unavailable.
- Stop if an update pulls in broad transitive changes that need design-level review.
- Stop if tests fail and the fix is no longer a small dependency update.
