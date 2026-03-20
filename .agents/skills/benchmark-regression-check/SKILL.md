---
name: benchmark-regression-check
description: Use when benchmark changes, performance-sensitive code, or JMH results in Hestia Store need review to identify likely regressions, measurement noise, and the smallest safe follow-up actions.
---

# Benchmark Regression Check

Use this skill when the task is to assess possible performance regressions in Hestia Store, especially around the `benchmarks` module or engine hot paths.

## Rules

- Do not claim a regression from a single noisy result.
- Prefer comparing like-for-like runs, commands, and environments.
- Focus on explaining signal versus noise before proposing fixes.
- Keep follow-up code changes minimal and justified by evidence.
- The `benchmarks` module has `skipTests=true` by default, so benchmark module tests and Python script smoke tests need `-DskipTests=false` when you expect them to run.

## Workflow

1. Gather the benchmark source.
   - Accept JMH output, benchmark diffs, CI benchmark logs, or a user-provided summary.
2. Identify the affected scope.
   - Note the benchmark names, modules, code paths, and suspected regression area.
3. Validate the evidence.
   - Compare baseline versus changed results.
   - Look for changes in parameters, warmup, measurement counts, or environment that could invalidate the comparison.
4. Inspect the related code path.
   - Focus on engine hot paths, allocations, synchronization, IO patterns, and unnecessary object churn.
5. Decide the next action.
   - If the signal is strong, propose or implement the smallest safe fix.
   - If the signal is weak, recommend a cleaner rerun instead of guessing.
6. Verify and summarize.
   - Re-run the most relevant benchmark command when practical.
   - State confidence level, suspected root cause, and recommended next step.

## Stop Conditions

- Stop if there is no usable baseline or changed result to compare.
- Stop if environment drift makes the benchmark evidence unreliable.
- Stop if the next action requires a broad redesign rather than a targeted investigation.
