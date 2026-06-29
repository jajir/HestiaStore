---
title: Startup Memory Estimate
audience: operator
doc_type: reference
owner: engine
---

# Startup Memory Estimate

HestiaStore logs a rough startup memory estimate after the effective
`SegmentIndex` configuration is resolved. The report estimates active heap
pressure from configuration values; it is not a JVM heap cap and it is not a
measurement of allocated objects.

The estimate is meant to explain why the configured index may need memory. Use
it as a sizing hint, then verify real usage with JVM and application monitoring.

## How to read the report

The report puts the final estimate first:

- `Estimated active heap`: steady-state memory plus the temporary margin.
- `steady state`: memory expected after startup caches are loaded.
- `temporary margin`: extra headroom for short-lived objects and snapshots.

The `Largest steady-state areas` section shows the biggest calculated areas in
descending order. The `Estimated memory by area` section keeps the detailed
inputs for each area.

Configuration values that are useful context but do not change the estimate are
listed under `Reported but not included`. Conditional areas use `if loaded` in
their labels because those structures are loaded lazily; the estimate shows
their active-heap cost when loaded.

## What the report uses

The estimator combines resolved configuration, descriptor size estimates, and a
small set of fixed assumptions:

- `entry overhead`: applied per cached key/value entry.
- `page overhead`: applied per chunk-store cache page.
- `tree map entry`: applied per route-map tree entry.
- `segment id`: applied when estimating route-map entries.
- `scarce index position`: applied per scarce-index entry.
- `fixed per loaded/cached segment`: applied once for each loaded or cached
  segment.
- `temporary margin`: extra space above steady state, calculated as the
  larger of a percentage of steady state or one loaded segment cache.

These assumptions are deliberately visible because they are not exact JVM
object-layout measurements. They are stable estimator constants used to make
the report understandable and repeatable.

## Incomplete estimates

If a key or value `TypeDescriptor` cannot provide
`getEstimatedAverageSizeInBytes()`, HestiaStore still starts normally. The
report marks dependent line items as unavailable and still shows independent
items such as Bloom filters and fixed segment infrastructure.

## Related docs

- [Performance Tuning](tuning.md)
- [Data Types](../configuration/data-types.md)
