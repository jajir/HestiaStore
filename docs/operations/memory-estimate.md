---
title: Startup Memory Estimate
audience: operator
doc_type: reference
owner: engine
---

# Startup Memory Estimate

HestiaStore logs a rough startup memory estimate after the effective
`SegmentIndex` configuration is resolved. The report estimates memory pressure
from configuration values; it is not a memory cap and it is not a measurement
of allocated objects.

The estimate is meant to explain why the configured index may need memory. Use
it as a sizing hint, then verify real usage with JVM and application monitoring.

## How to read the report

The report uses fixed-width text tables with three columns:

- `Component`: short name for the calculated area, input, or constant.
- `Estimate`: estimated memory or configured value.
- `Description`: formula, input context, or reason why an estimate is unknown.

Long descriptions wrap onto continuation rows so each report line stays within
the fixed report width. The final estimate appears first, followed by the
calculated memory areas:

- `Total index memory`: all segments, chunk-store page cache, and maintenance
  overhead.
- `All segments`: cached segment memory plus the segment routing map.
- `One cached segment`: per-segment memory blocks that are multiplied by the
  configured segment cache slots.
- `Delta cache`: configured maximum number of keys in the cache multiplied by
  the estimated key/value entry.
- `Chunk-store page cache`: cached chunk-store pages and their key/value
  entries.
- `Maintenance overhead`: temporary headroom for short-lived objects,
  maintenance work, and snapshots.
- `Key/value entry`: key size, value size, and fixed per-entry overhead
  used by key/value cache estimates.
- `Key/position entry`: key size, integer position size, and fixed per-entry
  overhead used by scarce-index estimates.
- `Key/segment-id entry`: key size and segment id size used by the segment
  routing map.
- `Inputs and constants`: configuration values and fixed assumptions used by
  the estimator. Values such as `Write-buffer keys` are reported for context
  and are not added to the total.

Area rows such as `Delta cache`, `Chunk-store page cache`, and `Maintenance
overhead` summarize the formula in the description column. Supporting rows in
`Entry sizes` and `Inputs and constants` show the entry-size assumptions and
configuration values used by those formulas.

## What the report uses

The estimator combines resolved configuration, descriptor size estimates, and a
small set of fixed assumptions:

- `fixed per-entry overhead`: applied when estimating key/value entries and
  key/position entries.
- `page overhead`: applied per chunk-store page-cache page.
- `segment id`: applied with key size when estimating key/segment-id route-map
  entries.
- `scarce index position`: integer position stored with each scarce-index key.
- `fixed per loaded/cached segment`: applied once for each loaded or cached
  segment.
- `maintenance overhead`: extra space above memory before maintenance,
  calculated as the larger of a percentage of memory before maintenance or one
  delta cache.

These assumptions are deliberately visible because they are not exact JVM
object-layout measurements. They are stable estimator constants used to make
the report understandable and repeatable.

## Incomplete estimates

If a key or value `TypeDescriptor` cannot provide
`getEstimatedAverageSizeInBytes()`, HestiaStore still starts normally. The
report marks dependent line items as `unknown` and still shows independent
items such as Bloom filters and fixed segment infrastructure.

## Related docs

- [Performance Tuning](tuning.md)
- [Data Types](../configuration/data-types.md)
