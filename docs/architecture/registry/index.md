# 🗂️ Registry Architecture

This section describes the segment registry as the lifecycle/cache orchestration
layer between SegmentIndex and Segment instances.

## Topics

- [Segment Registry](registry.md) — responsibilities, state machine,
  per-entry model, and operation semantics.

## Diagrams

- [Registry states](images/registry-states.plantuml)
- [Get: cache hit READY](images/registry-seq01.plantuml)
- [Get: cache hit LOADING wait](images/registry-seq02.plantuml)
- [Get: cache miss with `putIfAbsent(LOADING)`](images/registry-seq03.plantuml)
- [Eviction: `removeLastRecentUsedSegment(...)`](images/registry-seq04.plantuml)
