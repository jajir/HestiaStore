# 🧱 Segment Architecture

This is the central place for segment-specific internals.

## What a Segment Contains

- Main SST file: `vNN-index.sst`
- Sparse (scarce) index: `vNN-scarce.sst`
- Bloom filter: `vNN-bloom-filter.bin`
- Manifest: `manifest.txt`
- Lock file: `.lock`
- Delta cache files: `vNN-delta-NNNN.cache`

## Core Segment Structures

- **Delta cache**: in-memory and on-disk overlay for recent updates.
- **Bloom filter**: fast negative lookup guard before on-disk scans.
- **Sparse/scarce index**: maps sampled keys to chunk positions in main SST.

## Topics

- [Segment Design](segment.md) — segment behavior and implementation notes.
- [On-Disk Layout & File Names](on-disk-layout.md) — naming, directory
  examples, and transactional file writes.
- [Sparse Index](arch-index.md) — sparse/scarce index structure and lookup
  role.
- [Segment Concurrency](segment-concurrency.md) — state machine and
  operation gating.

## Diagrams

- [Segment state machine source](images/segment-state-machine.plantuml)
- [Segment write sequence source](images/segment-writing-seq.txt)
