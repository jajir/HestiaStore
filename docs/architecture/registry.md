# Segment Registry Concurrency

This document describes the segment registry responsibilities and the planned
split workflow. It mirrors the structure and tone of
`docs/architecture/segment-concurrency.md`, but focuses on registry-level
coordination, map updates, and directory swaps.

## Scope
- The registry owns:
  - in-memory segment cache (LRU)
  - segment directory swapping
  - the key-to-segment map updates (via the index layer)
  - registry-level state gate (`READY`, `FREEZE`, `CLOSED`, `ERROR`)
- The only structural operation that should reach the registry is **split**.
  Flush/compact belong to the segment package.

## Registry Operations

| Operation          | Description                                                      |
|-------------------|------------------------------------------------------------------|
| `getSegment(id)`  | Load or return cached segment by id.                             |
| `removeSegment(id)` | Close and delete a segment, then remove from cache.            |
| `swapSegmentDirectories(a, b)` | Atomic swap for segment directory replacement.      |
| `close()`         | Close cached segments and executors.                             |

### Response Codes

`SegmentRegistryResultStatus` mirrors segment semantics:

| Code     | Description                                                                 |
|----------|-----------------------------------------------------------------------------|
| `OK`     | Segment returned or operation accepted.                                     |
| `BUSY`   | Temporary refusal (e.g., registry in `FREEZE` or lock conflict).            |
| `CLOSED` | Registry closed; no further operations.                                     |
| `ERROR`  | Unrecoverable registry failure.                                             |

## Registry State Machine

Registry states are intentionally small and short‑lived.

```
READY
  | tryEnterFreeze
  v
FREEZE ----> READY
  | close()         ^
  v                 |
CLOSED <------------+
  |
  v
ERROR
```

### Transitions

| Original State | New State | When                                                     |
|---|---|---|
| `READY`  | `FREEZE` | short exclusive window for map + directory updates        |
| `FREEZE` | `READY`  | map swap + directory swap complete                        |
| any      | `CLOSED` | index closing                                             |
| any      | `ERROR`  | unrecoverable registry failure                            |

### Rules
- `FREEZE` is **short**; it only wraps changes to:
  - key‑to‑segment map updates
  - directory swaps
  - cache updates tied to the swap
- `getSegment()` and `removeSegment()` return `BUSY` if registry is `FREEZE`.
- `CLOSED` and `ERROR` are terminal.

## Planned Split Workflow

The split workflow is intentionally two‑phase: **prepare** outside the registry
and **apply** under a short registry freeze.

### Step‑by‑step (proposed)

1) **Open exclusive iterator**  
   Acquire `SegmentIteratorIsolation.FULL_ISOLATION` on the target segment.
   This blocks concurrent writes/flush/compact for the segment and yields a
   stable view.

2) **Split on maintenance executor**  
   Run the split in a background maintenance thread. Create two **new**
   segments (lower + upper) by streaming from the exclusive iterator.

3) **Return work to registry**  
   The split job returns a `SegmentSplitterResult` with the new segment ids and
   split metadata (min/max keys, outcome).

4) **Release segment resources**  
   Close the exclusive iterator and release segment locks. Close/free any
   temporary segment instances created for the split.

5) **Update map file**  
   Update the key‑to‑segment map:
   - insert the new lower segment
   - update max key for the current segment
   - flush the map to disk

6) **Freeze registry and apply directory swap**  
   Enter `FREEZE` and apply the directory swap atomically:
   - swap current segment directory with the new upper (or replacement) segment
   - relabel swapped files to the target segment id
   - evict old segment instance from cache

7) **Unlock and resume**  
   Exit `FREEZE`, return registry to `READY`, and allow normal operations.

### Split Outcome Mapping

| Split Status | Map Update | Directory Swap | Cache Update |
|---|---|---|---|
| `SPLIT` | insert lower + update current max key | swap current <- upper | evict old instance |
| `COMPACTED` | update current max key only | swap current <- lower | evict old instance |

## Locking & Ordering Rules

- **Segment lock first, registry lock second**  
  The exclusive segment iterator is acquired before registry `FREEZE`.
  This avoids long registry freezes and keeps map updates short.

- **Registry freeze is not held during split IO**  
  The split worker writes new segment files without holding registry `FREEZE`.

- **Single registry freeze per split**  
  The registry only freezes for the apply phase (map + directory swap).

## Failure Handling

### Split failure before apply
- Close iterator and free any temporary segments.
- Registry remains `READY`, map is unchanged.
- Callers see `BUSY` or `ERROR` depending on root cause.

### Split failure during apply
- Registry transitions to `ERROR` if directory swap or map flush fails.
- Recovery relies on `SegmentDirectorySwap` marker file to complete or rollback
  swap on next open.

### Lock conflicts
- Segment directory lock conflicts return `BUSY` (retryable).

## Invariants

- **Map + directory swap are consistent**: apply phase must update both or
  transition to `ERROR`.
- **FREEZE is short**: all IO happens outside `FREEZE`.
- **No hidden compaction**: split does not implicitly run `compact()`.

## Relation to Segment Concurrency

The segment state machine (see `segment-concurrency.md`) guarantees that
`FULL_ISOLATION` blocks writes and maintenance while the split iterator is
open. The registry relies on that exclusivity to build new segments without
concurrent mutation, then applies the map and directory swap atomically under
`FREEZE`.
