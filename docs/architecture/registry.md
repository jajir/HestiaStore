# Segment Registry Concurrency

This document describes the segment registry responsibilities and the planned
split workflow. It mirrors the structure and tone of
`docs/architecture/segment-concurrency.md`, but focuses on registry-level
coordination and map updates.

## Scope
- The registry owns:
  - safe access to segment resources (load/create/delete)
  - in-memory segment cache (LRU)
  - registry-level state gate (`READY`, `FREEZE`, `CLOSED`, `ERROR`)
  - segment id allocation for new segments via `SegmentIdAllocator`
- The registry does **not** own split execution, scheduling, or in-flight
  tracking. Those belong to the segment index layer.
- The registry is about safe access to segment resources; it should not manage
  operations *on* those resources (flush/compact/split remain outside).

## Registry Operations

| Operation                 | Description                                                      |
|--------------------------|------------------------------------------------------------------|
| `getSegment(id)`         | Load or return cached segment by id.                             |
| `allocateSegmentId()`    | Allocate a new segment id for split or growth.                   |
| `createSegment()`        | Allocate id and create a new segment (returns segment instance). |
| `deleteSegment(id)`      | Close and delete a segment, then remove from cache.              |
| `close()`                | Close cached segments.                                           |

All registry operations return `SegmentRegistryResult` so callers can react to
`BUSY`/`CLOSED`/`ERROR` states without explicit lock/unlock in normal flows.
Explicit lock/unlock is an internal split safety mechanism, not part of the
public registry API.

### Split Executor (out of scope)

Split scheduling and execution live in the segment index layer. The registry
only provides safe access to segment resources; it does not own executors or
track in-flight split operations.

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
| `READY`  | `FREEZE` | short exclusive window for registry map updates           |
| `FREEZE` | `READY`  | registry map update complete                              |
| any      | `CLOSED` | index closing                                             |
| any      | `ERROR`  | unrecoverable registry failure                            |

### Rules
- `FREEZE` is **short**; it only wraps changes to:
  - key‑to‑segment map updates
  - cache updates tied to the map replacement
- `getSegment()` and `removeSegment()` return `BUSY` if registry is `FREEZE`.
- `CLOSED` and `ERROR` are terminal.

## Split Workflow (Index Layer)

The split workflow is intentionally two‑phase: **prepare** outside the registry
and **apply** under short, targeted locks in the index layer (key‑map lock and
handler lock). Registry involvement is limited to safe segment access and id
allocation.

### Step‑by‑step (proposed)

1) **Lock handler and re‑check eligibility**  
   Acquire the `SegmentHandler` lock for the target segment to block
   non‑privileged access. Re‑check split eligibility under the handler lock.

2) **Open exclusive iterator**  
   Acquire `SegmentIteratorIsolation.FULL_ISOLATION` on the target segment.
   This blocks concurrent writes/flush/compact for the segment and yields a
   stable view.

3) **Split on maintenance executor**  
   Run the split in a background maintenance thread. Create two **new**
   segments (lower + upper) by streaming from the exclusive iterator.

4) **Persist key‑map update**  
   Update the key‑to‑segment map under the map lock:
   - remove the old segment id
   - add the new lower + upper segment ids with their key ranges
   - flush the map to disk

5) **Update registry cache**  
   Update registry state (no directory swap; new ids only):
   - remove the old segment id from the registry cache
   - allow new segment ids to be loaded on demand

6) **Release segment resources**  
   Close the exclusive iterator and release segment locks. Close/free any
   temporary segment instances created for the split.

7) **Delete the old segment from disk**  
   Remove the old segment directory after it is no longer referenced and
   no locks are held.

8) **Unlock and resume**  
   Ensure the registry is back to `READY` and release the handler lock.

### Split Outcome Mapping

| Split Status | Map Update | Directory Action | Cache Update |
|---|---|---|---|
| `SPLIT` | remove old + add lower + add upper | no swap (new ids only) | evict old instance |
| `COMPACTED` | remove old + add lower | no swap (new id only) | evict old instance |

## Locking & Ordering Rules

- **Handler lock first, iterator lock second**  
  The `SegmentHandler` lock is acquired before opening the
  `FULL_ISOLATION` iterator. Eligibility is re‑checked under the handler lock.

- **Key-map lock remains required**  
  The registry `FREEZE` does not replace the key‑to‑segment map’s own
  synchronization. Always use the map’s lock/adapter when mutating or reading
  the on‑disk map, because other index operations may bypass the registry lock.

- **Lock order (apply phase)**  
  During split apply, acquire the handler lock, then the key‑map lock.
  Release in reverse order.

- **No directory swap**  
  New segment ids are always created for split outputs; index data is not
  swapped in place.

- **Directory-backed id allocation**  
  Segment ids are allocated by `SegmentIdAllocator`, which scans the index
  directory for segment roots named `segment-00001` and returns max+1 (or 1
  when none exist). Allocation is not tied to the key-to-segment map.

- **Lock/unlock order is consistent**  
  Acquire locks in a single global order and release in reverse order
  (handler -> key‑map; unlock key‑map -> handler) to avoid deadlocks.

## Failure Handling

### Split failure before apply
- Close iterator and free any temporary segments.
- Registry remains `READY`, map is unchanged.
- Callers see `BUSY` or `ERROR` depending on root cause.

### Split failure during apply
- Registry transitions to `ERROR` if the map update or eviction fails.
- Recovery focuses on restoring a consistent map; orphaned segment directories
  are cleaned up on next open.

### Lock conflicts
- Segment directory lock conflicts return `BUSY` (retryable).

## Invariants

- **Map updates are consistent**: apply phase must fully replace the old
  segment with the new segment ids or transition to `ERROR`.
- **FREEZE is short**: split IO happens outside `FREEZE`; only key‑map
  persistence and cache updates run under `FREEZE`.
- **No hidden compaction**: split does not implicitly run `compact()`.

## Relation to Segment Concurrency

The segment state machine (see `segment-concurrency.md`) guarantees that
`FULL_ISOLATION` blocks writes and maintenance while the split iterator is
open. The registry relies on that exclusivity to build new segments without
concurrent mutation, then applies the map update and cache eviction atomically
under `FREEZE`.
