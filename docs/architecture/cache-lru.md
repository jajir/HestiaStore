# Cache LRU

This page describes the `CacheLru` API and the `CacheLruImpl` implementation,
including the compromise between throughput and strict LRU ordering. The goal
is to keep access hot paths lock-free while still bounding memory and evicting
least-recently-used entries approximately.

## Purpose

`CacheLru` provides a bounded in-memory cache with an approximate LRU eviction
policy. Each cached entry stores a monotonic access counter; the smallest
counter is treated as the least recently used entry during eviction.

Code: `src/main/java/org/hestiastore/index/cache/CacheLru.java`,
`src/main/java/org/hestiastore/index/cache/CacheLruImpl.java`,
`src/main/java/org/hestiastore/index/cache/CacheElement.java`.

## Thread-safety compromise

The implementation favors throughput over strict LRU accuracy:

- Reads and writes are lock-free for normal access.
- Eviction is serialized by a single lock when the size limit is exceeded.
- Recency updates use an atomic counter; per-entry counters are unsynchronized.

This keeps common operations fast while still maintaining a bounded cache.

The selected model is eviction-only locking: normal access never blocks, and
only eviction scans/removals are serialized. This avoids contention on reads
and writes at the cost of approximate LRU ordering under high concurrency.

## Supported operations

| Operation | Behavior | Thread-safety notes |
| --- | --- | --- |
| `put(key, value)` | Inserts or overwrites a value and may trigger eviction. | Lock-free write; eviction takes the eviction lock and scans the map. |
| `putNull(key)` | Inserts a null marker to short-circuit future lookups. | Lock-free write; eviction behavior same as `put`. |
| `get(key)` | Returns an `Optional` and updates recency on hit. | Lock-free read; access counter is atomic and entry counters are best-effort. |
| `ivalidate(key)` | Removes a single entry and notifies the eviction listener. | Lock-free remove; callback runs immediately. |
| `invalidateAll()` | Clears all entries and notifies the listener for each value entry. | Iterates concurrently; callbacks run during the traversal. |

### Data structures

- `ConcurrentHashMap<K, CacheElement<V>>` for concurrent access.
- `AtomicLong accessCx` for a monotonic access counter.
- `long cx` in each `CacheElement` as an unsynchronized access marker.
- `evictionLock` to serialize eviction scans and removals.

### Access flow

- `get`: read entry, update `cx` with the next counter.
- `put` / `putNull`: write entry, then trigger eviction if needed.

### Eviction flow

- If `cache.size() > limit`, acquire `evictionLock`.
- Scan entries to find the minimum `cx` (oldest).
- Remove using `remove(key, value)` to avoid racing with concurrent updates.
- Notify the eviction listener outside the lock.

Because eviction is serialized and removal is conditional, a key is evicted at
most once per insertion (unless it is reinserted). Eviction callbacks can run
concurrently for different keys, but the same key is not evicted twice.

## Trade-offs

- Approximate LRU: concurrent updates can reorder access counters, and entry
  counters are unsynchronized, so eviction is best-effort under contention.
- Eviction cost: each eviction is O(n) over the current map.
- Temporary overshoot: concurrent puts can exceed the limit until eviction runs.
- No pinning: eviction does not skip special entries; use higher-level guards if
  some entries must never be evicted while in use.

## When to use

Use `CacheLru` when you need high throughput and can tolerate approximate LRU
ordering. If strict LRU or pinned entries are required, prefer a different
cache structure or wrap `CacheLru` with additional coordination.
