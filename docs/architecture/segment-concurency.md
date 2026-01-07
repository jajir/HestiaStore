# Concurrency & Lifecycle

## Overview
Segments use a single writer and multiple readers. Published data stays immutable, while a dedicated maintenance thread performs all disk IO so application threads remain fast.

## Core Rules
- One writer per segment; readers never change published data.
- Keep locks short and avoid IO while holding them.
- If an operation cannot proceed, return immediately with a status so the caller can retry.
- Only the maintenance thread performs IO in the background.
- `flush()` and `compact()` are commit points

## Locking Model
- The segment state machine coordinates admission of operations.
- When segment is locked it's in state `FREEZE`, `CLOSED` or `ERROR`.
- No read/write IO operation run directly from segment caller thread.

## Thread Safety
- Segment version uses an `AtomicLong` for optimistic reads.
- The write cache map is thread-safe map.
- Segment State Machine transitions should be thread safe

## Segment Operations
- `Result put(K key, V value)`: parallel-safe; fails fast if locked or closed. Do not increments the version, writes to the cache snapshot; if locked, it returns immediately this information.
- `GetResult<V> get(K key)`: parallel-safe; no read lock required.
- `Result flush()` / `Result compact()`: start background IO jobs. For start it set segment state to `FREEZE`. When maintenance thread start it's work than segment state is set to `MAINTENANCE_RUNNING`. When it's done segment state is set to `FREEZE` and new delta cache or new index files are renamed to main one. Finally segmen state is `READY`. This two methods can't run in paraell. Concurrent request will be refused with `BUSY.`
- `openIterator(options)`: allowed only in `READY`. During `FREEZE` or `MAINTENANCE_RUNNING` it returns `BUSY`. Iterator is AutoCloseable it have to be closed manually.
  - `INTERRUPT_FAST`: default value, optimistic read; throw exception on flush() of compact().
  - `STOP_FAST`: optimistic read; silently stop reading on flush() of compact(). Potentionally dangerous function, user is now aware that doesn't work with all data.
  - `EXCLUSIVE_ACCESS`: is a stop-the-world maintenance mode. I should be as fast as possible. It block all other operations. Shouldn't be started by user. This operation set segment status to `FREEZE` util iterator is closed.

## Segment Version
The segment version is a monotonic epoch counter.
It is incremented immediatelly after followin is called:
- publication of a new immutable view (`flush()` / `compact()`)
- entering EXCLUSIVE_ACCESS maintenance mode

Both events invalidate optimistic iterators.

## Segment Version and Iterator Impact
| Operation              | Increments Version? | Effect on Iterators                                     |
|------------------------|----------------------|----------------------------------------------------------|
| `put`.                 | No                   | No direct effect; iterators continue unless a writer advances the version. |
| `get`                  | No                   | No direct effect; iterators continue unless a writer advances the version. |
| `flush`                | Yes                  | Yes. Any active iterator that watches version is interrupted.. |
| `compact`              | Yes                  | Yes. Any active iterator that watches version is interrupted. |
| `openIterator` with `EXCLUSIVE_ACCESS` | Yes | In this case is important that iterator become availabe as soon as possible, because called will perform maintenance opration. Other iterator are invalidated. |
| `openIterator`         | No                   | Chooses iterator behavior: `EXCLUSIVE_ACCESS` blocks writers; `STOP_FAST` or `INTERRUPT_FAST` will stop/throw on any version change. |

## Segment States
Segmen thave to in one from the following states:

- `READY`: accepts `put()` and `get()`.
- `MAINTENANCE_RUNNING`: Segment is in this state when operatio `flush()` / `compact()` is running. When write cache is during maintenance overloaded than value from all methods should be `BUSY` indicating temporary refusal of specific operations, not a global stop of all segment activity.
- `FREEZE` : Indicate that all segment operation will be refused until some short operation is performed. It will be used when segment lock write cache because of creating snapshot for `flush()` of during switching to new index files ofter `compact()`. In this state are all operations refused with `BUSY` return code.
- `CLOSED`: blocks all operations.
- `ERROR`: unrecoverable; all API calls are blocked.

### Which operation are allowed in which state

Following table shows which segment operations ara allowwed in wchich segment state. When operation is not allowed segment reponse with code `BUSY`, `CLOSED` or `ERROR`.

| Segment state                  | Possible operations  |
|--------------------------------|--------------------------|
| `READY`                        | `get`, `put`, `openIterator`, `flush`, `compact` |
| `FREEZE`                       | none                   |
| `MAINTENANCE_RUNNING`          | `get`, `put` until write cache is not over bloated  |
| `CLOSED`                       | none                 |
| `ERROR`                        | none                  |


### Segment State Machine Transitions

| Original State                  | New State        | When 
|--------------------------------|--------------------------|----|
| `READY`                        | `FREEZE`                 | start ot `flush()`, `compact()` or `openIterator` with `EXCLUSIVE_ACCESS` |
| `FREEZE`                       | `MAINTENANCE_RUNNING` | Maintenance thread starts `flush()`, `compact()` |
| `MAINTENANCE_RUNNING`          | `FREEZE`               | Maintenance thread finished `flush()`, `compact()` and switching to new ndex data started |
| `FREEZE`                       | `READY`                 | Switching to new data is done or `openIterator` with `EXCLUSIVE_ACCESS` is closed |
| any state                        | `ERROR`                  | Some index problem occured       |
| `READY`                        | `CLOSED`                  | calling of `close()`       |


### Mapping Segment States to response codes

| Segment state                  | Possible response codes  |
|--------------------------------|--------------------------|
| `READY`                        | `OK`                     |
| `FREEZE`                       | `BUSY`                   |
| `MAINTENANCE_RUNNING`          | `OK`, `BUSY`             |
| `CLOSED`                       | `CLOSED`                 |
| `ERROR`                        | `ERROR`                  |


## Call Results
- `OK`: processed successfully.
- `BUSY`: Operations may be refused with a BUSY status (e.g. backpressure or exclusive access). This does not imply a global stop of segment activity. retry is reasonable (write cache is full during `flush()` or `compact()`, exclusive section).
- `CLOSED`: segment is permanently unavailable.
- `ERROR`: segment is permanently unavailable.

## Publish Model

Poblished data view consist from:
- Immutable view object - main index, delta cache, bloom filter and scarce index
- Muttable objects - write cache

Table what data are available for which operation:

| Operation                              | Read from Write Cache | Read from Delta Cache |
|----------------------------------------|-----------------------|-----------------------|
| `get`                                  | Yes                   | Yes                   |
| `openIterator` with `STOP_FAST`        | No                    | Yes                   |
| `openIterator` with `EXCLUSIVE_ACCESS`*| Yes                   | Yes                   |
| `openIterator` with `INTERRUPT_FAST`   | No                    | Yes                   |

*) Reading from the write cache during EXCLUSIVE_ACCESS is safe because all concurrent writes and reads are blocked.

## Failure & Cancellation
- On `flush` or `compact` disk read/write IO operations are performed in separate maintenance thread. In case of failure segment move state from `MAINTENANCE_RUNNING` to `ERROR`.

## Components
- **Segment**: user-facing API (`put`, `get`, `openIterator`, `flush`, `compact`); delegates to internals.
- **SegmentCore**: writes to the write cache, owns indexes, bloom filter, delta cache, and segment version and segment status
- **MaintenanceController**: executor that schedules `flush` and `compact` work for `SegmentWriter`.
- **SegmentWriter**: performs serialized `flush`/`compact` using snapshots; calls back into `SegmentCore` and changing it's status.

## Future: MVCC
Currently unused. MVCC could support iterators that remain consistent across version changes, balancing deadlocks, performance, and memory.
