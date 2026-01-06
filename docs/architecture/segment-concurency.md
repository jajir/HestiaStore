# Concurrency & Lifecycle Spec

## General principles:
- Single-writer per segment
- Immutable published data
- Background maintenance only (only dedicated background thread performs IO operations)
- When it's not possible to perform some opration merhod immediatelly return segment state, call can be retry.
- locks short as possible
- no io reads or erite is done under the lock

Locks:
- Locks are realized with Segment state machine, some state effectively lock segment
- when it's not possible to perform some operation return code signalize to caller that should try again.
- Segment State BUSY effectively lock further segment operatoins
- No IO or blocking operation under BUSY segmen state
- pokud je write cache přeplněna, pak je volající thread odmitnut s tím, ze volajici process to muze zkusit znovu

All operation are thread safe
- verze segmentu pro optimistic loocking bude AtomicLong
- Map pro write cache musi byt thread safe implementace 
- get()
- increment version
- put() write cache snapshot, snapshot will be threead safe map, when lock is locked than immeditelly return PutResult tryPut(K key, V value);
- flush() & segment() - synchronize segment adn set state to BUSY, because of that one called is refused beacuse segment is BUSY

MVCC (Multi-Version Concurrency Control)
- currently not used,
- could be used in future in iterator, which are now interrupter in case of version increasing
- kontroluji konzistenci, je lepsi data nevracet, než vracet neaktuální

Maintenance thread
- provadi všechny IO read/write operace
- serializuje operace

Supported segment operations:
Result tryPut(K key, V value) - if segment is locked or closed return PutResult immediatelly. Increment version, run in paraell, write cache map is thread safe
GetResult<V> Get(K key) - run in parallel, could be blocked by lock, doesn't require read lock at all
Result flush() & Result compact() - both start background thread that execure disk IO operations
openIterator with option:
- EXCLUSIVE_ACCESS - blocking writes, in this case lock block all writes, probably all attempts to write or read should throw exception
- STOP_FAST - optimistic read, interrupted by any write
- INTERRUPT_FAST - interrupted with exception by any write

### Segment States
Segment is simlpe state machine with followin states:
- READY - accepting all operations put() and get()
- READY_MAINTENANCE - accepting get(), accepting put() up to filling write cache, than backpressure
- BUSY - starting or ending flush() or compact() operation or refusing because running background maintenance and write cache is overloaded. This is also when segment is locked.
- CLOSED - blocking all operations, it's correctly blocked
- ERROS - Something went wring, all api should be blocked

### Call results
When use call segment API object describing return status is returned 
- OK - request was processed
- BUSY - re-try make sense, e.g., write cache full, maintenance exclusive section
- MAINTENANCE - re-try make sense, flush/compact running, not an error by itself
- CLOSED - segment is unavailable forever
- ERROR - segment is unavailable forever

Failure & Cancellation semantics
- in case of flush or compact failure than:
- running thread is interrupted
- all operations are finished
- when it's possible finished with exceptions 

A co je v každém stavu povoleno.

Invariants
- 


Classes and their responsibility

Segment - client facing API 
- exposing methods put(), get(), openIterator(), flush(), compact()
- delegate calls to other clases

SegmentCore
- internal class that write put() to write cache
- musi umet backpressure
- Owns Data:
  - index data
  - scarce index
  - bloom filter
  - delta cache
  - write cache
- holds read/write locks
- holds segment version, which is increased in each call of put()

MaintennaceController
- Specialni threadExecutor poskytuje thread na flush a compact které jsou provedeni ve třídě SegmentWriter

SegmentWriter
- perform flush and compact operation, both will accepts snapshot of write cache
- this operation are serialized to one thread
- when each operation is done, proper method on SegmentCore with lock is called

MVCC (Multi-Version Concurrency Control)
endlessly trade deadlocks vs perf vs memory
