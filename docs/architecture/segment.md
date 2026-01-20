# 🧱 Segment implementation

Segment is core part of index. It represents one string sorted table file with:

* Partial consistency - iterator stop working or return consistent data
* Support Writing changes into delta files
* Bloom filter for faster evaluating if key is in index
* Scarce index for faster searching for data in main index

## 🔄 Segment put/get and iterate consistency

operations like write and get should be always consistent. What is written is read. Iteration behave differently. better than provide old data it stop providing any data.

Let's have a followin key value entries in main index:

```text
<a, 20 >
<b, 30 >
<c, 40 >
```

In segment cache are following entries:

```text
<a, 25>
<e, 28>
<b, tombstone>
```

When user will iterate throught segment data, there will be followin cases:

### Case 1 - Read data

```text
iterator.read() --> <a, 25>
iterator.read() --> <c, 40>
iterator.read() --> <e, 28>
```

### Case 2 - Change data

```text
iterator.read() --> <a, 25>
segment.write(c, 10)
iterator.read() --> null
```

Any segment write operation will break segment iterator. It's easier way to secure segment consistency.  

## 🗄️ Caching of segment data

Segment caching has two parts: in-memory caches for write/delta data and
lazy-loaded disk-backed resources.

In-memory caches:
* `SegmentCache` keeps three views: write cache (new writes), frozen write
  cache (snapshot during flush), and delta cache (in-memory view of on-disk
  delta files).
* On segment creation, `SegmentBuilder#createSegmentCache` calls
  `SegmentDeltaCacheLoader.loadInto`, which reads all delta files and populates
  the delta cache. This is the only eager load.
* During flush, `freezeWriteCache` moves the current write cache into the
  frozen cache, writes it to a delta file, then merges it into the delta cache
  (`mergeFrozenWriteCacheToDeltaCache`).
* Reads consult write → frozen → delta. Iteration merges the index iterator
  with `SegmentCache.getAsSortedList()`.

Lazy-loaded resources:
* `SegmentResourcesImpl` lazily loads and caches the Bloom filter and scarce
  index via `SegmentDataSupplier`. They are created on first access and held in
  `AtomicReference`s.
* `SegmentDeltaCacheController.clear(...)` invalidates these resources when
  delta files are cleared (compaction or replacement) to avoid stale lookups.
* `SegmentReadPath` also caches `SegmentIndexSearcher` for point lookups and
  resets it on maintenance.

## 📁 Segment directory layout

Segment writes all files into the `AsyncDirectory` passed to
`SegmentBuilder`. That directory can point to:

* Index root (flat layout): segment files live next to `index.map`.
* Per-segment directory (segment-root layout): e.g. `segment-00001/` contains
  all files for that segment. File names still include the segment prefix, so
  paths look like `segment-00001/segment-00001.index`.

For segment id `segment-00001` the directory contains:

* `segment-00001.index` - main SST file
* `segment-00001.scarce` - sparse index
* `segment-00001.bloom-filter` - Bloom filter store
* `segment-00001.properties` - segment metadata (active version, delta count)
* `segment-00001.lock` - segment lock file
* `segment-00001-delta-000.cache`, `segment-00001-delta-001.cache`, ... - delta
  cache files (3-digit padded counter)

Versioned layouts use the `-v<version>` marker in file names when
`SegmentPropertiesManager` records a positive active version, e.g.
`segment-00001-v2.index` and `segment-00001-v2-delta-000.cache`. Version `0`
keeps the legacy unversioned names.

## ✍️ Writing to segment

Opening segment writer immediatelly close all segment readers. When writing operation add key that is in index but is not in cache this value will not returned updated. 

Putting new entry into segment is here:

![Segment writing sequence diagram](../images/segment-writing-seq.png)
