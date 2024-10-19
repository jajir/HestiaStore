# Architecture

Here is described basic index concepts. This page explain you how to correctly configure index.

![basic index architecture](./images/architecture.png)

## Operation consistency

The `getStream()` method can sometimes return inconsistent results, occasionally omitting some items. This can occur in the following scenarios:

* Segment Compaction: If data is being streamed from a segment and new keys are added to that segment during the process, the segment may stop providing additional keys. In this case, the stream operation will either continue with the next segment or terminate if no more segments are available.
* Adding New Keys: If a completely new key is added to the index and is only present in the main index cache, it will not be returned.

To prevent these issues, you should call `compact()` before invoking `getStream()` and ensure no new keys are added during streaming.

Updating values in the index using `put()` or deleting keys using `delete()` does not cause inconsistencies. Updated values will be returned, and deleted keys will be excluded from the stream.

Other operations, like `get()`, remain consistently reliable.