package org.hestiastore.index.segmentasync;

import java.util.concurrent.CompletionStage;

import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentResult;

/**
 * Async-capable segment wrapper. Keeps the synchronous Segment contract while
 * exposing asynchronous maintenance operations.
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface SegmentAsync<K, V> extends Segment<K, V> {

    /**
     * Schedule a flush in the background.
     *
     * @return completion stage for the scheduled flush
     */
    CompletionStage<SegmentResult<Void>> flushAsync();

    /**
     * Schedule a compaction in the background.
     *
     * @return completion stage for the scheduled compaction
     */
    CompletionStage<SegmentResult<Void>> compactAsync();
}
