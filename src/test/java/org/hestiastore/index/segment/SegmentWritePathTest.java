package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import org.hestiastore.index.Entry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentWritePathTest {

    @Mock
    private SegmentCache<Integer, String> segmentCache;
    @Mock
    private VersionController versionController;
    @Captor
    private ArgumentCaptor<Entry<Integer, String>> entryCaptor;

    private SegmentWritePath<Integer, String> subject;

    @BeforeEach
    void setUp() {
        subject = new SegmentWritePath<>(segmentCache, versionController);
    }

    @Test
    void put_writes_to_cache_without_version_bump() {
        subject.put(1, "a");

        verify(segmentCache).putToWriteCache(entryCaptor.capture());
        assertEquals(Entry.of(1, "a"), entryCaptor.getValue());
        verify(versionController, never()).changeVersion();
    }

    @Test
    void tryPutWithoutWaiting_does_not_bump_version_on_success() {
        when(segmentCache.tryPutToWriteCacheWithoutWaiting(any()))
                .thenReturn(true);

        assertTrue(subject.tryPutWithoutWaiting(1, "a"));

        verify(versionController, never()).changeVersion();
    }

    @Test
    void tryPutWithoutWaiting_does_not_bump_version_on_reject() {
        when(segmentCache.tryPutToWriteCacheWithoutWaiting(any()))
                .thenReturn(false);

        assertFalse(subject.tryPutWithoutWaiting(1, "a"));

        verify(versionController, never()).changeVersion();
    }

    @Test
    void freezeWriteCacheForFlush_returns_snapshot_without_version_change() {
        final List<Entry<Integer, String>> entries = List
                .of(Entry.of(1, "a"));
        when(segmentCache.freezeWriteCache()).thenReturn(entries);

        assertEquals(entries, subject.freezeWriteCacheForFlush());

        verify(versionController, never()).changeVersion();
    }

    @Test
    void applyFrozenWriteCacheAfterFlush_merges_frozen_cache() {
        subject.applyFrozenWriteCacheAfterFlush();

        verify(segmentCache).mergeFrozenWriteCacheToDeltaCache();
    }

    @Test
    void applyFrozenWriteCacheAfterFlush_increments_version_when_frozen() {
        when(segmentCache.hasFrozenWriteCache()).thenReturn(true);

        subject.applyFrozenWriteCacheAfterFlush();

        verify(versionController).changeVersion();
    }

    @Test
    void applyFrozenWriteCacheAfterFlush_skips_version_when_no_frozen() {
        when(segmentCache.hasFrozenWriteCache()).thenReturn(false);

        subject.applyFrozenWriteCacheAfterFlush();

        verify(versionController, never()).changeVersion();
    }
}
