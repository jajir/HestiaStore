package com.coroptis.index.segment;

import java.util.Objects;

import com.coroptis.index.CloseableResource;
import com.coroptis.index.OptimisticLock;
import com.coroptis.index.OptimisticLockObjectVersionProvider;
import com.coroptis.index.datatype.TypeDescriptor;
import com.coroptis.index.directory.Directory;

/**
 * 
 * @author honza
 *
 * @param <K>
 * @param <V>
 */
public class SegmentSearcherManager<K, V> implements CloseableResource {

    private final Directory directory;
    private final SegmentId id;
    private final TypeDescriptor<K> keyTypeDescriptor;
    private final TypeDescriptor<V> valueTypeDescriptor;
    private final int maxNumberOfKeysInIndexPage;
    private final int bloomFilterNumberOfHashFunctions;
    private final int bloomFilterIndexSizeInBytes;
    private final OptimisticLockObjectVersionProvider versionProvider;

    private OptimisticLock lock;
    private SegmentSearcher<K, V> segmentSearcher;

    public SegmentSearcherManager(final Directory directory, final SegmentId id,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final int maxNumberOfKeysInIndexPage,
            final int bloomFilterNumberOfHashFunctions,
            final int bloomFilterIndexSizeInBytes,
            final OptimisticLockObjectVersionProvider versionProvider) {
        this.directory = Objects.requireNonNull(directory);
        this.id = Objects.requireNonNull(id);
        this.keyTypeDescriptor = Objects.requireNonNull(keyTypeDescriptor);
        this.valueTypeDescriptor = Objects.requireNonNull(valueTypeDescriptor);
        this.maxNumberOfKeysInIndexPage = Objects
                .requireNonNull(maxNumberOfKeysInIndexPage);
        this.bloomFilterNumberOfHashFunctions = Objects
                .requireNonNull(bloomFilterNumberOfHashFunctions);
        this.bloomFilterIndexSizeInBytes = Objects
                .requireNonNull(bloomFilterIndexSizeInBytes);
        this.versionProvider = Objects.requireNonNull(versionProvider);
        lock = new OptimisticLock(versionProvider);
    }

    public SegmentSearcher<K, V> getSearcher() {
        if (lock.isLocked()) {
            segmentSearcher = null;
        }
        if (segmentSearcher == null) {
            segmentSearcher = makeSearcher();
            lock = new OptimisticLock(versionProvider);
        }
        return segmentSearcher;
    }

    private SegmentSearcher<K, V> makeSearcher() {
        return new SegmentSearcher<>(directory, id, keyTypeDescriptor,
                valueTypeDescriptor, maxNumberOfKeysInIndexPage,
                bloomFilterNumberOfHashFunctions, bloomFilterIndexSizeInBytes);
    }

    @Override
    public void close() {
        if (segmentSearcher == null) {
            return;
        }
        segmentSearcher.close();
        segmentSearcher = null;
    }

}
