package org.hestiastore.index.segment;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import org.hestiastore.index.Commitable;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.properties.IndexPropertiesSchema;
import org.hestiastore.index.properties.PropertyWriter;

/**
 * Staged transaction for segment metadata updates.
 */
public class SegmentPropertiesManagerTx implements Commitable {

    private static final String NUMBER_OF_KEYS_IN_DELTA_CACHE = IndexPropertiesSchema.SegmentKeys.NUMBER_OF_KEYS_IN_DELTA_CACHE;
    private static final String NUMBER_OF_KEYS_IN_MAIN_INDEX = IndexPropertiesSchema.SegmentKeys.NUMBER_OF_KEYS_IN_MAIN_INDEX;
    private static final String NUMBER_OF_KEYS_IN_SCARCE_INDEX = IndexPropertiesSchema.SegmentKeys.NUMBER_OF_KEYS_IN_SCARCE_INDEX;
    private static final String NUMBER_OF_SEGMENT_CACHE_DELTA_FILES = IndexPropertiesSchema.SegmentKeys.NUMBER_OF_SEGMENT_CACHE_DELTA_FILES;
    private static final String SEGMENT_VERSION = IndexPropertiesSchema.SegmentKeys.SEGMENT_VERSION;

    private final SegmentPropertiesManager manager;
    private final List<Consumer<PropertyWriter>> stagedMutations = new ArrayList<>();
    private boolean committed;

    SegmentPropertiesManagerTx(final SegmentPropertiesManager manager) {
        this.manager = Vldtn.requireNonNull(manager, "manager");
    }

    public SegmentPropertiesManagerTx setVersion(final long version) {
        stageMutation(writer -> writer.setLong(SEGMENT_VERSION, version));
        return this;
    }

    public SegmentPropertiesManagerTx setDeltaFileCount(final int count) {
        stageMutation(writer -> writer.setInt(NUMBER_OF_SEGMENT_CACHE_DELTA_FILES,
                count));
        return this;
    }

    public SegmentPropertiesManagerTx setNumberOfKeysInCache(
            final long numberOfKeysInCache) {
        stageMutation(writer -> writer.setLong(NUMBER_OF_KEYS_IN_DELTA_CACHE,
                numberOfKeysInCache));
        return this;
    }

    public SegmentPropertiesManagerTx setNumberOfKeysInIndex(
            final long numberOfKeysInIndex) {
        stageMutation(writer -> writer.setLong(NUMBER_OF_KEYS_IN_MAIN_INDEX,
                numberOfKeysInIndex));
        return this;
    }

    public SegmentPropertiesManagerTx setNumberOfKeysInScarceIndex(
            final long numberOfKeysInScarceIndex) {
        stageMutation(writer -> writer.setLong(NUMBER_OF_KEYS_IN_SCARCE_INDEX,
                numberOfKeysInScarceIndex));
        return this;
    }

    public SegmentPropertiesManagerTx setKeyCounters(
            final long numberOfKeysInCache, final long numberOfKeysInIndex,
            final long numberOfKeysInScarceIndex) {
        stageMutation(writer -> {
            writer.setLong(NUMBER_OF_KEYS_IN_DELTA_CACHE, numberOfKeysInCache);
            writer.setLong(NUMBER_OF_KEYS_IN_MAIN_INDEX, numberOfKeysInIndex);
            writer.setLong(NUMBER_OF_KEYS_IN_SCARCE_INDEX,
                    numberOfKeysInScarceIndex);
        });
        return this;
    }

    @Override
    public void commit() {
        ensureNotCommitted();
        committed = true;
        if (stagedMutations.isEmpty()) {
            return;
        }
        manager.commitTx("segmentPropertiesTx",
                writer -> stagedMutations.forEach(mutation -> mutation.accept(
                        writer)));
    }

    private void stageMutation(final Consumer<PropertyWriter> mutation) {
        ensureNotCommitted();
        stagedMutations.add(Vldtn.requireNonNull(mutation, "mutation"));
    }

    private void ensureNotCommitted() {
        if (committed) {
            throw new IllegalStateException("Transaction already committed");
        }
    }
}
