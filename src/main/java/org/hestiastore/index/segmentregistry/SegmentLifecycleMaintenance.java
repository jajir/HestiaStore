package org.hestiastore.index.segmentregistry;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentBuildResult;
import org.hestiastore.index.segment.SegmentBuildStatus;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentResult;
import org.hestiastore.index.segment.SegmentResultStatus;
import org.hestiastore.index.segment.SegmentState;
import org.hestiastore.index.segmentindex.IndexRetryPolicy;

/**
 * Encapsulates direct segment lifecycle operations used by the registry cache.
 * <p>
 * This class is responsible for:
 * <ul>
 * <li>loading a segment from directory state with BUSY retry handling</li>
 * <li>closing a segment with BUSY retry handling</li>
 * </ul>
 *
 * @param <K> key type
 * @param <V> value type
 */
final class SegmentLifecycleMaintenance<K, V> {

    private final SegmentFactory<K, V> segmentFactory;
    private final SegmentRegistryFileSystem fileSystem;
    private final IndexRetryPolicy closeRetryPolicy;
    private final SegmentRegistryStateMachine gate;

    SegmentLifecycleMaintenance(final SegmentFactory<K, V> segmentFactory,
            final SegmentRegistryFileSystem fileSystem,
            final IndexRetryPolicy closeRetryPolicy,
            final SegmentRegistryStateMachine gate) {
        this.segmentFactory = Vldtn.requireNonNull(segmentFactory,
                "segmentFactory");
        this.fileSystem = Vldtn.requireNonNull(fileSystem, "fileSystem");
        this.closeRetryPolicy = Vldtn.requireNonNull(closeRetryPolicy,
                "closeRetryPolicy");
        this.gate = Vldtn.requireNonNull(gate, "gate");
    }

    Segment<K, V> loadSegment(final SegmentId segmentId) {
        Vldtn.requireNonNull(segmentId, "segmentId");
        if (!fileSystem.segmentDirectoryExists(segmentId)) {
            throw new SegmentBusyException(
                    String.format("Segment '%s' was not found.", segmentId));
        }
        final SegmentRegistryState state = gate.getState();
        if (state != SegmentRegistryState.READY) {
            throw new SegmentBusyException("Registry state is " + state);
        }
        final SegmentBuildResult<Segment<K, V>> buildResult = segmentFactory
                .buildSegment(segmentId);
        if (buildResult == null) {
            throw new IndexException(String.format(
                    "Segment '%s' failed to build: null result.", segmentId));
        }
        if (buildResult.getStatus() == SegmentBuildStatus.OK
                && buildResult.getValue() != null) {
            return buildResult.getValue();
        }
        if (buildResult.getStatus() == SegmentBuildStatus.BUSY) {
            throw new SegmentBusyException(
                    String.format("Segment '%s' is busy.", segmentId));
        }
        throw new IndexException(String.format(
                "Segment '%s' failed to build with status '%s'.", segmentId,
                buildResult.getStatus()));
    }

    void closeSegmentIfNeeded(final Segment<K, V> segment) {
        if (segment == null) {
            return;
        }
        final long startNanos = closeRetryPolicy.startNanos();
        while (true) {
            final SegmentState state = segment.getState();
            if (state == SegmentState.CLOSED) {
                return;
            }
            if (state == SegmentState.ERROR) {
                throw new IndexException(
                        String.format("Segment '%s' failed during close: %s",
                                segment.getId(), state));
            }
            final SegmentResult<Void> result = segment.close();
            final SegmentResultStatus status = result.getStatus();
            if (status == SegmentResultStatus.OK) {
                awaitSegmentClosed(segment, startNanos);
                return;
            }
            if (status == SegmentResultStatus.CLOSED) {
                return;
            }
            if (status == SegmentResultStatus.BUSY) {
                closeRetryPolicy.backoffOrThrow(startNanos, "close",
                        segment.getId());
                continue;
            }
            throw new IndexException(
                    String.format("Segment '%s' failed during close: %s",
                            segment.getId(), status));
        }
    }

    private void awaitSegmentClosed(final Segment<K, V> segment,
            final long startNanos) {
        while (true) {
            final SegmentState state = segment.getState();
            if (state == SegmentState.CLOSED) {
                return;
            }
            if (state == SegmentState.ERROR) {
                throw new IndexException(
                        String.format("Segment '%s' failed during close: %s",
                                segment.getId(), state));
            }
            closeRetryPolicy.backoffOrThrow(startNanos, "close",
                    segment.getId());
        }
    }
}
