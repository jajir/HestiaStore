package org.hestiastore.index.segmentindex.core.execution;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;

import org.hestiastore.index.BusyRetryPolicy;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.OperationResult;
import org.hestiastore.index.OperationStatus;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentState;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexMaintenanceConfiguration;
import org.hestiastore.index.segmentindex.core.split.SplitRuntime;
import org.hestiastore.index.segmentindex.core.storage.StorageCoordinator;
import org.hestiastore.index.segmentindex.core.storage.WalCheckpointMaintenance;
import org.hestiastore.index.segmentindex.routemap.SegmentRouteMap;
import org.hestiastore.index.segmentregistry.BlockingSegment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Owns foreground flush and compaction orchestration across mapped stable
 * segments.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class MappedSegmentMaintenanceService<K, V>
        implements WalCheckpointMaintenance {

    private static final Logger LOGGER = LoggerFactory
            .getLogger(MappedSegmentMaintenanceService.class);

    private final SegmentRouteMap<K> keyToSegmentMap;
    private final NonBlockingSegmentOperationGateway<K, V> stableSegmentGateway;
    private final SplitRuntime<K, V> splitService;
    private final BusyRetryPolicy retryPolicy;
    private final MaintenanceStatsRecorder statsRecorder;
    private final ExecutorService maintenanceExecutor;
    private final StorageCoordinator<K, V> storageService;
    private final LongSupplier nanoTimeSupplier;
    private final AtomicBoolean asyncMaintenanceClosed = new AtomicBoolean(false);
    private final AtomicInteger asyncMaintenanceInFlight = new AtomicInteger();

    MappedSegmentMaintenanceService(
            final SegmentRouteMap<K> keyToSegmentMap,
            final NonBlockingSegmentOperationGateway<K, V> stableSegmentGateway,
            final SplitRuntime<K, V> splitService,
            final BusyRetryPolicy retryPolicy,
            final MaintenanceStatsRecorder statsRecorder,
            final ExecutorService maintenanceExecutor,
            final StorageCoordinator<K, V> storageService,
            final LongSupplier nanoTimeSupplier) {
        this.keyToSegmentMap = Vldtn.requireNonNull(keyToSegmentMap,
                "keyToSegmentMap");
        this.stableSegmentGateway = Vldtn.requireNonNull(stableSegmentGateway,
                "stableSegmentGateway");
        this.splitService = Vldtn.requireNonNull(splitService, "splitService");
        this.retryPolicy = Vldtn.requireNonNull(retryPolicy, "retryPolicy");
        this.statsRecorder = Vldtn.requireNonNull(statsRecorder,
                "statsRecorder");
        this.maintenanceExecutor = Vldtn.requireNonNull(maintenanceExecutor,
                "maintenanceExecutor");
        this.storageService = Vldtn.requireNonNull(storageService,
                "storageService");
        this.nanoTimeSupplier = Vldtn.requireNonNull(nanoTimeSupplier,
                "nanoTimeSupplier");
    }

    /**
     * Creates a maintenance service from initialized runtime collaborators.
     *
     * @param <M>                  key type
     * @param <N>                  value type
     * @param keyToSegmentMap      route map scanned for maintenance
     * @param stableSegmentGateway stable segment operation gateway
     * @param splitService         split runtime service
     * @param maintenance          maintenance retry configuration
     * @param statsRecorder        maintenance telemetry recorder
     * @param maintenanceExecutor  executor used by asynchronous maintenance
     * @param storageService       storage service checkpointed after settled work
     * @return maintenance service
     */
    public static <M, N> MappedSegmentMaintenanceService<M, N> create(
            final SegmentRouteMap<M> keyToSegmentMap,
            final NonBlockingSegmentOperationGateway<M, N> stableSegmentGateway,
            final SplitRuntime<M, N> splitService,
            final EffectiveIndexMaintenanceConfiguration maintenance,
            final MaintenanceStatsRecorder statsRecorder,
            final ExecutorService maintenanceExecutor,
            final StorageCoordinator<M, N> storageService) {
        final EffectiveIndexMaintenanceConfiguration validatedMaintenance = Vldtn
                .requireNonNull(maintenance, "maintenance");
        return new MappedSegmentMaintenanceService<>(keyToSegmentMap, stableSegmentGateway,
                splitService,
                new BusyRetryPolicy(validatedMaintenance.busyBackoffMillis(),
                        validatedMaintenance.busyTimeoutMillis(),
                        "Maintenance operation"),
                statsRecorder, maintenanceExecutor, storageService,
                System::nanoTime);
    }

    /**
     * Schedules asynchronous compaction for mapped segments.
     */
    public void compact() {
        executeCompactAsync();
    }

    /**
     * Compacts mapped segments and waits until durable maintenance is settled.
     */
    public void compactAndWait() {
        splitService.awaitQuiescence();
        compactMappedSegmentsAndFlush();
        finalizeSettledCompaction();
    }

    /**
     * Schedules asynchronous flush for mapped segments and route metadata.
     */
    public void flush() {
        executeFlushAsync();
    }

    /**
     * Flushes mapped segments and waits until durable maintenance is settled.
     */
    @Override
    public void flushAndWait() {
        splitService.awaitQuiescence();
        flushMappedSegmentsAndWait();
        finalizeSettledFlush();
    }

    /**
     * Seals asynchronous maintenance submission and waits for accepted
     * asynchronous work to finish.
     */
    public void sealAsyncMaintenanceAndWait() {
        asyncMaintenanceClosed.set(true);
        final long startNanos = retryPolicy.startNanos();
        while (asyncMaintenanceInFlight.get() > 0) {
            retryPolicy.backoffOrThrow(startNanos, "asyncMaintenanceClose",
                    asyncMaintenanceInFlight.get());
        }
    }

    void flushSegments(final boolean waitForCompletion) {
        for (final SegmentId segmentId : keyToSegmentMap.getSegmentIds()) {
            flushSegment(segmentId, waitForCompletion);
        }
    }

    void flushMappedSegmentsAndWait() {
        flushSegments(true);
    }

    void compactMappedSegmentsAndFlush() {
        for (final SegmentId segmentId : keyToSegmentMap.getSegmentIds()) {
            compactSegment(segmentId, true);
        }
        flushSegments(true);
    }

    private void compactMappedSegments() {
        for (final SegmentId segmentId : keyToSegmentMap.getSegmentIds()) {
            compactSegment(segmentId, false);
        }
    }

    private void flushMappedSegmentsAndRoutes() {
        flushSegments(false);
        keyToSegmentMap.flushIfDirty();
    }

    void compactSegment(final SegmentId segmentId,
            final boolean waitForCompletion) {
        runSegmentMaintenance(MaintenanceOperation.COMPACT, segmentId,
                waitForCompletion);
    }

    void flushSegment(final SegmentId segmentId,
            final boolean waitForCompletion) {
        runSegmentMaintenance(MaintenanceOperation.FLUSH, segmentId,
                waitForCompletion);
    }

    private void runSegmentMaintenance(final MaintenanceOperation operation,
            final SegmentId segmentId, final boolean waitForCompletion) {
        recordRequest(operation);
        LOGGER.debug("{} attempt started: segment='{}' wait='{}'",
                operation.label(), segmentId, waitForCompletion);
        final long startNanos = retryPolicy.startNanos();
        while (true) {
            final OperationResult<BlockingSegment<K, V>> result = runGatewayOperation(operation, segmentId);
            final OperationStatus status = result.getStatus();
            if (status == OperationStatus.OK) {
                completeOperation(operation, segmentId, waitForCompletion,
                        result.getValue());
                return;
            }
            if (status == OperationStatus.CLOSED) {
                logOperation(
                        "{} skipped because segment is closed: segment='{}'",
                        operation.label(), segmentId);
                return;
            }
            if (status == OperationStatus.BUSY) {
                if (handleBusyOperation(segmentId, waitForCompletion,
                        operation.label())) {
                    return;
                }
                recordBusyRetry(operation);
                retryPolicy.backoffOrThrow(startNanos, operation.id(), segmentId);
                continue;
            }
            if (shouldIgnoreUnmappedError(status, segmentId,
                    operation.label())) {
                return;
            }
            throw newIndexException(operation.id(), segmentId, status);
        }
    }

    private void completeOperation(final MaintenanceOperation operation,
            final SegmentId segmentId, final boolean waitForCompletion,
            final BlockingSegment<K, V> segment) {
        final long latencyNanos = completeAcceptedOperation(segmentId,
                waitForCompletion, operation.id(), operation.label(), segment);
        if (latencyNanos >= 0L) {
            recordAcceptedToReadyNanos(operation, latencyNanos);
        }
    }

    private OperationResult<BlockingSegment<K, V>> runGatewayOperation(
            final MaintenanceOperation operation, final SegmentId segmentId) {
        if (operation == MaintenanceOperation.COMPACT) {
            return stableSegmentGateway.compact(segmentId);
        }
        return stableSegmentGateway.flush(segmentId);
    }

    private void recordRequest(final MaintenanceOperation operation) {
        if (operation == MaintenanceOperation.COMPACT) {
            statsRecorder.recordCompactRequest();
        } else {
            statsRecorder.recordFlushRequest();
        }
    }

    private void recordBusyRetry(final MaintenanceOperation operation) {
        if (operation == MaintenanceOperation.COMPACT) {
            statsRecorder.recordCompactBusyRetry();
        } else {
            statsRecorder.recordFlushBusyRetry();
        }
    }

    private void recordAcceptedToReadyNanos(
            final MaintenanceOperation operation, final long latencyNanos) {
        if (operation == MaintenanceOperation.COMPACT) {
            statsRecorder.recordCompactAcceptedToReadyNanos(latencyNanos);
        } else {
            statsRecorder.recordFlushAcceptedToReadyNanos(latencyNanos);
        }
    }

    private long completeAcceptedOperation(final SegmentId segmentId,
            final boolean waitForCompletion, final String operationId,
            final String operationLabel, final BlockingSegment<K, V> segment) {
        LOGGER.debug("{} attempt accepted: segment='{}' wait='{}'",
                operationLabel, segmentId, waitForCompletion);
        logOperation("{} accepted: segment='{}' wait='{}' state='{}'",
                operationLabel, segmentId, waitForCompletion,
                segment == null ? null : segment.getRuntime().getState());
        long latencyNanos = -1L;
        if (waitForCompletion && segment != null) {
            latencyNanos = awaitAcceptedToReadyNanos(segmentId, operationId,
                    segment);
        }
        logOperation("{} completed: segment='{}' wait='{}'",
                operationLabel, segmentId, waitForCompletion);
        return latencyNanos;
    }

    private boolean handleBusyOperation(final SegmentId segmentId,
            final boolean waitForCompletion, final String operationLabel) {
        if (!isSegmentStillMapped(segmentId)) {
            logOperation(
                    "{} aborted because segment is no longer mapped: segment='{}'",
                    operationLabel, segmentId);
            return true;
        }
        if (!waitForCompletion) {
            logOperation(
                    "{} coalesced because segment is already busy: segment='{}'",
                    operationLabel, segmentId);
            return true;
        }
        logOperation("{} busy, retrying: segment='{}'", operationLabel,
                segmentId);
        return false;
    }

    private boolean shouldIgnoreUnmappedError(final OperationStatus status,
            final SegmentId segmentId, final String operationLabel) {
        if (status != OperationStatus.ERROR || isSegmentStillMapped(segmentId)) {
            return false;
        }
        logOperation(
                "{} ignored error because segment is no longer mapped: segment='{}'",
                operationLabel, segmentId);
        return true;
    }

    private IndexException newIndexException(final String operation,
            final SegmentId segmentId, final OperationStatus status) {
        return new IndexException(
                String.format("Index operation '%s' failed on segment '%s': %s",
                        operation, segmentId, status));
    }

    private long awaitAcceptedToReadyNanos(final SegmentId segmentId,
            final String operation, final BlockingSegment<K, V> segment) {
        final long acceptedAtNanos = nanoTimeSupplier.getAsLong();
        awaitSegmentReady(segmentId, operation, segment);
        return nanoTimeSupplier.getAsLong() - acceptedAtNanos;
    }

    private void awaitSegmentReady(final SegmentId segmentId,
            final String operation, final BlockingSegment<K, V> segment) {
        final long startNanos = retryPolicy.startNanos();
        while (true) {
            final SegmentState state = segment.getRuntime().getState();
            if (state == SegmentState.READY || state == SegmentState.CLOSED) {
                return;
            }
            if (state == SegmentState.ERROR) {
                throw new IndexException(
                        String.format("Segment '%s' failed during %s.",
                                segmentId, operation));
            }
            retryPolicy.backoffOrThrow(startNanos, operation, segmentId);
        }
    }

    private boolean isSegmentStillMapped(final SegmentId segmentId) {
        return keyToSegmentMap.getSegmentIds().contains(segmentId);
    }

    private void finalizeSettledCompaction() {
        finalizeSettledMaintenance(MaintenanceOperation.COMPACT);
    }

    private void finalizeSettledFlush() {
        finalizeSettledMaintenance(MaintenanceOperation.FLUSH);
    }

    private void finalizeSettledMaintenance(
            final MaintenanceOperation operation) {
        final long topologyVersion = keyToSegmentMap.snapshot().version();
        splitService.awaitQuiescence();
        if (!keyToSegmentMap.isAtVersion(topologyVersion)) {
            rerunSettledMaintenance(operation);
        }
        finishSettledMaintenance();
    }

    private void rerunSettledMaintenance(final MaintenanceOperation operation) {
        if (operation == MaintenanceOperation.COMPACT) {
            compactMappedSegmentsAndFlush();
        } else {
            flushMappedSegmentsAndWait();
        }
    }

    private void finishSettledMaintenance() {
        keyToSegmentMap.flushIfDirty();
        storageService.checkpointWal();
    }

    private void executeCompactAsync() {
        enterAsyncMaintenance(MaintenanceOperation.COMPACT.id());
        try {
            maintenanceExecutor.execute(this::runCompactAsync);
        } catch (final RuntimeException e) {
            exitAsyncMaintenance();
            throw new IndexException(String.format(
                    "Unable to schedule index operation '%s'.",
                    MaintenanceOperation.COMPACT.id()), e);
        }
    }

    private void executeFlushAsync() {
        enterAsyncMaintenance(MaintenanceOperation.FLUSH.id());
        try {
            maintenanceExecutor.execute(this::runFlushAsync);
        } catch (final RuntimeException e) {
            exitAsyncMaintenance();
            throw new IndexException(String.format(
                    "Unable to schedule index operation '%s'.",
                    MaintenanceOperation.FLUSH.id()), e);
        }
    }

    private void enterAsyncMaintenance(final String operation) {
        if (asyncMaintenanceClosed.get()) {
            throw new IndexException(String.format(
                    "Index operation '%s' cannot be scheduled because maintenance is closing.",
                    operation));
        }
        asyncMaintenanceInFlight.incrementAndGet();
        if (asyncMaintenanceClosed.get()) {
            exitAsyncMaintenance();
            throw new IndexException(String.format(
                    "Index operation '%s' cannot be scheduled because maintenance is closing.",
                    operation));
        }
    }

    private void runCompactAsync() {
        try {
            compactMappedSegments();
        } catch (final RuntimeException e) {
            LOGGER.error("Index operation '{}' failed.",
                    MaintenanceOperation.COMPACT.id(), e);
        } finally {
            exitAsyncMaintenance();
        }
    }

    private void runFlushAsync() {
        try {
            flushMappedSegmentsAndRoutes();
        } catch (final RuntimeException e) {
            LOGGER.error("Index operation '{}' failed.",
                    MaintenanceOperation.FLUSH.id(), e);
        } finally {
            exitAsyncMaintenance();
        }
    }

    private void exitAsyncMaintenance() {
        asyncMaintenanceInFlight.decrementAndGet();
    }

    private void logOperation(final String message, final Object... args) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(message, args);
        }
    }
}
