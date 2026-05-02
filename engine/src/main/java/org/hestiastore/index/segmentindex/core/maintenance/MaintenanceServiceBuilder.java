package org.hestiastore.index.segmentindex.core.maintenance;

import java.util.concurrent.ExecutorService;
import java.util.function.LongSupplier;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.IndexRetryPolicy;
import org.hestiastore.index.segmentindex.metrics.Stats;
import org.hestiastore.index.segmentindex.core.stablesegment.StableSegmentOperationAccess;
import org.hestiastore.index.segmentindex.core.split.SplitService;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.slf4j.Logger;

/**
 * Builder for {@link MaintenanceService} instances.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class MaintenanceServiceBuilder<K, V> {

    private Logger logger;
    private KeyToSegmentMap<K> keyToSegmentMap;
    private StableSegmentOperationAccess<K, V> stableSegmentGateway;
    private SplitService splitService;
    private IndexRetryPolicy retryPolicy;
    private Stats stats;
    private ExecutorService maintenanceExecutor;
    private Runnable checkpointAction;
    private LongSupplier nanoTimeSupplier = System::nanoTime;

    MaintenanceServiceBuilder() {
    }

    /**
     * Sets the logger used by stable segment maintenance operations.
     *
     * @param logger logger
     * @return this builder
     */
    public MaintenanceServiceBuilder<K, V> logger(final Logger logger) {
        this.logger = Vldtn.requireNonNull(logger, "logger");
        return this;
    }

    /**
     * Sets the key-to-segment map used for mapped segment scans.
     *
     * @param keyToSegmentMap key-to-segment map
     * @return this builder
     */
    public MaintenanceServiceBuilder<K, V> keyToSegmentMap(
            final KeyToSegmentMap<K> keyToSegmentMap) {
        this.keyToSegmentMap = Vldtn.requireNonNull(keyToSegmentMap,
                "keyToSegmentMap");
        return this;
    }

    /**
     * Sets the stable segment gateway used for maintenance operations.
     *
     * @param stableSegmentGateway stable segment gateway
     * @return this builder
     */
    public MaintenanceServiceBuilder<K, V> stableSegmentGateway(
            final StableSegmentOperationAccess<K, V> stableSegmentGateway) {
        this.stableSegmentGateway = Vldtn.requireNonNull(stableSegmentGateway,
                "stableSegmentGateway");
        return this;
    }

    /**
     * Sets the split service used to settle split work around blocking
     * maintenance calls.
     *
     * @param splitService split service
     * @return this builder
     */
    public MaintenanceServiceBuilder<K, V> splitService(
            final SplitService splitService) {
        this.splitService = Vldtn.requireNonNull(splitService, "splitService");
        return this;
    }

    /**
     * Sets the retry policy used for transient busy states.
     *
     * @param retryPolicy retry policy
     * @return this builder
     */
    public MaintenanceServiceBuilder<K, V> retryPolicy(
            final IndexRetryPolicy retryPolicy) {
        this.retryPolicy = Vldtn.requireNonNull(retryPolicy, "retryPolicy");
        return this;
    }

    /**
     * Sets the stats sink used for maintenance telemetry.
     *
     * @param stats stats sink
     * @return this builder
     */
    public MaintenanceServiceBuilder<K, V> stats(final Stats stats) {
        this.stats = Vldtn.requireNonNull(stats, "stats");
        return this;
    }

    /**
     * Sets the executor used for nonblocking index-level maintenance requests.
     *
     * @param maintenanceExecutor executor for asynchronous flush and compaction
     *            orchestration
     * @return this builder
     */
    public MaintenanceServiceBuilder<K, V> maintenanceExecutor(
            final ExecutorService maintenanceExecutor) {
        this.maintenanceExecutor = Vldtn.requireNonNull(maintenanceExecutor,
                "maintenanceExecutor");
        return this;
    }

    /**
     * Sets the action that checkpoints WAL after blocking maintenance reaches a
     * durable state.
     *
     * @param checkpointAction checkpoint action
     * @return this builder
     */
    public MaintenanceServiceBuilder<K, V> checkpointAction(
            final Runnable checkpointAction) {
        this.checkpointAction = Vldtn.requireNonNull(checkpointAction,
                "checkpointAction");
        return this;
    }

    MaintenanceServiceBuilder<K, V> nanoTimeSupplier(
            final LongSupplier nanoTimeSupplier) {
        this.nanoTimeSupplier = Vldtn.requireNonNull(nanoTimeSupplier,
                "nanoTimeSupplier");
        return this;
    }

    /**
     * Builds the service.
     *
     * @return maintenance service
     */
    public MaintenanceService build() {
        return new MaintenanceServiceImpl<>(
                Vldtn.requireNonNull(logger, "logger"),
                Vldtn.requireNonNull(keyToSegmentMap, "keyToSegmentMap"),
                Vldtn.requireNonNull(stableSegmentGateway,
                        "stableSegmentGateway"),
                Vldtn.requireNonNull(splitService, "splitService"),
                Vldtn.requireNonNull(retryPolicy, "retryPolicy"),
                Vldtn.requireNonNull(stats, "stats"),
                Vldtn.requireNonNull(maintenanceExecutor,
                        "maintenanceExecutor"),
                Vldtn.requireNonNull(checkpointAction, "checkpointAction"),
                Vldtn.requireNonNull(nanoTimeSupplier, "nanoTimeSupplier"));
    }
}
