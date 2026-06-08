package org.hestiastore.index.segmentindex.core.maintenance;

import java.util.concurrent.ExecutorService;
import java.util.function.LongSupplier;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.core.stablesegment.StableSegmentOperationAccess;
import org.hestiastore.index.segmentindex.core.split.SplitService;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;

/**
 * Builder for {@link MaintenanceService} instances.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class MaintenanceServiceBuilder<K, V> {

    private KeyToSegmentMap<K> keyToSegmentMap;
    private StableSegmentOperationAccess<K, V> stableSegmentGateway;
    private SplitService splitService;
    private Integer busyBackoffMillis;
    private Integer busyTimeoutMillis;
    private MaintenanceStatsRecorder statsRecorder;
    private ExecutorService maintenanceExecutor;
    private Runnable checkpointAction;
    private LongSupplier nanoTimeSupplier = System::nanoTime;

    MaintenanceServiceBuilder() {
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
     * Sets the backoff value used to create the package-local maintenance retry
     * policy.
     *
     * @param busyBackoffMillis backoff in milliseconds
     * @return this builder
     */
    public MaintenanceServiceBuilder<K, V> busyBackoffMillis(
            final int busyBackoffMillis) {
        this.busyBackoffMillis = busyBackoffMillis;
        return this;
    }

    /**
     * Sets the timeout value used to create the package-local maintenance retry
     * policy.
     *
     * @param busyTimeoutMillis timeout in milliseconds
     * @return this builder
     */
    public MaintenanceServiceBuilder<K, V> busyTimeoutMillis(
            final int busyTimeoutMillis) {
        this.busyTimeoutMillis = busyTimeoutMillis;
        return this;
    }

    /**
     * Sets the stats recorder used for maintenance telemetry.
     *
     * @param statsRecorder stats recorder
     * @return this builder
     */
    public MaintenanceServiceBuilder<K, V> statsRecorder(
            final MaintenanceStatsRecorder statsRecorder) {
        this.statsRecorder = Vldtn.requireNonNull(statsRecorder,
                "statsRecorder");
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
                Vldtn.requireNonNull(keyToSegmentMap, "keyToSegmentMap"),
                Vldtn.requireNonNull(stableSegmentGateway,
                        "stableSegmentGateway"),
                Vldtn.requireNonNull(splitService, "splitService"),
                new MaintenanceRetryPolicy(Vldtn.requireNonNull(
                        busyBackoffMillis, "busyBackoffMillis"),
                        Vldtn.requireNonNull(busyTimeoutMillis,
                                "busyTimeoutMillis")),
                Vldtn.requireNonNull(statsRecorder, "statsRecorder"),
                Vldtn.requireNonNull(maintenanceExecutor,
                        "maintenanceExecutor"),
                Vldtn.requireNonNull(checkpointAction, "checkpointAction"),
                Vldtn.requireNonNull(nanoTimeSupplier, "nanoTimeSupplier"));
    }
}
