package org.hestiastore.index.segmentindex.core.facade;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.core.consistency.IndexConsistencyCoordinator;
import org.hestiastore.index.segmentindex.core.maintenance.SegmentIndexMaintenanceAccess;

/**
 * Assembles data and maintenance facades as one cohesive boundary so outer core
 * composition does not need to know facade-internal wiring details.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class SegmentIndexFacades<K, V> {

    private final SegmentIndexMutationFacade<K, V> mutationFacade;
    private final SegmentIndexReadFacade<K, V> readFacade;
    private final SegmentIndexMaintenanceCommands<K, V> maintenanceCommands;

    private SegmentIndexFacades(
            final SegmentIndexMutationFacade<K, V> mutationFacade,
            final SegmentIndexReadFacade<K, V> readFacade,
            final SegmentIndexMaintenanceCommands<K, V> maintenanceCommands) {
        this.mutationFacade = Vldtn.requireNonNull(mutationFacade,
                "mutationFacade");
        this.readFacade = Vldtn.requireNonNull(readFacade, "readFacade");
        this.maintenanceCommands = Vldtn.requireNonNull(maintenanceCommands,
                "maintenanceCommands");
    }

    public static <K, V> SegmentIndexFacades<K, V> create(
            final IndexConfiguration<K, V> conf,
            final SegmentIndexTrackedOperationRunner<K, V> trackedRunner,
            final SegmentIndexDataAccess<K, V> dataAccess,
            final SegmentIndexMaintenanceAccess<K, V> maintenanceAccess,
            final IndexConsistencyCoordinator<K, V> consistencyCoordinator) {
        final IndexConfiguration<K, V> validatedConfiguration = Vldtn
                .requireNonNull(conf, "conf");
        final SegmentIndexTrackedOperationRunner<K, V> validatedTrackedRunner =
                Vldtn.requireNonNull(trackedRunner, "trackedRunner");
        final SegmentIndexDataAccess<K, V> validatedDataAccess = Vldtn
                .requireNonNull(dataAccess, "dataAccess");
        final SegmentIndexMaintenanceAccess<K, V> validatedMaintenanceAccess =
                Vldtn.requireNonNull(maintenanceAccess, "maintenanceAccess");
        final IndexConsistencyCoordinator<K, V> validatedConsistencyCoordinator =
                Vldtn.requireNonNull(consistencyCoordinator,
                        "consistencyCoordinator");
        return new SegmentIndexFacades<>(
                new SegmentIndexMutationFacade<>(
                        validatedTrackedRunner,
                        validatedDataAccess),
                new SegmentIndexReadFacade<>(
                        validatedTrackedRunner,
                        validatedDataAccess,
                        new SegmentIndexEntryIteratorDecorator<>(
                                validatedConfiguration)),
                new SegmentIndexMaintenanceCommands<>(
                        validatedTrackedRunner,
                        validatedMaintenanceAccess,
                        validatedConsistencyCoordinator));
    }

    public SegmentIndexMutationFacade<K, V> mutationFacade() {
        return mutationFacade;
    }

    public SegmentIndexReadFacade<K, V> readFacade() {
        return readFacade;
    }

    public SegmentIndexMaintenanceCommands<K, V> maintenanceCommands() {
        return maintenanceCommands;
    }
}
