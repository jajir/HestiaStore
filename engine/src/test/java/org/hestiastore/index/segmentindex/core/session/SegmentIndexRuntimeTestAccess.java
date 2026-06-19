package org.hestiastore.index.segmentindex.core.session;

import static org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfigurationTestSupport.effective;
import static org.mockito.Mockito.mock;

import java.lang.reflect.Field;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.SegmentIndex;
import org.hestiastore.index.segmentindex.configuration.persistence.IndexConfigurationStore;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuning;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuningState;
import org.hestiastore.index.segmentindex.configuration.api.IndexConfiguration;
import org.hestiastore.index.segmentindex.core.SegmentIndexStateMachine;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistry;
import org.hestiastore.index.segmentindex.core.execution.MappedSegmentMaintenanceService;
import org.hestiastore.index.segmentindex.core.execution.IndexOperationStatsRecorder;
import org.hestiastore.index.segmentindex.core.execution.PointOperationCoordinator;
import org.hestiastore.index.segmentindex.core.execution.SegmentIteratorService;
import org.hestiastore.index.segmentindex.core.split.SplitRuntime;
import org.hestiastore.index.segmentindex.core.storage.OpenedStorageRuntime;
import org.hestiastore.index.segmentindex.core.storage.StorageCoordinator;
import org.hestiastore.index.segmentindex.routemap.SegmentRouteMap;
import org.hestiastore.index.segmentindex.wal.WalRuntime;
import org.hestiastore.index.segmentregistry.SegmentRegistry;

/**
 * Test-only bridge exposing package-private runtime internals without keeping
 * them public in production code.
 */
public final class SegmentIndexRuntimeTestAccess {

    private SegmentIndexRuntimeTestAccess() {
    }

    public static <K, V> Object openRuntime(
            final Directory directoryFacade,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final IndexConfiguration<K, V> conf,
            final ExecutorRegistry executorRegistry) {
        Vldtn.requireNonNull(keyTypeDescriptor, "keyTypeDescriptor");
        Vldtn.requireNonNull(valueTypeDescriptor, "valueTypeDescriptor");
        try {
            new IndexConfigurationStore<K, V>(directoryFacade)
                    .save(effective(conf));
            final SegmentIndex<K, V> index = SegmentIndex.open(directoryFacade);
            return new OpenedRuntime<>(index, runtimeFromIndex(index));
        } finally {
            if (!executorRegistry.wasClosed()) {
                executorRegistry.close();
            }
        }
    }

    public static Object runtime(final Object runtime) {
        return castRuntimeView(runtime);
    }

    public static <K> SegmentRouteMap<K> keyToSegmentMap(
            final Object runtime) {
        @SuppressWarnings("unchecked")
        final SegmentRouteMap<K> keyToSegmentMap = (SegmentRouteMap<K>) field(
                segmentLeaseService(castRuntimeView(runtime)),
                "keyToSegmentMap");
        return keyToSegmentMap;
    }

    public static <K, V> SegmentRegistry<K, V> segmentRegistry(
            final Object runtime) {
        @SuppressWarnings("unchecked")
        final SegmentRegistry<K, V> segmentRegistry = (SegmentRegistry<K, V>) field(
                segmentLeaseService(castRuntimeView(runtime)),
                "segmentRegistry");
        return segmentRegistry;
    }

    public static <K, V> WalRuntime<K, V> walRuntime(
            final Object runtime) {
        try {
            final SegmentIndexRuntimeView<K, V> runtimeView = castRuntimeView(runtime);
            final Object walCoordinator = walCoordinator(runtimeView);
            final Field walRuntimeField = walCoordinator.getClass()
                    .getDeclaredField("walRuntime");
            walRuntimeField.setAccessible(true);
            @SuppressWarnings("unchecked")
            final WalRuntime<K, V> walRuntime = (WalRuntime<K, V>) walRuntimeField
                    .get(walCoordinator);
            return walRuntime;
        } catch (final ReflectiveOperationException ex) {
            throw new IllegalStateException(
                    "Unable to unwrap active WAL runtime for test access", ex);
        }
    }

    public static <K, V> SplitRuntime<K, V> splitService(
            final Object runtime) {
        final SegmentIndexRuntimeView<K, V> runtimeView = castRuntimeView(runtime);
        return runtimeView.splitService();
    }

    public static <K, V> StorageCoordinator<K, V> storageService(
            final Object runtime) {
        final SegmentIndexRuntimeView<K, V> runtimeView = castRuntimeView(runtime);
        return runtimeView.coreStorageRuntime().getStorageService();
    }

    private static <K, V> Object walCoordinator(
            final SegmentIndexRuntimeView<K, V> runtime) {
        try {
            final StorageCoordinator<K, V> storageService = runtime
                    .coreStorageRuntime().getStorageService();
            final Field walCoordinatorField = storageService.getClass()
                    .getDeclaredField("walCoordinator");
            walCoordinatorField.setAccessible(true);
            return walCoordinatorField.get(storageService);
        } catch (final ReflectiveOperationException ex) {
            throw new IllegalStateException(
                    "Unable to unwrap WAL coordinator for test access", ex);
        }
    }

    public static <K, V> PointOperationCoordinator<K, V> operationAccess(
            final Object runtime) {
        final SegmentIndexRuntimeView<K, V> runtimeView = castRuntimeView(runtime);
        return runtimeView.operationAccess();
    }

    public static <K, V> RuntimeTuningState runtimeTuningState(
            final Object runtime) {
        return (RuntimeTuningState) field(castRuntimeView(runtime).runtimeTuning(),
                "runtimeTuningState");
    }

    public static <K, V> RuntimeTuning runtimeTuning(
            final Object runtime) {
        return castRuntimeView(runtime).runtimeTuning();
    }

    public static <K, V> void closeRuntime(
            final SegmentIndexRuntimeView<K, V> runtime,
            final String indexName, final ExecutorRegistry executorRegistry) {
        final SegmentIndexStateMachine stateMachine = new SegmentIndexStateMachine();
        stateMachine.markReady();
        new SessionCloseCoordinator<>(indexName, stateMachine,
                mock(SessionOperationGate.class),
                new IndexOperationStatsRecorder(),
                runtime.splitService(),
                runtime.maintenance(),
                runtime.coreStorageRuntime(),
                runtime.coreStorageRuntime().getStorageService(),
                executorRegistry,
                new IndexDirectoryLock(new MemDirectory()))
                .close();
    }

    public static void closeRuntime(final Object runtime,
            final String indexName, final ExecutorRegistry executorRegistry) {
        if (runtime instanceof OpenedRuntime<?, ?> openedRuntime) {
            openedRuntime.index.close();
            return;
        }
        closeRuntime(castRuntimeView(runtime), indexName, executorRegistry);
    }

    @SuppressWarnings("unchecked")
    private static <K, V> SegmentIndexRuntimeView<K, V> castRuntimeView(
            final Object runtime) {
        if (runtime instanceof OpenedRuntime<?, ?> openedRuntime) {
            return (SegmentIndexRuntimeView<K, V>) openedRuntime.runtime;
        }
        return (SegmentIndexRuntimeView<K, V>) runtime;
    }

    private static Object segmentLeaseService(
            final SegmentIndexRuntimeView<?, ?> runtime) {
        return field(runtime.operationAccess(), "segmentLeaseService");
    }

    private static Object segmentLeaseService(
            final PointOperationCoordinator<?, ?> operationAccess) {
        return field(operationAccess, "segmentLeaseService");
    }

    private static Object field(final Object target, final String fieldName) {
        Class<?> type = target.getClass();
        while (type != null) {
            try {
                final Field result = type.getDeclaredField(fieldName);
                result.setAccessible(true);
                return result.get(target);
            } catch (final NoSuchFieldException ex) {
                type = type.getSuperclass();
            } catch (final IllegalAccessException ex) {
                throw new IllegalStateException(
                        "Unable to access field '" + fieldName + "'", ex);
            }
        }
        throw new IllegalStateException(
                "Field '" + fieldName + "' not found on "
                        + target.getClass().getName());
    }

    static <K, V> SegmentIndexRuntimeView<K, V> runtimeFromIndex(
            final Object index) {
        final SegmentIndexSession<K, V> implementation = indexImplementation(
                index);
        final Object maintenance = maintenanceDelegate(implementation);
        final PointOperationCoordinator<K, V> operationAccess =
                operationAccess(implementation);
        return new SegmentIndexRuntimeView<>(
                new OpenedStorageRuntime<>(
                        runtimeTuningState(implementation.runtimeTuning()),
                        storageServiceFromMaintenance(maintenance),
                        segmentRegistry(operationAccess),
                        keyToSegmentMap(operationAccess)),
                splitService(maintenanceService(maintenance)),
                streamingService(implementation),
                operationAccess,
                maintenanceService(maintenance),
                implementation.runtimeMonitoring(),
                implementation.runtimeTuning());
    }

    private static RuntimeTuningState runtimeTuningState(
            final RuntimeTuning runtimeTuning) {
        return (RuntimeTuningState) field(runtimeTuning, "runtimeTuningState");
    }

    @SuppressWarnings("unchecked")
    private static <K, V> SegmentIndexSession<K, V> indexImplementation(
            final Object index) {
        Object current = index;
        while (!(current instanceof SegmentIndexSession<?, ?>)) {
            try {
                final Field delegateField = current.getClass()
                        .getDeclaredField("delegate");
                delegateField.setAccessible(true);
                current = delegateField.get(current);
            } catch (final ReflectiveOperationException ex) {
                throw new IllegalStateException(
                        "Unable to unwrap segment index for test access", ex);
            }
        }
        return (SegmentIndexSession<K, V>) current;
    }

    @SuppressWarnings("unchecked")
    private static <K, V> PointOperationCoordinator<K, V> operationAccess(
            final SegmentIndexSession<K, V> implementation) {
        return (PointOperationCoordinator<K, V>) field(implementation,
                "operationAccess");
    }

    @SuppressWarnings("unchecked")
    private static <K> SegmentRouteMap<K> keyToSegmentMap(
            final PointOperationCoordinator<K, ?> operationAccess) {
        return (SegmentRouteMap<K>) field(segmentLeaseService(operationAccess),
                "keyToSegmentMap");
    }

    @SuppressWarnings("unchecked")
    private static <K, V> SegmentRegistry<K, V> segmentRegistry(
            final PointOperationCoordinator<K, V> operationAccess) {
        return (SegmentRegistry<K, V>) field(
                segmentLeaseService(operationAccess), "segmentRegistry");
    }

    @SuppressWarnings("unchecked")
    private static <K, V> SegmentIteratorService<K, V> streamingService(
            final SegmentIndexSession<K, V> implementation) {
        return (SegmentIteratorService<K, V>) field(implementation,
                "streamingService");
    }

    private static Object maintenanceDelegate(
            final SegmentIndexSession<?, ?> implementation) {
        return implementation.maintenance();
    }

    @SuppressWarnings("unchecked")
    private static <K, V> MappedSegmentMaintenanceService<K, V> maintenanceService(
            final Object maintenance) {
        return (MappedSegmentMaintenanceService<K, V>) field(maintenance,
                "maintenanceService");
    }

    @SuppressWarnings("unchecked")
    private static <K, V> SplitRuntime<K, V> splitService(
            final MappedSegmentMaintenanceService<K, V> maintenanceService) {
        return (SplitRuntime<K, V>) field(maintenanceService, "splitService");
    }

    @SuppressWarnings("unchecked")
    private static <K, V> StorageCoordinator<K, V> storageServiceFromMaintenance(
            final Object maintenance) {
        return (StorageCoordinator<K, V>) field(maintenance, "storageService");
    }

    private static final class OpenedRuntime<K, V> {

        private final SegmentIndex<K, V> index;
        private final SegmentIndexRuntimeView<K, V> runtime;

        OpenedRuntime(final SegmentIndex<K, V> index,
                final SegmentIndexRuntimeView<K, V> runtime) {
            this.index = Vldtn.requireNonNull(index, "index");
            this.runtime = Vldtn.requireNonNull(runtime, "runtime");
        }
    }
}
