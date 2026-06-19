package org.hestiastore.index.segmentindex.core.session;

import static org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfigurationTestSupport.effective;
import static org.mockito.Mockito.mock;

import java.lang.reflect.Field;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.SegmentIndex;
import org.hestiastore.index.segmentindex.configuration.persistence.IndexConfigurationStorage;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuning;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuningState;
import org.hestiastore.index.segmentindex.configuration.user.IndexConfiguration;
import org.hestiastore.index.segmentindex.core.SegmentIndexStateMachine;
import org.hestiastore.index.segmentindex.core.bootstrap.SegmentIndexFactory;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistry;
import org.hestiastore.index.segmentindex.core.maintenance.MaintenanceService;
import org.hestiastore.index.segmentindex.core.operations.IndexOperationStatsRecorder;
import org.hestiastore.index.segmentindex.core.operations.IndexOperationCoordinator;
import org.hestiastore.index.segmentindex.core.storage.CoreStorageRuntime;
import org.hestiastore.index.segmentindex.core.storage.StorageService;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentindex.maintenance.SegmentIndexMaintenance;
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
            new IndexConfigurationStorage<K, V>(directoryFacade)
                    .save(effective(conf));
            final SegmentIndex<K, V> index = SegmentIndexFactory
                    .openStored(directoryFacade);
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

    public static <K> KeyToSegmentMap<K> keyToSegmentMap(
            final Object runtime) {
        @SuppressWarnings("unchecked")
        final KeyToSegmentMap<K> keyToSegmentMap = (KeyToSegmentMap<K>) field(
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

    public static <K, V> SegmentTopologyRuntimeAccess<K, V> topologyRuntime(
            final Object runtime) {
        final SegmentIndexRuntimeView<K, V> runtimeView = castRuntimeView(runtime);
        return runtimeView.topologyRuntime();
    }

    public static <K, V> StorageService<K, V> storageService(
            final Object runtime) {
        final SegmentIndexRuntimeView<K, V> runtimeView = castRuntimeView(runtime);
        return runtimeView.coreStorageRuntime().getStorageService();
    }

    private static <K, V> Object walCoordinator(
            final SegmentIndexRuntimeView<K, V> runtime) {
        try {
            final StorageService<K, V> storageService = runtime
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

    public static <K, V> IndexOperationCoordinator<K, V> operationAccess(
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
        new IndexCloseCoordinator<>(indexName, stateMachine,
                mock(SegmentIndexOperationGate.class),
                new IndexOperationStatsRecorder(),
                runtime.topologyRuntime(),
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
            final IndexOperationCoordinator<?, ?> operationAccess) {
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
        final SegmentIndexImpl<K, V> implementation = indexImplementation(
                index);
        final Object maintenance = maintenanceDelegate(implementation);
        final IndexOperationCoordinator<K, V> operationAccess =
                operationAccess(implementation);
        return new SegmentIndexRuntimeView<>(
                new CoreStorageRuntime<>(
                        runtimeTuningState(implementation.runtimeTuning()),
                        storageServiceFromMaintenance(maintenance),
                        segmentRegistry(operationAccess),
                        keyToSegmentMap(operationAccess)),
                topologyRuntime(implementation),
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
    private static <K, V> SegmentIndexImpl<K, V> indexImplementation(
            final Object index) {
        Object current = index;
        while (!(current instanceof SegmentIndexImpl<?, ?>)) {
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
        return (SegmentIndexImpl<K, V>) current;
    }

    @SuppressWarnings("unchecked")
    private static <K, V> IndexOperationCoordinator<K, V> operationAccess(
            final SegmentIndexImpl<K, V> implementation) {
        return (IndexOperationCoordinator<K, V>) field(implementation,
                "operationAccess");
    }

    @SuppressWarnings("unchecked")
    private static <K> KeyToSegmentMap<K> keyToSegmentMap(
            final IndexOperationCoordinator<K, ?> operationAccess) {
        return (KeyToSegmentMap<K>) field(segmentLeaseService(operationAccess),
                "keyToSegmentMap");
    }

    @SuppressWarnings("unchecked")
    private static <K, V> SegmentRegistry<K, V> segmentRegistry(
            final IndexOperationCoordinator<K, V> operationAccess) {
        return (SegmentRegistry<K, V>) field(
                segmentLeaseService(operationAccess), "segmentRegistry");
    }

    @SuppressWarnings("unchecked")
    private static <K, V> SegmentTopologyRuntimeAccess<K, V> topologyRuntime(
            final SegmentIndexImpl<K, V> implementation) {
        return (SegmentTopologyRuntimeAccess<K, V>) field(implementation,
                "topologyRuntime");
    }

    private static Object maintenanceDelegate(
            final SegmentIndexImpl<?, ?> implementation) {
        final SegmentIndexMaintenance maintenanceApi = implementation.maintenance();
        return field(maintenanceApi, "delegate");
    }

    @SuppressWarnings("unchecked")
    private static <K, V> MaintenanceService<K, V> maintenanceService(
            final Object maintenance) {
        return (MaintenanceService<K, V>) field(maintenance,
                "maintenanceService");
    }

    @SuppressWarnings("unchecked")
    private static <K, V> StorageService<K, V> storageServiceFromMaintenance(
            final Object maintenance) {
        return (StorageService<K, V>) field(maintenance, "storageService");
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
