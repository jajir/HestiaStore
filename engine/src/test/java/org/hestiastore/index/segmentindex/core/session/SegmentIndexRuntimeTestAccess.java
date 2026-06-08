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
import org.hestiastore.index.segmentindex.core.operations.IndexOperationStatsRecorder;
import org.hestiastore.index.segmentindex.core.operations.SegmentIndexOperationAccess;
import org.hestiastore.index.segmentindex.core.storage.StorageService;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
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

    public static <K, V> SegmentIndexRuntime<K, V> runtime(
            final Object runtime) {
        return castRuntime(runtime);
    }

    public static <K> KeyToSegmentMap<K> keyToSegmentMap(
            final Object runtime) {
        return keyToSegmentMap(castRuntime(runtime));
    }

    public static <K> KeyToSegmentMap<K> keyToSegmentMap(
            final SegmentIndexRuntime<K, ?> runtime) {
        @SuppressWarnings("unchecked")
        final KeyToSegmentMap<K> keyToSegmentMap = (KeyToSegmentMap<K>) field(
                segmentLeaseService(runtime), "keyToSegmentMap");
        return keyToSegmentMap;
    }

    public static <K, V> SegmentRegistry<K, V> segmentRegistry(
            final SegmentIndexRuntime<K, V> runtime) {
        @SuppressWarnings("unchecked")
        final SegmentRegistry<K, V> segmentRegistry =
                (SegmentRegistry<K, V>) field(segmentLeaseService(runtime),
                        "segmentRegistry");
        return segmentRegistry;
    }

    public static <K, V> WalRuntime<K, V> walRuntime(
            final SegmentIndexRuntime<K, V> runtime) {
        try {
            final Object walCoordinator = walCoordinator(runtime);
            final Field delegateField = walCoordinator.getClass()
                    .getDeclaredField("delegate");
            delegateField.setAccessible(true);
            final Object delegate = delegateField.get(walCoordinator);
            final Field walRuntimeField = delegate.getClass()
                    .getDeclaredField("walRuntime");
            walRuntimeField.setAccessible(true);
            @SuppressWarnings("unchecked")
            final WalRuntime<K, V> walRuntime = (WalRuntime<K, V>) walRuntimeField.get(delegate);
            return walRuntime;
        } catch (final ReflectiveOperationException ex) {
            throw new IllegalStateException(
                    "Unable to unwrap active WAL runtime for test access", ex);
        }
    }

    public static <K, V> SegmentTopologyRuntimeAccess<K, V> topologyRuntime(
            final SegmentIndexRuntime<K, V> runtime) {
        return runtime.topologyRuntime();
    }

    public static <K, V> SegmentTopologyRuntimeAccess<K, V> topologyRuntime(
            final Object runtime) {
        return topologyRuntime(castRuntime(runtime));
    }

    public static <K, V> StorageService<K, V> storageService(
            final SegmentIndexRuntime<K, V> runtime) {
        return runtime.storageService();
    }

    private static <K, V> Object walCoordinator(
            final SegmentIndexRuntime<K, V> runtime) {
        try {
            final StorageService<K, V> storageService = runtime.storageService();
            final Field walCoordinatorField = storageService.getClass()
                    .getDeclaredField("walCoordinator");
            walCoordinatorField.setAccessible(true);
            return walCoordinatorField.get(storageService);
        } catch (final ReflectiveOperationException ex) {
            throw new IllegalStateException(
                    "Unable to unwrap WAL coordinator for test access", ex);
        }
    }

    public static <K, V> SegmentIndexOperationAccess<K, V> operationAccess(
            final SegmentIndexRuntime<K, V> runtime) {
        return runtime.operationAccess();
    }

    public static <K, V> RuntimeTuningState runtimeTuningState(
            final SegmentIndexRuntime<K, V> runtime) {
        return (RuntimeTuningState) field(runtime.runtimeTuning(),
                "runtimeTuningState");
    }

    public static <K, V> RuntimeTuning runtimeTuning(
            final SegmentIndexRuntime<K, V> runtime) {
        return runtime.runtimeTuning();
    }

    public static <K, V> void closeRuntime(final SegmentIndexRuntime<K, V> runtime,
            final String indexName, final ExecutorRegistry executorRegistry) {
        final SegmentIndexStateMachine stateMachine = new SegmentIndexStateMachine();
        stateMachine.markReady();
        new IndexCloseCoordinator<>(indexName, stateMachine,
                mock(SegmentIndexOperationGate.class),
                new IndexOperationStatsRecorder(), runtime,
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
        closeRuntime(castRuntime(runtime), indexName, executorRegistry);
    }

    @SuppressWarnings("unchecked")
    private static <K, V> SegmentIndexRuntime<K, V> castRuntime(
            final Object runtime) {
        if (runtime instanceof OpenedRuntime<?, ?> openedRuntime) {
            return (SegmentIndexRuntime<K, V>) openedRuntime.runtime;
        }
        return (SegmentIndexRuntime<K, V>) runtime;
    }

    private static Object segmentLeaseService(
            final SegmentIndexRuntime<?, ?> runtime) {
        return field(runtime.operationAccess(), "segmentLeaseService");
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

    @SuppressWarnings("unchecked")
    private static <K, V> SegmentIndexRuntime<K, V> runtimeFromIndex(
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
        return ((SegmentIndexImpl<K, V>) current).runtime();
    }

    private static final class OpenedRuntime<K, V> {

        private final SegmentIndex<K, V> index;
        private final SegmentIndexRuntime<K, V> runtime;

        OpenedRuntime(final SegmentIndex<K, V> index,
                final SegmentIndexRuntime<K, V> runtime) {
            this.index = Vldtn.requireNonNull(index, "index");
            this.runtime = Vldtn.requireNonNull(runtime, "runtime");
        }
    }
}
