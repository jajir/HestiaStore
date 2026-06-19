package org.hestiastore.index.segmentindex.wal;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.segmentindex.configuration.api.IndexWalConfiguration;

final class WalRuntimeTestSupport {

    private WalRuntimeTestSupport() {
    }

    static <K, V> WalRuntime<K, V> openWithStorage(
            final IndexWalConfiguration wal, final WalStorage storage,
            final TypeDescriptor<K> keyDescriptor,
            final TypeDescriptor<V> valueDescriptor) {
        final IndexWalConfiguration resolvedWal =
                requireEnabledWal(effective(wal));
        return createRuntime(resolvedWal,
                Vldtn.requireNonNull(storage, "storage"), keyDescriptor,
                valueDescriptor);
    }

    static IndexWalConfiguration effective(
            final IndexWalConfiguration wal) {
        return IndexWalConfiguration.orEmpty(wal);
    }

    private static IndexWalConfiguration requireEnabledWal(
            final IndexWalConfiguration wal) {
        final IndexWalConfiguration resolvedWal =
                IndexWalConfiguration.orEmpty(wal);
        Vldtn.requireTrue(resolvedWal.isEnabled(),
                "WAL configuration must be enabled to create WalRuntime.");
        return resolvedWal;
    }

    private static <K, V> WalRuntime<K, V> createRuntime(
            final IndexWalConfiguration wal,
            final WalStorage storage, final TypeDescriptor<K> keyDescriptor,
            final TypeDescriptor<V> valueDescriptor) {
        final Object monitor = new Object();
        final WalRuntimeMetrics metrics = new WalRuntimeMetrics();
        final AtomicBoolean closed = new AtomicBoolean();
        final WalMetadataCatalog metadataCatalog = new WalMetadataCatalog(
                storage);
        final WalRecordCodec<K, V> recordCodec = new WalRecordCodec<>(
                keyDescriptor == null ? null : keyDescriptor.getTypeEncoder(),
                keyDescriptor == null ? null : keyDescriptor.getTypeDecoder(),
                valueDescriptor == null ? null
                        : valueDescriptor.getTypeEncoder(),
                valueDescriptor == null ? null
                        : valueDescriptor.getTypeDecoder());
        final WalSegmentCatalog segmentCatalog = new WalSegmentCatalog(wal,
                storage, metadataCatalog);
        final WalSyncPolicy syncPolicy = new WalSyncPolicy(wal, storage,
                metrics, monitor, segmentCatalog, closed);
        final WalWriter<K, V> writer = new WalWriter<>(wal, storage,
                recordCodec, segmentCatalog, metrics, syncPolicy);
        final WalRecoveryManager<K, V> recoveryManager =
                new WalRecoveryManager<>(wal, storage, metadataCatalog,
                        recordCodec, segmentCatalog, metrics);
        final WalRuntime<K, V> runtime = new WalRuntime<>(monitor, metrics,
                closed, metadataCatalog, segmentCatalog, syncPolicy, writer,
                recoveryManager, newGroupSyncExecutor(wal, syncPolicy));
        metadataCatalog.ensureFormatMarker();
        return runtime;
    }

    private static ScheduledExecutorService newGroupSyncExecutor(
            final IndexWalConfiguration wal,
            final WalSyncPolicy syncPolicy) {
        if (!wal.isGroupSyncDurabilityMode()
                || wal.getGroupSyncDelayMillis() <= 0) {
            return null;
        }
        final ScheduledExecutorService executor =
                Executors.newSingleThreadScheduledExecutor(
                        new NamedDaemonThreadFactory(
                                "hestiastore-wal-group-sync"));
        executor.scheduleWithFixedDelay(syncPolicy::syncGroupPendingSafely,
                wal.getGroupSyncDelayMillis(), wal.getGroupSyncDelayMillis(),
                TimeUnit.MILLISECONDS);
        return executor;
    }

    private static final class NamedDaemonThreadFactory
            implements ThreadFactory {

        private final String namePrefix;
        private final AtomicLong sequence = new AtomicLong(0L);

        private NamedDaemonThreadFactory(final String namePrefix) {
            this.namePrefix = Vldtn.requireNonNull(namePrefix, "namePrefix");
        }

        @Override
        public Thread newThread(final Runnable runnable) {
            final Thread thread = new Thread(runnable,
                    namePrefix + "-" + sequence.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        }
    }
}
