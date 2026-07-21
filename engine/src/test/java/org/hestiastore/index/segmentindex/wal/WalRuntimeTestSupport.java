package org.hestiastore.index.segmentindex.wal;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;

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
                valueDescriptor, new ArrayBlockingQueue<>(8192));
    }

    static <K, V> WalRuntime<K, V> openWithStorageAndQueue(
            final IndexWalConfiguration wal, final WalStorage storage,
            final TypeDescriptor<K> keyDescriptor,
            final TypeDescriptor<V> valueDescriptor,
            final BlockingQueue<WalAppendTask<K, V>> appendQueue) {
        final IndexWalConfiguration resolvedWal =
                requireEnabledWal(effective(wal));
        return createRuntime(resolvedWal,
                Vldtn.requireNonNull(storage, "storage"), keyDescriptor,
                valueDescriptor,
                Vldtn.requireNonNull(appendQueue, "appendQueue"));
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
            final TypeDescriptor<V> valueDescriptor,
            final BlockingQueue<WalAppendTask<K, V>> appendQueue) {
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
                metrics, monitor, segmentCatalog);
        final WalWriter<K, V> writer = new WalWriter<>(storage,
                recordCodec, segmentCatalog, metrics);
        final WalRecoveryManager<K, V> recoveryManager =
                new WalRecoveryManager<>(wal, storage, metadataCatalog,
                        recordCodec, segmentCatalog, metrics);
        final WalRuntime<K, V> runtime = new WalRuntime<>(monitor, metrics,
                closed, storage, metadataCatalog, segmentCatalog, syncPolicy,
                writer, recoveryManager, appendQueue,
                appendThreadFactory());
        metadataCatalog.ensureFormatMarker();
        return runtime;
    }

    private static ThreadFactory appendThreadFactory() {
        final ThreadFactory delegate = Executors.defaultThreadFactory();
        return runnable -> {
            final Thread thread = delegate.newThread(runnable);
            thread.setName("hestia-wal-runtime-test-wal-append-1");
            thread.setDaemon(true);
            return thread;
        };
    }
}
