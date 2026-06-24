package org.hestiastore.index.segmentindex;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistry;
import org.hestiastore.index.segmentindex.core.session.SegmentIndexRuntimeHandle;

final class HestiaStoreRuntimeAccess {

    private HestiaStoreRuntimeAccess() {
    }

    static SegmentIndexRuntimeHandle owned() {
        return new RuntimeHandle(HestiaStoreRuntime.builder().build(), true);
    }

    static SegmentIndexRuntimeHandle borrowed(
            final HestiaStoreRuntime runtime) {
        return new RuntimeHandle(
                Vldtn.requireNonNull(runtime, "runtime"), false);
    }

    private static final class RuntimeHandle
            implements SegmentIndexRuntimeHandle {

        private final HestiaStoreRuntime runtime;
        private final boolean closeRuntime;

        private RuntimeHandle(final HestiaStoreRuntime runtime,
                final boolean closeRuntime) {
            this.runtime = Vldtn.requireNonNull(runtime, "runtime");
            this.closeRuntime = closeRuntime;
        }

        @Override
        public ExecutorRegistry createExecutorRegistry(final String indexName,
                final boolean contextLoggingEnabled,
                final int indexMaintenanceThreads,
                final int registryMaintenanceThreads,
                final int shutdownTimeoutMillis) {
            return runtime.createExecutorRegistry(indexName,
                    contextLoggingEnabled, indexMaintenanceThreads,
                    registryMaintenanceThreads, shutdownTimeoutMillis);
        }

        @Override
        public String threadNamePrefix() {
            return runtime.threadNamePrefix();
        }

        @Override
        public void close() {
            if (closeRuntime) {
                runtime.close();
            }
        }
    }
}
