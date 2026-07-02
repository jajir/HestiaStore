package org.hestiastore.index.segmentindex.core.session;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.chunkstore.ChunkData;
import org.hestiastore.index.chunkstore.ChunkFilter;
import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.chunkstore.ChunkFilterProvider;
import org.hestiastore.index.chunkstore.ChunkFilterProviderResolver;
import org.hestiastore.index.chunkstore.ChunkFilterProviderResolverImpl;
import org.hestiastore.index.chunkstore.ChunkFilterSpec;
import org.hestiastore.index.datatype.ByteArray;
import org.hestiastore.index.datatype.TypeDescriptorByteArray;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.properties.IndexPropertiesSchema;
import org.hestiastore.index.segmentindex.MemoryEstimateReport;
import org.hestiastore.index.segmentindex.SegmentIndex;
import org.hestiastore.index.segmentindex.configuration.persistence.IndexConfigurationStore;
import org.hestiastore.index.segmentindex.configuration.api.IndexConfiguration;
import org.hestiastore.index.segmentindex.configuration.api.IndexConfigurationBuilder;
import org.hestiastore.index.segmentindex.configuration.api.IndexWalConfiguration;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistry;
import org.hestiastore.index.segmentindex.core.executorregistry.RuntimeExecutorPools;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

class SegmentIndexBootstrapOperationTest {

    private static final String LOCK_FILE_NAME = ".lock";
    private static final String CONFIGURATION_FILE_NAME = IndexPropertiesSchema.IndexConfigurationKeys.CONFIGURATION_FILENAME;
    private static final String MDC_INDEX_NAME_KEY = "index.name";

    @AfterEach
    void tearDown() {
        MDC.clear();
    }

    @Test
    void createLoadsDefaultsAndSavesConfiguration() {
        final MemDirectory directory = new MemDirectory();

        operation(directory, buildConf("bootstrap-operation-create", false))
                .create()
                .close();

        assertTrue(directory.isFileExists(CONFIGURATION_FILE_NAME));
        final var loaded = new IndexConfigurationStore<Integer, String>(directory)
                .load();
        assertEquals("bootstrap-operation-create", loaded.identity().name());
        assertEquals(Integer.class, loaded.identity().keyClass());
        assertEquals(String.class, loaded.identity().valueClass());
    }

    @Test
    void createFailsWhenConfigurationAlreadyExistsAndDoesNotAcquireLock() {
        final MemDirectory directory = new MemDirectory();
        operation(directory,
                buildConf("bootstrap-operation-create-existing", false))
                .create()
                .close();

        assertThrows(IndexException.class,
                () -> operation(directory,
                        buildConf("bootstrap-operation-create-existing-new",
                                false))
                        .create());

        assertFalse(directory.isFileExists(LOCK_FILE_NAME));
        assertEquals("bootstrap-operation-create-existing",
                new IndexConfigurationStore<Integer, String>(directory)
                        .load().identity().name());
    }

    @Test
    void descriptorResolutionFailureKeepsLockAndDoesNotWriteConfiguration() {
        final MemDirectory directory = new MemDirectory();

        assertThrows(RuntimeException.class,
                () -> operation(directory,
                        buildConfWithInvalidKeyDescriptor(
                                "bootstrap-operation-invalid-descriptor"))
                        .create());

        assertTrue(directory.isFileExists(LOCK_FILE_NAME));
        assertFalse(directory.isFileExists(CONFIGURATION_FILE_NAME));
    }

    @Test
    void startupFailureLeavesExistingIndexMdcScopeUntouched() {
        final MemDirectory directory = new FailingFileNamesMemDirectory();

        MDC.put(MDC_INDEX_NAME_KEY, "outer");
        assertThrows(IndexException.class,
                () -> operation(directory,
                        buildConf("bootstrap-operation-mdc-restore", true))
                        .create());

        assertEquals("outer", MDC.get(MDC_INDEX_NAME_KEY));
        assertTrue(directory.isFileExists(LOCK_FILE_NAME));
    }

    @Test
    void successfulCloseReleasesLock() {
        final MemDirectory directory = new MemDirectory();
        final SegmentIndex<Integer, String> index = operation(directory,
                buildConf("bootstrap-operation-successful-close", false))
                .create();

        assertTrue(directory.isFileExists(LOCK_FILE_NAME));
        index.close();
        assertFalse(directory.isFileExists(LOCK_FILE_NAME));
        assertTrue(index.wasClosed());
    }

    @Test
    void createWrapsRuntimeIndexWithContextLoggingWhenEnabled() {
        final SegmentIndex<Integer, String> index = operation(
                new MemDirectory(),
                buildConf("bootstrap-operation-logging", true))
                .create();

        try {
            assertInstanceOf(SegmentIndexMdcLoggingAdapter.class, index);
            assertTrue(index.startupMemoryEstimate().isComplete());
            assertTrue(index.startupMemoryEstimate().text().contains(
                    "├─ Total index memory - "));
        } finally {
            index.close();
        }
    }

    @Test
    void createWrapsConcurrentRuntimeIndexWhenContextLoggingDisabled() {
        final SegmentIndex<Integer, String> index = operation(
                new MemDirectory(),
                buildConf("bootstrap-operation-plain", false))
                .create();

        try {
            assertFalse(index instanceof SegmentIndexMdcLoggingAdapter);
        } finally {
            index.close();
        }
    }

    @Test
    void openMergesStoredConfigurationWithOverrides() {
        final MemDirectory directory = new MemDirectory();
        operation(directory, buildConf("bootstrap-operation-open", false, 1))
                .create()
                .close();

        final SegmentIndex<Integer, String> index = operation(directory,
                buildConf("bootstrap-operation-open", false, 2)).open();

        try {
            assertEquals(2,
                    new IndexConfigurationStore<Integer, String>(directory)
                            .load()
                            .maintenance().registryLifecycleThreads());
            assertTrue(index.startupMemoryEstimate().isComplete());
            assertTrue(index.startupMemoryEstimate().text().contains(
                    "├─ Total index memory - "));
        } finally {
            index.close();
        }
    }

    @Test
    void openDescriptorOverrideFailureKeepsLockAndDoesNotPersistMergedConfiguration() {
        final MemDirectory directory = new MemDirectory();
        operation(directory,
                buildConf("bootstrap-operation-open-invalid", false, 1))
                .create()
                .close();

        assertThrows(RuntimeException.class,
                () -> operation(directory,
                        buildConfWithInvalidKeyDescriptor(
                                "bootstrap-operation-open-invalid"))
                        .open());

        assertTrue(directory.isFileExists(LOCK_FILE_NAME));
        assertEquals(1,
                new IndexConfigurationStore<Integer, String>(directory)
                        .load().maintenance().registryLifecycleThreads());
    }

    @Test
    void explicitProviderResolverIsUsedForPersistedCustomChunkFilters() {
        final MemDirectory directory = new MemDirectory();
        final ChunkFilterProviderResolver resolver = ChunkFilterProviderResolverImpl.builder()
                .withDefaultProviders()
                .withProvider(new BootstrapChunkFilterProvider())
                .build();
        final IndexConfiguration<Integer, String> original = buildCustomFilterConf("bootstrap-operation-provider");

        operation(directory, original, resolver).create().close();

        final SegmentIndex<Integer, String> index = operation(directory,
                buildCustomFilterConf("bootstrap-operation-provider"),
                resolver).open();

        try {
            final var loaded = new IndexConfigurationStore<Integer, String>(directory,
                    resolver)
                    .load();
            assertEquals(original.filters().encodingChunkFilterSpecs(),
                    loaded.filters().encodingChunkFilterSpecs());
            assertEquals(original.filters().decodingChunkFilterSpecs(),
                    loaded.filters().decodingChunkFilterSpecs());
            assertNotSame(original.filters().encodingChunkFilterSpecs(),
                    loaded.filters().encodingChunkFilterSpecs());
            assertNotSame(original.filters().decodingChunkFilterSpecs(),
                    loaded.filters().decodingChunkFilterSpecs());
            assertNotSame(original.filters().encodingChunkFilterSpecs().get(0),
                    loaded.filters().encodingChunkFilterSpecs().get(0));
            assertNotSame(original.filters().decodingChunkFilterSpecs().get(0),
                    loaded.filters().decodingChunkFilterSpecs().get(0));
        } finally {
            index.close();
        }
    }

    @Test
    void createLogsStartupMemoryEstimate() {
        final TestLogAppender appender = TestLogAppender.attachToLogger(
                SegmentIndexBootstrapOperation.class.getName(), Level.INFO);

        try {
            final SegmentIndex<Integer, String> index = operation(
                    new MemDirectory(),
                    buildConf("bootstrap-operation-memory-estimate", false))
                    .create();
            try {
                final List<String> loggedReportLines = appender
                        .messagesStartingWith("memory-estimate ")
                        .stream()
                        .map(message -> message
                                .substring("memory-estimate ".length()))
                        .toList();

                assertEquals(index.startupMemoryEstimate().lines(),
                        loggedReportLines);
                printReport("bootstrap-complete",
                        index.startupMemoryEstimate());
                assertTrue(appender.countMessageStartingWith(
                        "memory-estimate ") > 1);
                assertTrue(index.startupMemoryEstimate().isComplete());
            } finally {
                index.close();
            }
        } finally {
            appender.detach();
        }
    }

    @Test
    void createLogsIncompleteStartupMemoryEstimateForUnknownValueSize() {
        final TestLogAppender appender = TestLogAppender.attachToLogger(
                SegmentIndexBootstrapOperation.class.getName(), Level.INFO);

        try {
            final SegmentIndex<Integer, ByteArray> index = new SegmentIndexBootstrapOperation<Integer, ByteArray>(
                    new MemDirectory(),
                    buildUnknownValueSizeConf(
                            "bootstrap-operation-memory-incomplete"),
                    null, runtimeHandle()).create();
            printReport("bootstrap-incomplete",
                    index.startupMemoryEstimate());
            index.close();

            final List<String> loggedReportLines = appender
                    .messagesStartingWith("memory-estimate ")
                    .stream()
                    .map(message -> message
                            .substring("memory-estimate ".length()))
                    .toList();

            assertEquals(index.startupMemoryEstimate().lines(),
                    loggedReportLines);
            assertTrue(appender.countMessageStartingWith("memory-estimate ") > 1);
            assertFalse(index.startupMemoryEstimate().isComplete());
            assertTrue(index.startupMemoryEstimate()
                    .totalEstimatedBytes().isEmpty());
        } finally {
            appender.detach();
        }
    }

    @Test
    void tryOpenReturnsEmptyWithoutConfigurationAndDoesNotAcquireLock() {
        final MemDirectory directory = new MemDirectory();
        final TestLogAppender appender = TestLogAppender.attachToLogger(
                SegmentIndexBootstrapOperation.class.getName(), Level.INFO);

        try {
            final Optional<SegmentIndex<Integer, String>> index = operation(directory,
                    buildConf("bootstrap-operation-try-open-empty",
                            false))
                    .tryOpen();

            assertTrue(index.isEmpty());
            assertFalse(directory.isFileExists(LOCK_FILE_NAME));
            assertFalse(directory.isFileExists(CONFIGURATION_FILE_NAME));
            assertEquals(0,
                    appender.countMessageStartingWith("memory-estimate "));
        } finally {
            appender.detach();
        }
    }

    @Test
    void tryOpenOpensExistingConfiguration() {
        final MemDirectory directory = new MemDirectory();
        operation(directory, buildConf("bootstrap-operation-try-open", false))
                .create()
                .close();

        final Optional<SegmentIndex<Integer, String>> index = operation(directory,
                buildConf("bootstrap-operation-try-open", false))
                .tryOpen();

        assertTrue(index.isPresent());
        try {
            assertTrue(index.get().startupMemoryEstimate()
                    .isComplete());
        } finally {
            index.get().close();
        }
    }

    private static SegmentIndexBootstrapOperation<Integer, String> operation(
            final MemDirectory directory,
            final IndexConfiguration<Integer, String> configuration) {
        return operation(directory, configuration, null);
    }

    private static SegmentIndexBootstrapOperation<Integer, String> operation(
            final MemDirectory directory,
            final IndexConfiguration<Integer, String> configuration,
            final ChunkFilterProviderResolver resolver) {
        return new SegmentIndexBootstrapOperation<>(directory, configuration,
                resolver, runtimeHandle());
    }

    private static SegmentIndexRuntimeHandle runtimeHandle() {
        final RuntimeExecutorPools executorPools = RuntimeExecutorPools.create(
                "hestia-test", 1, 1, 30_000);
        return new SegmentIndexRuntimeHandle() {
            @Override
            public ExecutorRegistry createExecutorRegistry(
                    final String indexName,
                    final boolean contextLoggingEnabled,
                    final int indexMaintenanceThreads,
                    final int registryMaintenanceThreads,
                    final int shutdownTimeoutMillis) {
                return ExecutorRegistry.create(indexName,
                        contextLoggingEnabled, indexMaintenanceThreads,
                        executorPools, registryMaintenanceThreads,
                        shutdownTimeoutMillis);
            }

            @Override
            public String threadNamePrefix() {
                return "hestia-test";
            }

            @Override
            public void close() {
                if (!executorPools.wasClosed()) {
                    executorPools.close();
                }
            }
        };
    }

    private static IndexConfiguration<Integer, String> buildConf(
            final String indexName, final boolean contextLoggingEnabled) {
        return buildConf(indexName, contextLoggingEnabled, 1);
    }

    private static IndexConfiguration<Integer, String> buildConf(
            final String indexName, final boolean contextLoggingEnabled,
            final int registryLifecycleThreads) {
        return buildConf(indexName, contextLoggingEnabled,
                registryLifecycleThreads, null);
    }

    private static IndexConfiguration<Integer, String> buildConf(
            final String indexName, final boolean contextLoggingEnabled,
            final int registryLifecycleThreads,
            final IndexWalConfiguration walConfiguration) {
        final IndexConfigurationBuilder<Integer, String> builder = IndexConfiguration.<Integer, String>builder()
                .identity(identity -> identity.keyClass(Integer.class))
                .identity(identity -> identity
                        .valueClass(String.class))
                .identity(identity -> identity.keyTypeDescriptor(
                        new TypeDescriptorInteger()))
                .identity(identity -> identity.valueTypeDescriptor(
                        new TypeDescriptorShortString()))
                .identity(identity -> identity.name(indexName))
                .logging(logging -> logging
                        .contextEnabled(contextLoggingEnabled))
                .segment(segment -> segment.cacheKeyLimit(10))
                .writePath(writePath -> writePath
                        .segmentWriteCacheKeyLimit(5))
                .writePath(writePath -> writePath
                        .maintenanceWriteCacheKeyLimit(6))
                .segment(segment -> segment.chunkKeyLimit(2))
                .segment(segment -> segment.maxKeys(100))
                .segment(segment -> segment.cachedSegmentLimit(3))
                .bloomFilter(bloomFilter -> bloomFilter
                        .hashFunctions(1))
                .bloomFilter(bloomFilter -> bloomFilter
                        .indexSizeBytes(1024))
                .bloomFilter(bloomFilter -> bloomFilter
                        .falsePositiveProbability(0.01D))
                .io(io -> io.diskBufferSizeBytes(1024))
                .maintenance(maintenance -> maintenance
                        .backgroundAutoEnabled(false))
                .maintenance(maintenance -> maintenance
                        .registryLifecycleThreads(
                                registryLifecycleThreads))
                .filters(filters -> filters.encodingFilters(
                        List.of(new ChunkFilterDoNothing())))
                .filters(filters -> filters.decodingFilters(
                        List.of(new ChunkFilterDoNothing())));
        if (walConfiguration != null) {
            builder.wal(wal -> wal.configuration(walConfiguration));
        }
        return builder.build();
    }

    private static IndexConfiguration<Integer, String> buildConfWithInvalidKeyDescriptor(
            final String indexName) {
        return IndexConfiguration.<Integer, String>builder()
                .identity(identity -> identity.keyClass(Integer.class))
                .identity(identity -> identity.valueClass(String.class))
                .identity(identity -> identity.keyTypeDescriptor(
                        "org.hestiastore.index.DoesNotExistTypeDescriptor"))
                .identity(identity -> identity.valueTypeDescriptor(
                        new TypeDescriptorShortString()))
                .identity(identity -> identity.name(indexName))
                .logging(logging -> logging.contextEnabled(false))
                .segment(segment -> segment.cacheKeyLimit(10))
                .writePath(writePath -> writePath.segmentWriteCacheKeyLimit(5))
                .writePath(writePath -> writePath
                        .maintenanceWriteCacheKeyLimit(6))
                .segment(segment -> segment.chunkKeyLimit(2))
                .segment(segment -> segment.maxKeys(100))
                .segment(segment -> segment.cachedSegmentLimit(3))
                .bloomFilter(bloomFilter -> bloomFilter.hashFunctions(1))
                .bloomFilter(bloomFilter -> bloomFilter.indexSizeBytes(1024))
                .bloomFilter(bloomFilter -> bloomFilter
                        .falsePositiveProbability(0.01D))
                .io(io -> io.diskBufferSizeBytes(1024))
                .maintenance(maintenance -> maintenance
                        .backgroundAutoEnabled(false))
                .maintenance(maintenance -> maintenance
                        .registryLifecycleThreads(1))
                .filters(filters -> filters.encodingFilters(
                        List.of(new ChunkFilterDoNothing())))
                .filters(filters -> filters.decodingFilters(
                        List.of(new ChunkFilterDoNothing())))
                .build();
    }

    private static IndexConfiguration<Integer, String> buildCustomFilterConf(
            final String indexName) {
        final ChunkFilterSpec spec = ChunkFilterSpec
                .ofProvider("bootstrap-filter")
                .withParameter("keyRef", "orders-main");
        return IndexConfiguration.<Integer, String>builder()
                .identity(identity -> identity.keyClass(Integer.class))
                .identity(identity -> identity.valueClass(String.class))
                .identity(identity -> identity
                        .keyTypeDescriptor(new TypeDescriptorInteger()))
                .identity(identity -> identity
                        .valueTypeDescriptor(new TypeDescriptorShortString()))
                .identity(identity -> identity.name(indexName))
                .logging(logging -> logging.contextEnabled(false))
                .segment(segment -> segment.cacheKeyLimit(10))
                .writePath(writePath -> writePath.segmentWriteCacheKeyLimit(5))
                .writePath(writePath -> writePath
                        .maintenanceWriteCacheKeyLimit(6))
                .segment(segment -> segment.chunkKeyLimit(2))
                .segment(segment -> segment.maxKeys(100))
                .segment(segment -> segment.cachedSegmentLimit(3))
                .bloomFilter(bloomFilter -> bloomFilter.hashFunctions(1))
                .bloomFilter(bloomFilter -> bloomFilter.indexSizeBytes(1024))
                .bloomFilter(bloomFilter -> bloomFilter
                        .falsePositiveProbability(0.01D))
                .io(io -> io.diskBufferSizeBytes(1024))
                .maintenance(maintenance -> maintenance
                        .backgroundAutoEnabled(false))
                .maintenance(maintenance -> maintenance
                        .registryLifecycleThreads(1))
                .filters(filters -> filters.addEncodingFilter(spec))
                .filters(filters -> filters.addDecodingFilter(spec))
                .build();
    }

    private static IndexConfiguration<Integer, ByteArray> buildUnknownValueSizeConf(
            final String indexName) {
        return IndexConfiguration.<Integer, ByteArray>builder()
                .identity(identity -> identity.keyClass(Integer.class))
                .identity(identity -> identity.valueClass(ByteArray.class))
                .identity(identity -> identity
                        .keyTypeDescriptor(new TypeDescriptorInteger()))
                .identity(identity -> identity
                        .valueTypeDescriptor(new TypeDescriptorByteArray()))
                .identity(identity -> identity.name(indexName))
                .logging(logging -> logging.contextEnabled(false))
                .segment(segment -> segment.cacheKeyLimit(10))
                .writePath(writePath -> writePath.segmentWriteCacheKeyLimit(5))
                .writePath(writePath -> writePath
                        .maintenanceWriteCacheKeyLimit(6))
                .segment(segment -> segment.chunkKeyLimit(2))
                .segment(segment -> segment.maxKeys(100))
                .segment(segment -> segment.cachedSegmentLimit(3))
                .bloomFilter(bloomFilter -> bloomFilter.hashFunctions(1))
                .bloomFilter(bloomFilter -> bloomFilter.indexSizeBytes(1024))
                .bloomFilter(bloomFilter -> bloomFilter
                        .falsePositiveProbability(0.01D))
                .io(io -> io.diskBufferSizeBytes(1024))
                .maintenance(maintenance -> maintenance
                        .backgroundAutoEnabled(false))
                .maintenance(maintenance -> maintenance
                        .registryLifecycleThreads(1))
                .filters(filters -> filters.encodingFilters(
                        List.of(new ChunkFilterDoNothing())))
                .filters(filters -> filters.decodingFilters(
                        List.of(new ChunkFilterDoNothing())))
                .build();
    }

    private static final class BootstrapChunkFilterProvider
            implements ChunkFilterProvider {

        @Override
        public String getProviderId() {
            return "bootstrap-filter";
        }

        @Override
        public Supplier<? extends ChunkFilter> createEncodingSupplier(
                final ChunkFilterSpec spec) {
            return BootstrapChunkFilter::new;
        }

        @Override
        public Supplier<? extends ChunkFilter> createDecodingSupplier(
                final ChunkFilterSpec spec) {
            return BootstrapChunkFilter::new;
        }
    }

    private static final class BootstrapChunkFilter implements ChunkFilter {

        @Override
        public ChunkData apply(final ChunkData input) {
            return input;
        }
    }

    private static final class FailingFileNamesMemDirectory
            extends MemDirectory {

        @Override
        public Stream<String> getFileNames() {
            throw new IndexException("File names unavailable.");
        }
    }

    private static final class TestLogAppender extends AbstractAppender {

        private final LoggerContext loggerContext;
        private final String loggerConfigName;
        private final List<String> messages = Collections
                .synchronizedList(new ArrayList<>());

        private TestLogAppender(final String name,
                final LoggerContext loggerContext,
                final String loggerConfigName) {
            super(name, null, null, false, null);
            this.loggerContext = loggerContext;
            this.loggerConfigName = loggerConfigName;
        }

        static TestLogAppender attachToLogger(final String loggerName,
                final Level level) {
            final LoggerContext context = (LoggerContext) LogManager
                    .getContext(false);
            final Configuration configuration = context.getConfiguration();
            final LoggerConfig loggerConfig = configuration
                    .getLoggerConfig(loggerName);
            final String appenderName = "test-appender-"
                    + System.nanoTime();
            final TestLogAppender appender = new TestLogAppender(appenderName,
                    context, loggerConfig.getName());
            appender.start();
            loggerConfig.addAppender(appender, level, null);
            context.updateLoggers();
            return appender;
        }

        @Override
        public void append(final LogEvent event) {
            messages.add(event.getMessage().getFormattedMessage());
        }

        long countMessageStartingWith(final String prefix) {
            synchronized (messages) {
                return messages.stream()
                        .filter(message -> message.startsWith(prefix))
                        .count();
            }
        }

        List<String> messagesStartingWith(final String prefix) {
            synchronized (messages) {
                return messages.stream()
                        .filter(message -> message.startsWith(prefix))
                        .toList();
            }
        }

        void detach() {
            final Configuration configuration = loggerContext
                    .getConfiguration();
            final LoggerConfig loggerConfig = configuration
                    .getLoggerConfig(loggerConfigName);
            loggerConfig.removeAppender(getName());
            stop();
            loggerContext.updateLoggers();
        }
    }

    private static void printReport(final String scenario,
            final MemoryEstimateReport report) {
        System.out.println();
        System.out.println("==== memory estimate report: " + scenario
                + " ====");
        System.out.println(report.text());
        System.out.println("==== end memory estimate report: " + scenario
                + " ====");
        assertReadableLineLengths(report);
        assertFalse(report.text().contains(" bytes"));
    }

    private static void assertReadableLineLengths(
            final MemoryEstimateReport report) {
        report.lines().forEach(line -> assertTrue(line.length() <= 100,
                () -> "Report line is too long: " + line));
    }
}
