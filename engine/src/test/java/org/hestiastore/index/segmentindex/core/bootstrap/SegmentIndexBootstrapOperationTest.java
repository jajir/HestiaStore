package org.hestiastore.index.segmentindex.core.bootstrap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.chunkstore.ChunkData;
import org.hestiastore.index.chunkstore.ChunkFilter;
import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.chunkstore.ChunkFilterProvider;
import org.hestiastore.index.chunkstore.ChunkFilterProviderResolver;
import org.hestiastore.index.chunkstore.ChunkFilterProviderResolverImpl;
import org.hestiastore.index.chunkstore.ChunkFilterSpec;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.properties.IndexPropertiesSchema;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentindex.SegmentIndex;
import org.hestiastore.index.segmentindex.configuration.persistence.IndexConfigurationStorage;
import org.hestiastore.index.segmentindex.configuration.user.IndexConfiguration;
import org.hestiastore.index.segmentindex.configuration.user.IndexConfigurationBuilder;
import org.hestiastore.index.segmentindex.configuration.user.IndexWalConfiguration;
import org.hestiastore.index.segmentindex.core.session.IndexContextLoggingAdapter;
import org.hestiastore.index.segmentindex.core.session.SegmentIndexResourceClosingAdapter;
import org.hestiastore.index.segmentindex.core.session.SegmentIndexSessionResources;
import org.hestiastore.index.segmentindex.core.session.SegmentIndexTestAccess;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

class SegmentIndexBootstrapOperationTest {

    private static final String LOCK_FILE_NAME = ".lock";
    private static final String CONFIGURATION_FILE_NAME =
            IndexPropertiesSchema.IndexConfigurationKeys.CONFIGURATION_FILENAME;
    private static final String MDC_INDEX_NAME_KEY = "index.name";

    @AfterEach
    void tearDown() {
        MDC.clear();
    }

    @Test
    void createLoadsDefaultsAndSavesConfiguration() {
        final MemDirectory directory = new MemDirectory();

        SegmentIndexBootstrapOperation.create(directory,
                buildConf("bootstrap-operation-create", false), null)
                .create()
                .close();

        assertTrue(directory.isFileExists(CONFIGURATION_FILE_NAME));
        final var loaded = new IndexConfigurationStorage<Integer, String>(directory)
                .load();
        assertEquals("bootstrap-operation-create", loaded.identity().name());
        assertEquals(Integer.class, loaded.identity().keyClass());
        assertEquals(String.class, loaded.identity().valueClass());
    }

    @Test
    void createFailsWhenConfigurationAlreadyExistsAndDoesNotAcquireLock() {
        final MemDirectory directory = new MemDirectory();
        SegmentIndexBootstrapOperation.create(directory,
                buildConf("bootstrap-operation-create-existing", false), null)
                .create()
                .close();

        assertThrows(IndexException.class,
                () -> SegmentIndexBootstrapOperation.create(directory,
                        buildConf("bootstrap-operation-create-existing-new",
                                false),
                        null)
                        .create());

        assertFalse(directory.isFileExists(LOCK_FILE_NAME));
        assertEquals("bootstrap-operation-create-existing",
                new IndexConfigurationStorage<Integer, String>(directory)
                        .load().identity().name());
    }

    @Test
    void createWrapsRuntimeIndexWithContextLoggingWhenEnabled() {
        final SegmentIndex<Integer, String> index = SegmentIndexBootstrapOperation.create(new MemDirectory(),
                buildConf("bootstrap-operation-logging", true),
                null)
                .create();

        try {
            assertInstanceOf(SegmentIndexResourceClosingAdapter.class, index);
            assertInstanceOf(IndexContextLoggingAdapter.class,
                    wrappedIndex(index));
        } finally {
            index.close();
        }
    }

    @Test
    void descriptorResolutionFailureKeepsLockAndDoesNotWriteConfiguration() {
        final MemDirectory directory = new MemDirectory();
        final SegmentIndexBootstrapRequest<Integer, String> request = request(
                directory, buildConfWithInvalidKeyDescriptor(
                        "bootstrap-operation-invalid-descriptor"), true);
        final SegmentIndexBootstrapState<Integer, String> state =
                new SegmentIndexBootstrapState<>();

        assertThrows(RuntimeException.class,
                () -> runBootstrapLikeOperation(request, state));

        assertTrue(directory.isFileExists(LOCK_FILE_NAME));
        assertFalse(directory.isFileExists(CONFIGURATION_FILE_NAME));
        assertThrows(IllegalStateException.class,
                state::getExecutorRegistry);
    }

    @Test
    void failureAfterExecutorCreationKeepsLockAndClosesExecutorRegistry() {
        final MemDirectory directory = new MemDirectory();
        final SegmentIndexBootstrapRequest<Integer, String> request = request(
                directory,
                buildConf("bootstrap-operation-executor-cleanup", false),
                true);
        final SegmentIndexBootstrapState<Integer, String> state =
                new SegmentIndexBootstrapState<>();

        assertThrows(RuntimeException.class,
                () -> runBootstrapLikeOperation(request, state,
                        stepsThroughExecutorThen(failingStep())));

        assertTrue(directory.isFileExists(LOCK_FILE_NAME));
        assertTrue(state.getExecutorRegistry().wasClosed());
    }

    @Test
    void failureAfterLockCreationKeepsLockAndClosesExecutorRegistry() {
        final FailingSubDirectoryMemDirectory directory = new FailingSubDirectoryMemDirectory();
        final SegmentIndexBootstrapRequest<Integer, String> request = request(
                directory, buildConf("bootstrap-operation-lock-kept", false),
                true);
        final SegmentIndexBootstrapState<Integer, String> state =
                new SegmentIndexBootstrapState<>();

        assertThrows(RuntimeException.class,
                () -> runBootstrapLikeOperation(request, state));

        assertTrue(directory.isFileExists(LOCK_FILE_NAME));
        assertTrue(state.getExecutorRegistry().wasClosed());
    }

    @Test
    void failureAfterRuntimeCreationClosesRuntimeResourcesAndKeepsLock() {
        final MemDirectory directory = new MemDirectory();
        final SegmentIndexBootstrapRequest<Integer, String> request = request(
                directory,
                buildConfWithWal("bootstrap-operation-runtime-cleanup"),
                true);
        final SegmentIndexBootstrapState<Integer, String> state =
                new SegmentIndexBootstrapState<>();
        assertThrows(RuntimeException.class,
                () -> runBootstrapLikeOperation(request, state,
                        stepsThroughRuntimeThen(failingStep())));

        assertTrue(directory.isFileExists(LOCK_FILE_NAME));
        assertTrue(state.getExecutorRegistry().wasClosed());
        assertTrue(SegmentIndexTestAccess
                .keyToSegmentMap(state.getInternalIndex())
                .wasClosed());
        assertWalClosed(state);
    }

    @Test
    void failureAfterCoreStorageClosesCoreStorage() {
        final SegmentIndexBootstrapState<Integer, String> state =
                runBootstrapExpectingFailureAfterRuntimeStep(4,
                        "bootstrap-operation-core-cleanup");

        assertTrue(state.getKeyToSegmentMap().wasClosed());
    }

    @Test
    void failureAfterRuntimeTopologyClosesSplitAndCoreStorage() {
        final SegmentIndexBootstrapState<Integer, String> state =
                runBootstrapExpectingFailureAfterRuntimeStep(5,
                        "bootstrap-operation-topology-cleanup");

        assertSplitServiceClosed(state);
        assertTrue(state.getKeyToSegmentMap().wasClosed());
    }

    @Test
    void failureAfterRuntimeWalClosesWalSplitAndCoreStorage() {
        final SegmentIndexBootstrapState<Integer, String> state =
                runBootstrapExpectingFailureAfterRuntimeStep(6,
                        "bootstrap-operation-wal-cleanup");

        assertWalClosed(state);
        assertSplitServiceClosed(state);
        assertTrue(state.getKeyToSegmentMap().wasClosed());
    }

    @Test
    void failureAfterRuntimeServicesClosesEarlierRuntimeResources() {
        final SegmentIndexBootstrapState<Integer, String> state =
                runBootstrapExpectingFailureAfterRuntimeStep(7,
                        "bootstrap-operation-services-cleanup");

        assertWalClosed(state);
        assertSplitServiceClosed(state);
        assertTrue(state.getKeyToSegmentMap().wasClosed());
    }

    @Test
    void failureAfterRuntimeCreationClosesEarlierRuntimeResources() {
        final SegmentIndexBootstrapState<Integer, String> state =
                runBootstrapExpectingFailureAfterRuntimeStep(8,
                        "bootstrap-operation-runtime-creation-cleanup");

        assertWalClosed(state);
        assertSplitServiceClosed(state);
        assertTrue(state.getKeyToSegmentMap().wasClosed());
    }

    @Test
    void successfulCloseReleasesLockAndClosesExecutorRegistry() {
        final MemDirectory directory = new MemDirectory();
        final SegmentIndexBootstrapRequest<Integer, String> request = request(
                directory,
                buildConf("bootstrap-operation-successful-close", false),
                true);
        final SegmentIndexBootstrapState<Integer, String> state =
                new SegmentIndexBootstrapState<>();

        runBootstrapLikeOperation(request, state);

        assertTrue(directory.isFileExists(LOCK_FILE_NAME));
        state.getIndex().close();
        assertFalse(directory.isFileExists(LOCK_FILE_NAME));
        assertTrue(state.getExecutorRegistry().wasClosed());
    }

    @Test
    void createWrapsConcurrentRuntimeIndexWhenContextLoggingDisabled() {
        final SegmentIndex<Integer, String> index = SegmentIndexBootstrapOperation.create(new MemDirectory(),
                buildConf("bootstrap-operation-plain", false), null)
                .create();

        try {
            assertInstanceOf(SegmentIndexResourceClosingAdapter.class, index);
            assertFalse(wrappedIndex(index) instanceof IndexContextLoggingAdapter);
        } finally {
            index.close();
        }
    }

    @Test
    void scopedStartupApplyAndRollbackRunInsideIndexMdcScope() {
        final MemDirectory directory = new MemDirectory();
        final RuntimeException failure = new IllegalStateException(
                "bootstrap failed");
        final List<String> calls = new ArrayList<>();
        final SegmentIndexBootstrapRequest<Integer, String> request = request(
                directory, buildConf("bootstrap-operation-mdc", true), true);
        final SegmentIndexBootstrapState<Integer, String> state =
                new SegmentIndexBootstrapState<>();

        final RuntimeException thrown = assertThrows(RuntimeException.class,
                () -> runBootstrapLikeOperation(request, state,
                        stepsThroughMdcCallWrapperThen(
                                mdcRecordingStep("resource", calls, failure))));

        assertSame(failure, thrown);
        assertEquals(List.of("apply resource:bootstrap-operation-mdc",
                "close resource:bootstrap-operation-mdc"), calls);
        assertNull(MDC.get(MDC_INDEX_NAME_KEY));
    }

    @Test
    void scopedStartupFailureRestoresExistingIndexMdcScope() {
        final MemDirectory directory = new MemDirectory();
        final SegmentIndexBootstrapRequest<Integer, String> request = request(
                directory, buildConf("bootstrap-operation-mdc-restore", true),
                true);
        final SegmentIndexBootstrapState<Integer, String> state =
                new SegmentIndexBootstrapState<>();

        MDC.put(MDC_INDEX_NAME_KEY, "outer");
        assertThrows(RuntimeException.class,
                () -> runBootstrapLikeOperation(request, state,
                        stepsThroughMdcCallWrapperThen(failingStep())));

        assertEquals("outer", MDC.get(MDC_INDEX_NAME_KEY));
    }

    @Test
    void openMergesStoredConfigurationWithOverrides() {
        final MemDirectory directory = new MemDirectory();
        SegmentIndexBootstrapOperation.create(directory,
                buildConf("bootstrap-operation-open", false, 1), null)
                .create()
                .close();

        final SegmentIndex<Integer, String> index = SegmentIndexBootstrapOperation.create(directory,
                buildConf("bootstrap-operation-open", false, 2), null)
                .open();

        try {
            assertEquals(2,
                    new IndexConfigurationStorage<Integer, String>(directory)
                            .load()
                            .maintenance().registryLifecycleThreads());
        } finally {
            index.close();
        }
    }

    @Test
    void openDescriptorOverrideFailureKeepsLockAndDoesNotPersistMergedConfiguration() {
        final MemDirectory directory = new MemDirectory();
        SegmentIndexBootstrapOperation.create(directory,
                buildConf("bootstrap-operation-open-invalid", false, 1), null)
                .create()
                .close();

        assertThrows(RuntimeException.class,
                () -> SegmentIndexBootstrapOperation.create(directory,
                        buildConfWithInvalidKeyDescriptor(
                                "bootstrap-operation-open-invalid"),
                        null).open());

        assertTrue(directory.isFileExists(LOCK_FILE_NAME));
        assertEquals(1,
                new IndexConfigurationStorage<Integer, String>(directory)
                        .load().maintenance().registryLifecycleThreads());
    }

    @Test
    void explicitProviderResolverIsUsedForPersistedCustomChunkFilters() {
        final MemDirectory directory = new MemDirectory();
        final ChunkFilterProviderResolver resolver = ChunkFilterProviderResolverImpl.builder().withDefaultProviders()
                .withProvider(new BootstrapChunkFilterProvider())
                .build();
        final IndexConfiguration<Integer, String> original = buildCustomFilterConf("bootstrap-operation-provider");

        SegmentIndexBootstrapOperation.create(directory, original, resolver)
                .create().close();

        final SegmentIndex<Integer, String> index = SegmentIndexBootstrapOperation.create(directory,
                buildCustomFilterConf("bootstrap-operation-provider"),
                resolver)
                .open();

        try {
            final var loaded = new IndexConfigurationStorage<Integer, String>(directory,
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
    void tryOpenReturnsEmptyWithoutConfigurationAndDoesNotAcquireLock() {
        final MemDirectory directory = new MemDirectory();

        final Optional<SegmentIndexResourceClosingAdapter<Integer, String>> index =
                SegmentIndexBootstrapOperation.create(directory,
                        buildConf("bootstrap-operation-try-open-empty", false),
                        null)
                        .tryOpen();

        assertTrue(index.isEmpty());
        assertFalse(directory.isFileExists(LOCK_FILE_NAME));
        assertFalse(directory.isFileExists(CONFIGURATION_FILE_NAME));
    }

    @Test
    void tryOpenOpensExistingConfiguration() {
        final MemDirectory directory = new MemDirectory();
        SegmentIndexBootstrapOperation.create(directory,
                buildConf("bootstrap-operation-try-open", false), null)
                .create()
                .close();

        final Optional<SegmentIndexResourceClosingAdapter<Integer, String>> index =
                SegmentIndexBootstrapOperation.create(directory,
                        buildConf("bootstrap-operation-try-open", false),
                        null)
                        .tryOpen();

        assertTrue(index.isPresent());
        index.get().close();
    }

    private static Object wrappedIndex(final SegmentIndex<Integer, String> index) {
        try {
            final Field field = index.getClass().getDeclaredField("delegate");
            field.setAccessible(true);
            return field.get(index);
        } catch (final ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
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
        final IndexConfigurationBuilder<Integer, String> builder =
                IndexConfiguration.<Integer, String>builder()
                .identity(identity -> identity.keyClass(Integer.class))
                .identity(identity -> identity.valueClass(String.class))
                .identity(identity -> identity.keyTypeDescriptor(new TypeDescriptorInteger()))
                .identity(identity -> identity.valueTypeDescriptor(new TypeDescriptorShortString()))
                .identity(identity -> identity.name(indexName))
                .logging(logging -> logging.contextEnabled(contextLoggingEnabled))
                .segment(segment -> segment.cacheKeyLimit(10))
                .writePath(writePath -> writePath.segmentWriteCacheKeyLimit(5))
                .writePath(writePath -> writePath.maintenanceWriteCacheKeyLimit(6))
                .segment(segment -> segment.chunkKeyLimit(2))
                .segment(segment -> segment.maxKeys(100))
                .segment(segment -> segment.cachedSegmentLimit(3))
                .bloomFilter(bloomFilter -> bloomFilter.hashFunctions(1))
                .bloomFilter(bloomFilter -> bloomFilter.indexSizeBytes(1024))
                .bloomFilter(bloomFilter -> bloomFilter.falsePositiveProbability(0.01D))
                .io(io -> io.diskBufferSizeBytes(1024))
                .maintenance(maintenance -> maintenance.backgroundAutoEnabled(false))
                .maintenance(maintenance -> maintenance.segmentThreads(1))
                .maintenance(maintenance -> maintenance.registryLifecycleThreads(registryLifecycleThreads))
                .filters(filters -> filters.encodingFilters(List.of(new ChunkFilterDoNothing())))
                .filters(filters -> filters.decodingFilters(List.of(new ChunkFilterDoNothing())));
        if (walConfiguration != null) {
            builder.wal(wal -> wal.configuration(walConfiguration));
        }
        return builder.build();
    }

    private static IndexConfiguration<Integer, String> buildConfWithWal(
            final String indexName) {
        return buildConf(indexName, false, 1,
                IndexWalConfiguration.builder().build());
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
                .maintenance(maintenance -> maintenance.segmentThreads(1))
                .maintenance(maintenance -> maintenance
                        .registryLifecycleThreads(1))
                .filters(filters -> filters.encodingFilters(
                        List.of(new ChunkFilterDoNothing())))
                .filters(filters -> filters.decodingFilters(
                        List.of(new ChunkFilterDoNothing())))
                .build();
    }

    private static SegmentIndexBootstrapRequest<Integer, String> request(
            final MemDirectory directory,
            final IndexConfiguration<Integer, String> conf,
            final boolean createIndex) {
        return request(directory, conf, createIndex
                ? SegmentIndexBootstrapMode.CREATE
                : SegmentIndexBootstrapMode.OPEN);
    }

    private static SegmentIndexBootstrapRequest<Integer, String> request(
            final MemDirectory directory,
            final IndexConfiguration<Integer, String> conf,
            final SegmentIndexBootstrapMode mode) {
        return new SegmentIndexBootstrapRequest<>(directory, conf, null,
                mode);
    }

    private static void runBootstrapLikeOperation(
            final SegmentIndexBootstrapRequest<Integer, String> request,
            final SegmentIndexBootstrapState<Integer, String> state) {
        runBootstrapLikeOperation(request, state,
                SegmentIndexBootstrapSteps.<Integer, String>startingSteps());
    }

    private static void runBootstrapLikeOperation(
            final SegmentIndexBootstrapRequest<Integer, String> request,
            final SegmentIndexBootstrapState<Integer, String> state,
            final List<SegmentIndexBootstrapStep<Integer, String>> steps) {
        new SegmentIndexBootstrapPipeline<>(steps).run(request, state);
    }

    private static List<SegmentIndexBootstrapStep<Integer, String>> stepsThroughExecutorThen(
            final SegmentIndexBootstrapStep<Integer, String> nextStep) {
        final SegmentIndexSessionResources<Integer, String> sessionResources =
                new SegmentIndexSessionResources<>();
        final List<SegmentIndexBootstrapStep<Integer, String>> steps =
                commonStepsThroughExecutor(sessionResources);
        steps.add(nextStep);
        return List.copyOf(steps);
    }

    private static List<SegmentIndexBootstrapStep<Integer, String>> stepsThroughRuntimeThen(
            final SegmentIndexBootstrapStep<Integer, String> nextStep) {
        final SegmentIndexSessionResources<Integer, String> sessionResources =
                new SegmentIndexSessionResources<>();
        final List<SegmentIndexBootstrapStep<Integer, String>> steps =
                commonStepsThroughExecutor(sessionResources);
        steps.add(new BootstrapStepCreateSessionInfrastructure<>(
                sessionResources));
        addRuntimeSteps(steps, sessionResources, 8);
        steps.add(new BootstrapStepCreateIndex<>(sessionResources));
        steps.add(nextStep);
        return List.copyOf(steps);
    }

    private static List<SegmentIndexBootstrapStep<Integer, String>> stepsThroughRuntimeStepThen(
            final int runtimeStepCount,
            final SegmentIndexBootstrapStep<Integer, String> nextStep) {
        final SegmentIndexSessionResources<Integer, String> sessionResources =
                new SegmentIndexSessionResources<>();
        final List<SegmentIndexBootstrapStep<Integer, String>> steps =
                commonStepsThroughExecutor(sessionResources);
        steps.add(new BootstrapStepCreateSessionInfrastructure<>(
                sessionResources));
        addRuntimeSteps(steps, sessionResources, runtimeStepCount);
        steps.add(nextStep);
        return List.copyOf(steps);
    }

    private static void addRuntimeSteps(
            final List<SegmentIndexBootstrapStep<Integer, String>> steps,
            final SegmentIndexSessionResources<Integer, String> sessionResources,
            final int runtimeStepCount) {
        final List<SegmentIndexBootstrapStep<Integer, String>> runtimeSteps =
                List.of(new BootstrapStepOpenKeyToSegmentMap<>(),
                        new BootstrapStepCreateChunkStoreCache<>(),
                        new BootstrapStepOpenSegmentRegistry<>(),
                        new BootstrapStepOpenCoreStorage<>(),
                        new BootstrapStepCreateRuntimeTopology<>(
                                sessionResources),
                        new BootstrapStepOpenRuntimeWal<>(),
                        new BootstrapStepCreateRuntimeServices<>(
                                sessionResources),
                        new BootstrapStepCreateRuntime<>(sessionResources));
        steps.addAll(runtimeSteps.subList(0, runtimeStepCount));
    }

    private static SegmentIndexBootstrapState<Integer, String> runBootstrapExpectingFailureAfterRuntimeStep(
            final int runtimeStepCount, final String indexName) {
        final MemDirectory directory = new MemDirectory();
        final SegmentIndexBootstrapRequest<Integer, String> request = request(
                directory, buildConfWithWal(indexName), true);
        final SegmentIndexBootstrapState<Integer, String> state =
                new SegmentIndexBootstrapState<>();

        assertThrows(RuntimeException.class,
                () -> runBootstrapLikeOperation(request, state,
                        stepsThroughRuntimeStepThen(runtimeStepCount,
                                failingStep())));

        assertTrue(directory.isFileExists(LOCK_FILE_NAME));
        assertTrue(state.getExecutorRegistry().wasClosed());
        return state;
    }

    private static void assertSplitServiceClosed(
            final SegmentIndexBootstrapState<Integer, String> state) {
        assertThrows(RuntimeException.class,
                () -> state.getRuntimeSplitService()
                        .hintSplitCandidate(SegmentId.of(1)));
    }

    private static void assertWalClosed(
            final SegmentIndexBootstrapState<Integer, String> state) {
        assertThrows(RuntimeException.class,
                () -> state.getRuntimeWalRuntime().appendPut(1, "one"));
    }

    private static List<SegmentIndexBootstrapStep<Integer, String>> stepsThroughMdcCallWrapperThen(
            final SegmentIndexBootstrapStep<Integer, String> nextStep) {
        final SegmentIndexSessionResources<Integer, String> sessionResources =
                new SegmentIndexSessionResources<>();
        return List.of(new BootstrapStepAcquireDirectoryLock<>(sessionResources),
                SegmentIndexBootstrapSteps.resolveConfiguration(),
                SegmentIndexBootstrapSteps.createMdcCallWrapper(), nextStep);
    }

    private static List<SegmentIndexBootstrapStep<Integer, String>> commonStepsThroughExecutor(
            final SegmentIndexSessionResources<Integer, String> sessionResources) {
        final List<SegmentIndexBootstrapStep<Integer, String>> steps =
                new ArrayList<>();
        steps.add(new BootstrapStepAcquireDirectoryLock<>(sessionResources));
        steps.add(SegmentIndexBootstrapSteps.resolveConfiguration());
        steps.add(SegmentIndexBootstrapSteps.createMdcCallWrapper());
        steps.add(SegmentIndexBootstrapSteps.resolveTypeDescriptors());
        steps.add(SegmentIndexBootstrapSteps.writeConfiguration());
        steps.add(SegmentIndexBootstrapSteps.createExecutorRegistry());
        return steps;
    }

    private static SegmentIndexBootstrapStep<Integer, String> failingStep() {
        return new SegmentIndexBootstrapStep<>() {
            @Override
            void apply(
                    final SegmentIndexBootstrapRequest<Integer, String> request,
                    final SegmentIndexBootstrapState<Integer, String> state) {
                throw new IllegalStateException("bootstrap failed");
            }
        };
    }

    private static SegmentIndexBootstrapStep<Integer, String> mdcRecordingStep(
            final String name, final List<String> calls,
            final RuntimeException failure) {
        return new SegmentIndexBootstrapStep<>() {

            @Override
            void apply(
                    final SegmentIndexBootstrapRequest<Integer, String> request,
                    final SegmentIndexBootstrapState<Integer, String> state) {
                calls.add("apply " + name + ":"
                        + MDC.get(MDC_INDEX_NAME_KEY));
                throw failure;
            }

            @Override
            void closeResource() {
                calls.add("close " + name + ":"
                        + MDC.get(MDC_INDEX_NAME_KEY));
            }
        };
    }

    private static IndexConfiguration<Integer, String> buildCustomFilterConf(
            final String indexName) {
        final ChunkFilterSpec spec = ChunkFilterSpec
                .ofProvider("bootstrap-filter")
                .withParameter("keyRef", "orders-main");
        return IndexConfiguration.<Integer, String>builder()
                .identity(identity -> identity.keyClass(Integer.class))
                .identity(identity -> identity.valueClass(String.class))
                .identity(identity -> identity.keyTypeDescriptor(new TypeDescriptorInteger()))
                .identity(identity -> identity.valueTypeDescriptor(new TypeDescriptorShortString()))
                .identity(identity -> identity.name(indexName))
                .logging(logging -> logging.contextEnabled(false))
                .segment(segment -> segment.cacheKeyLimit(10))
                .writePath(writePath -> writePath.segmentWriteCacheKeyLimit(5))
                .writePath(writePath -> writePath.maintenanceWriteCacheKeyLimit(6))
                .segment(segment -> segment.chunkKeyLimit(2))
                .segment(segment -> segment.maxKeys(100))
                .segment(segment -> segment.cachedSegmentLimit(3))
                .bloomFilter(bloomFilter -> bloomFilter.hashFunctions(1))
                .bloomFilter(bloomFilter -> bloomFilter.indexSizeBytes(1024))
                .bloomFilter(bloomFilter -> bloomFilter.falsePositiveProbability(0.01D))
                .io(io -> io.diskBufferSizeBytes(1024))
                .maintenance(maintenance -> maintenance.backgroundAutoEnabled(false))
                .maintenance(maintenance -> maintenance.segmentThreads(1))
                .maintenance(maintenance -> maintenance.registryLifecycleThreads(1))
                .filters(filters -> filters.addEncodingFilter(spec))
                .filters(filters -> filters.addDecodingFilter(spec))
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

    public static final class BootstrapChunkFilter implements ChunkFilter {

        @Override
        public ChunkData apply(final ChunkData input) {
            return input;
        }
    }

    private static final class FailingSubDirectoryMemDirectory
            extends MemDirectory {

        @Override
        public Directory openSubDirectory(final String directoryName) {
            throw new IndexException("Subdirectory open failed.");
        }
    }
}
