package org.hestiastore.index.segmentindex.configuration.effective;

import java.util.List;
import java.util.Optional;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.chunkstore.ChunkFilterProviderResolver;
import org.hestiastore.index.chunkstore.ChunkFilterSpec;
import org.hestiastore.index.chunkstore.ChunkFilterSpecs;
import org.hestiastore.index.segmentindex.IndexBloomFilterConfiguration;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.IndexConfigurationContract;
import org.hestiastore.index.segmentindex.IndexFilterConfiguration;
import org.hestiastore.index.segmentindex.IndexIdentityConfiguration;
import org.hestiastore.index.segmentindex.IndexMaintenanceConfiguration;
import org.hestiastore.index.segmentindex.IndexSegmentConfiguration;
import org.hestiastore.index.segmentindex.IndexWritePathConfiguration;
import org.hestiastore.index.segmentindex.configuration.DataTypeDescriptorRegistry;
import org.hestiastore.index.segmentindex.configuration.defaults.IndexConfigurationRegistry;

/**
 * Resolves nullable user configuration requests into complete effective
 * configurations.
 */
public final class EffectiveIndexConfigurationResolver {

    private EffectiveIndexConfigurationResolver() {
    }

    public static <K, V> EffectiveIndexConfiguration<K, V> resolveForCreate(
            final IndexConfiguration<K, V> request) {
        final IndexConfiguration<K, V> validatedRequest = Vldtn.requireNonNull(
                request, "request");
        return resolveForCreate(validatedRequest,
                validatedRequest.filters().getChunkFilterProviderResolver());
    }

    public static <K, V> EffectiveIndexConfiguration<K, V> resolveForCreate(
            final IndexConfiguration<K, V> request,
            final ChunkFilterProviderResolver chunkFilterProviderResolver) {
        final IndexConfiguration<K, V> validatedRequest = Vldtn.requireNonNull(
                request, "request");
        final ChunkFilterProviderResolver validatedResolver = Vldtn
                .requireNonNull(chunkFilterProviderResolver,
                        "chunkFilterProviderResolver");
        validateRequiredDatatypesAndIndexName(validatedRequest);
        final IndexConfigurationContract defaults = defaultsFor(
                validatedRequest);
        return buildEffective(validatedRequest, defaults, validatedResolver);
    }

    public static <K, V> EffectiveIndexConfiguration<K, V> mergeWithStored(
            final EffectiveIndexConfiguration<K, V> stored,
            final IndexConfiguration<K, V> request) {
        final IndexConfiguration<K, V> validatedRequest = Vldtn.requireNonNull(
                request, "request");
        return mergeWithStored(stored, validatedRequest,
                validatedRequest.filters().getChunkFilterProviderResolver());
    }

    public static <K, V> EffectiveIndexConfiguration<K, V> mergeWithStored(
            final EffectiveIndexConfiguration<K, V> stored,
            final IndexConfiguration<K, V> request,
            final ChunkFilterProviderResolver chunkFilterProviderResolver) {
        final EffectiveIndexConfiguration<K, V> validatedStored =
                Vldtn.requireNonNull(stored, "stored");
        final IndexConfiguration<K, V> validatedRequest = Vldtn.requireNonNull(
                request, "request");
        final ChunkFilterProviderResolver validatedResolver = Vldtn
                .requireNonNull(chunkFilterProviderResolver,
                        "chunkFilterProviderResolver");
        validateThatFixedPropertiesAreNotOverridden(validatedStored,
                validatedRequest);
        return new EffectiveIndexConfiguration<>(
                mergeIdentity(validatedStored, validatedRequest),
                mergeSegment(validatedStored, validatedRequest),
                mergeWritePath(validatedStored, validatedRequest),
                mergeBloomFilter(validatedStored),
                mergeMaintenance(validatedStored, validatedRequest),
                mergeIo(validatedStored, validatedRequest),
                mergeLogging(validatedStored, validatedRequest),
                validatedStored.wal(),
                mergeFilters(validatedStored, validatedResolver));
    }

    private static <K, V> EffectiveIndexConfiguration<K, V> buildEffective(
            final IndexConfiguration<K, V> request,
            final IndexConfigurationContract defaults,
            final ChunkFilterProviderResolver chunkFilterProviderResolver) {
        final IndexSegmentConfiguration defaultSegment = defaults.segment();
        final IndexWritePathConfiguration defaultWritePath =
                defaults.writePath();
        final int segmentSplitKeyThreshold = intOr(
                request.writePath().segmentSplitKeyThreshold(),
                intOr(request.segment().maxKeys(),
                        defaultWritePath.segmentSplitKeyThreshold()));
        final int maxKeys = intOr(request.segment().maxKeys(),
                intOr(request.writePath().segmentSplitKeyThreshold(),
                        defaultSegment.maxKeys()));
        final int segmentCacheKeyLimit = intOr(request.segment().cacheKeyLimit(),
                defaultSegment.cacheKeyLimit());
        final int cachedSegmentLimit = intOr(
                request.segment().cachedSegmentLimit(),
                defaultSegment.cachedSegmentLimit());
        final int segmentWriteCacheKeyLimit = intOr(
                request.writePath().segmentWriteCacheKeyLimit(),
                Math.max(1, segmentCacheKeyLimit / 2));
        final int maintenanceWriteCacheKeyLimit = intOr(request.writePath()
                .segmentWriteCacheKeyLimitDuringMaintenance(),
                Math.max(segmentWriteCacheKeyLimit + 1,
                        (int) Math.ceil(segmentWriteCacheKeyLimit * 1.4d)));
        final int indexBufferedWriteKeyLimit = intOr(
                request.writePath().indexBufferedWriteKeyLimit(),
                Math.max(maintenanceWriteCacheKeyLimit,
                        maintenanceWriteCacheKeyLimit
                                * Math.max(1, cachedSegmentLimit)));
        final int chunkKeyLimit = intOr(request.segment().chunkKeyLimit(),
                Math.min(defaultSegment.chunkKeyLimit(), maxKeys));
        return new EffectiveIndexConfiguration<>(
                effectiveIdentity(request),
                new EffectiveIndexSegmentConfiguration(maxKeys,
                        chunkKeyLimit, segmentCacheKeyLimit,
                        cachedSegmentLimit, intOr(
                                request.segment().deltaCacheFileLimit(),
                                defaultSegment.deltaCacheFileLimit())),
                new EffectiveIndexWritePathConfiguration(
                        segmentWriteCacheKeyLimit, maintenanceWriteCacheKeyLimit,
                        indexBufferedWriteKeyLimit, segmentSplitKeyThreshold),
                effectiveBloomFilter(request, defaults),
                effectiveMaintenance(request, defaults),
                new EffectiveIndexIoConfiguration(
                        intOr(request.io().diskBufferSizeBytes(),
                                defaults.io().diskBufferSizeBytes())),
                new EffectiveIndexLoggingConfiguration(booleanOr(
                        request.logging().contextEnabled(),
                        defaults.logging().contextEnabled())),
                EffectiveIndexWalConfiguration.fromIndexWalConfiguration(
                        request.wal()),
                effectiveFilters(request, defaults, chunkFilterProviderResolver));
    }

    private static <K, V> EffectiveIndexIdentityConfiguration<K, V> effectiveIdentity(
            final IndexConfiguration<K, V> request) {
        final IndexIdentityConfiguration<K, V> identity = request.identity();
        final String keyTypeDescriptor = identity.keyTypeDescriptor() == null
                ? DataTypeDescriptorRegistry
                        .getTypeDescriptor(identity.keyClass())
                : identity.keyTypeDescriptor();
        final String valueTypeDescriptor = identity.valueTypeDescriptor() == null
                ? DataTypeDescriptorRegistry
                        .getTypeDescriptor(identity.valueClass())
                : identity.valueTypeDescriptor();
        return new EffectiveIndexIdentityConfiguration<>(identity.name(),
                identity.keyClass(), identity.valueClass(), keyTypeDescriptor,
                valueTypeDescriptor);
    }

    private static <K, V> EffectiveIndexBloomFilterConfiguration effectiveBloomFilter(
            final IndexConfiguration<K, V> request,
            final IndexConfigurationContract defaults) {
        final IndexBloomFilterConfiguration bloomFilter = request.bloomFilter();
        final IndexBloomFilterConfiguration defaultBloomFilter =
                defaults.bloomFilter();
        return new EffectiveIndexBloomFilterConfiguration(
                intOr(bloomFilter.hashFunctions(),
                        defaultBloomFilter.hashFunctions()),
                intOr(bloomFilter.indexSizeBytes(),
                        defaultBloomFilter.indexSizeBytes()),
                doubleOr(bloomFilter.falsePositiveProbability(),
                        defaultBloomFilter.falsePositiveProbability()));
    }

    private static <K, V> EffectiveIndexMaintenanceConfiguration effectiveMaintenance(
            final IndexConfiguration<K, V> request,
            final IndexConfigurationContract defaults) {
        final IndexMaintenanceConfiguration maintenance = request.maintenance();
        final IndexMaintenanceConfiguration defaultMaintenance =
                defaults.maintenance();
        return new EffectiveIndexMaintenanceConfiguration(
                intOr(maintenance.segmentThreads(),
                        defaultMaintenance.segmentThreads()),
                intOr(maintenance.indexThreads(),
                        defaultMaintenance.indexThreads()),
                intOr(maintenance.registryLifecycleThreads(),
                        defaultMaintenance.registryLifecycleThreads()),
                intOr(maintenance.busyBackoffMillis(),
                        defaultMaintenance.busyBackoffMillis()),
                intOr(maintenance.busyTimeoutMillis(),
                        defaultMaintenance.busyTimeoutMillis()),
                booleanOr(maintenance.backgroundAutoEnabled(),
                        defaultMaintenance.backgroundAutoEnabled()));
    }

    private static <K, V> EffectiveIndexFilterConfiguration effectiveFilters(
            final IndexConfiguration<K, V> request,
            final IndexConfigurationContract defaults,
            final ChunkFilterProviderResolver chunkFilterProviderResolver) {
        final IndexFilterConfiguration requestFilters = request.filters();
        final IndexFilterConfiguration defaultFilters = defaults.filters();
        final List<ChunkFilterSpec> encoding = requestFilters
                .encodingChunkFilterSpecs().isEmpty()
                        ? defaultFilters.encodingChunkFilterSpecs()
                        : requestFilters.encodingChunkFilterSpecs();
        final List<ChunkFilterSpec> decoding = requestFilters
                .decodingChunkFilterSpecs().isEmpty()
                        ? defaultFilters.decodingChunkFilterSpecs()
                        : requestFilters.decodingChunkFilterSpecs();
        return EffectiveIndexFilterConfiguration.fromSpecs(encoding, decoding,
                chunkFilterProviderResolver);
    }

    private static <K, V> EffectiveIndexIdentityConfiguration<K, V> mergeIdentity(
            final EffectiveIndexConfiguration<K, V> stored,
            final IndexConfiguration<K, V> request) {
        final String name = request.identity().name() == null
                ? stored.identity().name()
                : request.identity().name();
        return new EffectiveIndexIdentityConfiguration<>(name,
                stored.identity().keyClass(), stored.identity().valueClass(),
                stored.identity().keyTypeDescriptor(),
                stored.identity().valueTypeDescriptor());
    }

    private static <K, V> EffectiveIndexSegmentConfiguration mergeSegment(
            final EffectiveIndexConfiguration<K, V> stored,
            final IndexConfiguration<K, V> request) {
        return new EffectiveIndexSegmentConfiguration(
                stored.segment().maxKeys(),
                stored.segment().chunkKeyLimit(),
                intOr(request.segment().cacheKeyLimit(),
                        stored.segment().cacheKeyLimit()),
                stored.segment().cachedSegmentLimit(),
                intOr(request.segment().deltaCacheFileLimit(),
                        stored.segment().deltaCacheFileLimit()));
    }

    private static <K, V> EffectiveIndexWritePathConfiguration mergeWritePath(
            final EffectiveIndexConfiguration<K, V> stored,
            final IndexConfiguration<K, V> request) {
        return new EffectiveIndexWritePathConfiguration(
                intOr(request.writePath().segmentWriteCacheKeyLimit(),
                        stored.writePath().segmentWriteCacheKeyLimit()),
                intOr(request.writePath()
                        .segmentWriteCacheKeyLimitDuringMaintenance(),
                        stored.writePath()
                                .segmentWriteCacheKeyLimitDuringMaintenance()),
                stored.writePath().indexBufferedWriteKeyLimit(),
                stored.writePath().segmentSplitKeyThreshold());
    }

    private static <K, V> EffectiveIndexBloomFilterConfiguration mergeBloomFilter(
            final EffectiveIndexConfiguration<K, V> stored) {
        return new EffectiveIndexBloomFilterConfiguration(
                stored.bloomFilter().hashFunctions(),
                stored.bloomFilter().indexSizeBytes(),
                stored.bloomFilter().falsePositiveProbability());
    }

    private static <K, V> EffectiveIndexMaintenanceConfiguration mergeMaintenance(
            final EffectiveIndexConfiguration<K, V> stored,
            final IndexConfiguration<K, V> request) {
        return new EffectiveIndexMaintenanceConfiguration(
                intOr(request.maintenance().segmentThreads(),
                        stored.maintenance().segmentThreads()),
                intOr(request.maintenance().indexThreads(),
                        stored.maintenance().indexThreads()),
                intOr(request.maintenance().registryLifecycleThreads(),
                        stored.maintenance().registryLifecycleThreads()),
                intOr(request.maintenance().busyBackoffMillis(),
                        stored.maintenance().busyBackoffMillis()),
                intOr(request.maintenance().busyTimeoutMillis(),
                        stored.maintenance().busyTimeoutMillis()),
                booleanOr(request.maintenance().backgroundAutoEnabled(),
                        stored.maintenance().backgroundAutoEnabled()));
    }

    private static <K, V> EffectiveIndexIoConfiguration mergeIo(
            final EffectiveIndexConfiguration<K, V> stored,
            final IndexConfiguration<K, V> request) {
        return new EffectiveIndexIoConfiguration(
                intOr(request.io().diskBufferSizeBytes(),
                        stored.io().diskBufferSizeBytes()));
    }

    private static <K, V> EffectiveIndexLoggingConfiguration mergeLogging(
            final EffectiveIndexConfiguration<K, V> stored,
            final IndexConfiguration<K, V> request) {
        return new EffectiveIndexLoggingConfiguration(
                booleanOr(request.logging().contextEnabled(),
                        stored.logging().contextEnabled()));
    }

    private static <K, V> EffectiveIndexFilterConfiguration mergeFilters(
            final EffectiveIndexConfiguration<K, V> stored,
            final ChunkFilterProviderResolver chunkFilterProviderResolver) {
        return EffectiveIndexFilterConfiguration.fromSpecs(
                stored.filters().encodingChunkFilterSpecs(),
                stored.filters().decodingChunkFilterSpecs(),
                chunkFilterProviderResolver);
    }

    private static <K, V> void validateRequiredDatatypesAndIndexName(
            final IndexConfiguration<K, V> request) {
        Vldtn.requireNonNull(request.identity().keyClass(), "keyClass");
        Vldtn.requireNonNull(request.identity().valueClass(), "valueClass");
        Vldtn.requireNotBlank(request.identity().name(), "indexName");
    }

    private static <K, V> IndexConfigurationContract defaultsFor(
            final IndexConfiguration<K, V> request) {
        final Optional<IndexConfigurationContract> defaults =
                IndexConfigurationRegistry.get(request.identity().keyClass());
        return defaults.orElseGet(() -> new IndexConfigurationContract() {
        });
    }

    private static <K, V> void validateThatFixedPropertiesAreNotOverridden(
            final EffectiveIndexConfiguration<K, V> stored,
            final IndexConfiguration<K, V> request) {
        validateClassNotChanged(request.identity().keyClass(),
                stored.identity().keyClass(), "KeyClass");
        validateClassNotChanged(request.identity().valueClass(),
                stored.identity().valueClass(), "ValueClass");
        throwIfChanged(isChanged(request.identity().keyTypeDescriptor(),
                stored.identity().keyTypeDescriptor()), "KeyTypeDescriptor",
                stored.identity().keyTypeDescriptor(),
                request.identity().keyTypeDescriptor());
        throwIfChanged(isChanged(request.identity().valueTypeDescriptor(),
                stored.identity().valueTypeDescriptor()), "ValueTypeDescriptor",
                stored.identity().valueTypeDescriptor(),
                request.identity().valueTypeDescriptor());
        throwIfChanged(isPositiveOverride(request.segment().maxKeys(),
                stored.segment().maxKeys()), "MaxNumberOfKeysInSegment",
                stored.segment().maxKeys(), request.segment().maxKeys());
        throwIfChanged(isPositiveOverride(request.segment().chunkKeyLimit(),
                stored.segment().chunkKeyLimit()),
                "MaxNumberOfKeysInSegmentChunk",
                stored.segment().chunkKeyLimit(),
                request.segment().chunkKeyLimit());
        throwIfChanged(isPositiveOverride(request.bloomFilter().indexSizeBytes(),
                stored.bloomFilter().indexSizeBytes()),
                "BloomFilterIndexSizeInBytes",
                stored.bloomFilter().indexSizeBytes(),
                request.bloomFilter().indexSizeBytes());
        throwIfChanged(isPositiveOverride(request.bloomFilter().hashFunctions(),
                stored.bloomFilter().hashFunctions()),
                "BloomFilterNumberOfHashFunctions",
                stored.bloomFilter().hashFunctions(),
                request.bloomFilter().hashFunctions());
        throwIfChanged(isChanged(request.bloomFilter()
                .falsePositiveProbability(),
                stored.bloomFilter().falsePositiveProbability()),
                "BloomFilterProbabilityOfFalsePositive",
                stored.bloomFilter().falsePositiveProbability(),
                request.bloomFilter().falsePositiveProbability());
        throwIfChanged(chunkFiltersChanged(
                request.filters().encodingChunkFilterSpecs(),
                stored.filters().encodingChunkFilterSpecs()),
                "EncodingChunkFilters",
                stored.filters().encodingChunkFilterSpecs(),
                request.filters().encodingChunkFilterSpecs());
        throwIfChanged(chunkFiltersChanged(
                request.filters().decodingChunkFilterSpecs(),
                stored.filters().decodingChunkFilterSpecs()),
                "DecodingChunkFilters",
                stored.filters().decodingChunkFilterSpecs(),
                request.filters().decodingChunkFilterSpecs());
        throwIfChanged(request.wal() != null
                && !EffectiveIndexWalConfiguration
                        .fromIndexWalConfiguration(request.wal())
                        .equals(stored.wal()),
                "IndexWalConfiguration", stored.wal(), request.wal());
    }

    private static boolean chunkFiltersChanged(
            final List<ChunkFilterSpec> requestFilters,
            final List<ChunkFilterSpec> storedFilters) {
        if (requestFilters == null || requestFilters.isEmpty()) {
            return false;
        }
        return !canonicalizeChunkFilterSpecs(requestFilters)
                .equals(canonicalizeChunkFilterSpecs(storedFilters));
    }

    private static List<ChunkFilterSpec> canonicalizeChunkFilterSpecs(
            final List<ChunkFilterSpec> specs) {
        return Vldtn.requireNonNull(specs, "specs").stream()
                .map(ChunkFilterSpecs::canonicalize).toList();
    }

    private static void validateClassNotChanged(final Class<?> requested,
            final Class<?> stored, final String propertyName) {
        if (requested == null) {
            return;
        }
        throwIfChanged(!requested.equals(stored), propertyName,
                stored.getName(), requested.getName());
    }

    private static <T> boolean isChanged(final T candidate, final T stored) {
        return candidate != null && !candidate.equals(stored);
    }

    private static boolean isPositiveOverride(final Integer candidate,
            final int stored) {
        return candidate != null && candidate.intValue() > 0
                && candidate.intValue() != stored;
    }

    private static void throwIfChanged(final boolean wasChanged,
            final String propertyName, final Object storedValue,
            final Object newValue) {
        if (wasChanged) {
            throw new IllegalArgumentException(String.format(
                    "Value of '%s' is already set to '%s' and can't be changed to '%s'",
                    propertyName, storedValue, newValue));
        }
    }

    private static int intOr(final Integer value, final Integer fallback) {
        return intOr(value, Vldtn.requireNonNull(fallback, "fallback")
                .intValue());
    }

    private static int intOr(final Integer value, final int fallback) {
        return value == null ? fallback : value.intValue();
    }

    private static boolean booleanOr(final Boolean value,
            final Boolean fallback) {
        return booleanOr(value, Vldtn.requireNonNull(fallback, "fallback")
                .booleanValue());
    }

    private static boolean booleanOr(final Boolean value,
            final boolean fallback) {
        return value == null ? fallback : value.booleanValue();
    }

    private static double doubleOr(final Double value, final Double fallback) {
        return value == null
                ? Vldtn.requireNonNull(fallback, "fallback").doubleValue()
                : value.doubleValue();
    }
}
