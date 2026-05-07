package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.hestiastore.index.chunkstore.ChunkFilter;
import org.hestiastore.index.chunkstore.ChunkFilterCrc32Validation;
import org.hestiastore.index.chunkstore.ChunkFilterCrc32Writing;
import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.chunkstore.ChunkFilterRegistration;
import org.hestiastore.index.chunkstore.ChunkFilterSpec;
import org.hestiastore.index.chunkstore.ChunkFilterSpecs;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.junit.jupiter.api.Test;

class IndexConfigurationBuilderValidationTest {

    @Test
    void test_identity_nullCustomizerRejected() {
        final IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> newBuilder().identity(null));

        assertEquals("Property 'customizer' must not be null.",
                exception.getMessage());
    }

    @Test
    void test_segment_nullCustomizerRejected() {
        final IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> newBuilder().segment(null));

        assertEquals("Property 'customizer' must not be null.",
                exception.getMessage());
    }

    @Test
    void test_writePath_nullCustomizerRejected() {
        final IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> newBuilder().writePath(null));

        assertEquals("Property 'customizer' must not be null.",
                exception.getMessage());
    }

    @Test
    void test_bloomFilter_nullCustomizerRejected() {
        final IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> newBuilder().bloomFilter(null));

        assertEquals("Property 'customizer' must not be null.",
                exception.getMessage());
    }

    @Test
    void test_wal_nullCustomizerRejected() {
        final IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> newBuilder().wal(null));

        assertEquals("Property 'customizer' must not be null.",
                exception.getMessage());
    }

    @Test
    void test_maintenance_nullCustomizerRejected() {
        final IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> newBuilder().maintenance(null));

        assertEquals("Property 'customizer' must not be null.",
                exception.getMessage());
    }

    @Test
    void test_io_nullCustomizerRejected() {
        final IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> newBuilder().io(null));

        assertEquals("Property 'customizer' must not be null.",
                exception.getMessage());
    }

    @Test
    void test_logging_nullCustomizerRejected() {
        final IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> newBuilder().logging(null));

        assertEquals("Property 'customizer' must not be null.",
                exception.getMessage());
    }

    @Test
    void test_filters_nullCustomizerRejected() {
        final IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> newBuilder().filters(null));

        assertEquals("Property 'customizer' must not be null.",
                exception.getMessage());
    }

    @Test
    void test_name_setsValue() {
        final IndexConfiguration<Integer, String> config = newBuilder()
                .identity(identity -> identity.name("orders")).build();

        assertEquals("orders", config.identity().name());
    }

    @Test
    void test_keyClass_setsValue() {
        final IndexConfiguration<Integer, String> config = newBuilder()
                .identity(identity -> identity.keyClass(Integer.class)).build();

        assertEquals(Integer.class, config.identity().keyClass());
    }

    @Test
    void test_valueClass_setsValue() {
        final IndexConfiguration<Integer, String> config = newBuilder()
                .identity(identity -> identity.valueClass(String.class))
                .build();

        assertEquals(String.class, config.identity().valueClass());
    }

    @Test
    void test_keyTypeDescriptor_descriptorSetsClassName() {
        final TypeDescriptorInteger descriptor = new TypeDescriptorInteger();
        final IndexConfiguration<Integer, String> config = newBuilder()
                .identity(identity -> identity.keyTypeDescriptor(descriptor))
                .build();

        assertEquals(TypeDescriptorInteger.class.getName(),
                config.identity().keyTypeDescriptor());
    }

    @Test
    void test_keyTypeDescriptor_nullDescriptorRejected() {
        final IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> newBuilder().identity(identity -> identity
                        .keyTypeDescriptor((TypeDescriptorInteger) null)));

        assertEquals("Property 'keyTypeDescriptor' must not be null.",
                exception.getMessage());
    }

    @Test
    void test_keyTypeDescriptor_stringSetsValue() {
        final IndexConfiguration<Integer, String> config = newBuilder()
                .identity(identity -> identity
                        .keyTypeDescriptor("key-descriptor"))
                .build();

        assertEquals("key-descriptor",
                config.identity().keyTypeDescriptor());
    }

    @Test
    void test_valueTypeDescriptor_descriptorSetsClassName() {
        final TypeDescriptorShortString descriptor =
                new TypeDescriptorShortString();
        final IndexConfiguration<Integer, String> config = newBuilder()
                .identity(identity -> identity.valueTypeDescriptor(descriptor))
                .build();

        assertEquals(TypeDescriptorShortString.class.getName(),
                config.identity().valueTypeDescriptor());
    }

    @Test
    void test_valueTypeDescriptor_nullDescriptorRejected() {
        final IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> newBuilder().identity(identity -> identity
                        .valueTypeDescriptor(
                                (TypeDescriptorShortString) null)));

        assertEquals("Property 'valueTypeDescriptor' must not be null.",
                exception.getMessage());
    }

    @Test
    void test_valueTypeDescriptor_stringSetsValue() {
        final IndexConfiguration<Integer, String> config = newBuilder()
                .identity(identity -> identity
                        .valueTypeDescriptor("value-descriptor"))
                .build();

        assertEquals("value-descriptor",
                config.identity().valueTypeDescriptor());
    }

    @Test
    void test_maxKeys_setsValue() {
        final IndexConfiguration<Integer, String> config = newBuilder()
                .segment(segment -> segment.maxKeys(42)).build();

        assertEquals(42, config.segment().maxKeys());
    }

    @Test
    void test_chunkKeyLimit_setsValue() {
        final IndexConfiguration<Integer, String> config = newBuilder()
                .segment(segment -> segment.chunkKeyLimit(7)).build();

        assertEquals(7, config.segment().chunkKeyLimit());
    }

    @Test
    void test_cacheKeyLimit_setsValue() {
        final IndexConfiguration<Integer, String> config = newBuilder()
                .segment(segment -> segment.cacheKeyLimit(11)).build();

        assertEquals(11, config.segment().cacheKeyLimit());
    }

    @Test
    void test_cachedSegmentLimit_setsValue() {
        final IndexConfiguration<Integer, String> config = newBuilder()
                .segment(segment -> segment.cachedSegmentLimit(3)).build();

        assertEquals(3, config.segment().cachedSegmentLimit());
        assertEquals(3, config.runtimeTuning().maxSegmentsInCache());
    }

    @Test
    void test_deltaCacheFileLimit_setsValue() {
        final IndexConfiguration<Integer, String> config = newBuilder()
                .segment(segment -> segment.deltaCacheFileLimit(5)).build();

        assertEquals(5, config.segment().deltaCacheFileLimit());
    }

    @Test
    void test_segmentWriteCacheKeyLimit_setsValueAndDerivedLimits() {
        final IndexConfiguration<Integer, String> config = newBuilder()
                .writePath(writePath -> writePath
                        .segmentWriteCacheKeyLimit(10))
                .build();

        assertEquals(10, config.writePath().segmentWriteCacheKeyLimit());
        assertEquals(14, config.writePath()
                .segmentWriteCacheKeyLimitDuringMaintenance());
        assertEquals(140, config.writePath().indexBufferedWriteKeyLimit());
    }

    @Test
    void test_segmentWriteCacheKeyLimit_rejectsZero() {
        final IllegalArgumentException exception = assertBuildThrows(
                newBuilder().writePath(writePath -> writePath
                        .segmentWriteCacheKeyLimit(0)));

        assertEquals("segmentWriteCacheKeyLimit must be >= 1",
                exception.getMessage());
    }

    @Test
    void test_segmentWriteCacheKeyLimit_rejectsNegative() {
        final IllegalArgumentException exception = assertBuildThrows(
                newBuilder().writePath(writePath -> writePath
                        .segmentWriteCacheKeyLimit(-1)));

        assertEquals("segmentWriteCacheKeyLimit must be >= 1",
                exception.getMessage());
    }

    @Test
    void test_maintenanceWriteCacheKeyLimit_setsValue() {
        final IndexConfiguration<Integer, String> config = newBuilder()
                .writePath(writePath -> writePath
                        .segmentWriteCacheKeyLimit(10)
                        .maintenanceWriteCacheKeyLimit(15))
                .build();

        assertEquals(15, config.writePath()
                .segmentWriteCacheKeyLimitDuringMaintenance());
    }

    @Test
    void test_maintenanceWriteCacheKeyLimit_rejectsZero() {
        final IllegalArgumentException exception = assertBuildThrows(
                newBuilder().writePath(writePath -> writePath
                        .maintenanceWriteCacheKeyLimit(0)));

        assertEquals(
                "Property 'segmentWriteCacheKeyLimitDuringMaintenance' must be greater than 0",
                exception.getMessage());
    }

    @Test
    void test_maintenanceWriteCacheKeyLimit_rejectsActiveLimit() {
        final IllegalArgumentException exception = assertBuildThrows(
                newBuilder().writePath(writePath -> writePath
                        .segmentWriteCacheKeyLimit(10)
                        .maintenanceWriteCacheKeyLimit(10)));

        assertEquals(
                "Property 'segmentWriteCacheKeyLimitDuringMaintenance' must be greater "
                        + "than 'segmentWriteCacheKeyLimit'",
                exception.getMessage());
    }

    @Test
    void test_indexBufferedWriteKeyLimit_setsValue() {
        final IndexConfiguration<Integer, String> config = newBuilder()
                .writePath(writePath -> writePath
                        .indexBufferedWriteKeyLimit(42))
                .build();

        assertEquals(42, config.writePath().indexBufferedWriteKeyLimit());
    }

    @Test
    void test_indexBufferedWriteKeyLimit_rejectsZero() {
        final IllegalArgumentException exception = assertBuildThrows(
                newBuilder().writePath(writePath -> writePath
                        .indexBufferedWriteKeyLimit(0)));

        assertEquals("indexBufferedWriteKeyLimit must be >= 1",
                exception.getMessage());
    }

    @Test
    void test_indexBufferedWriteKeyLimit_rejectsBelowMaintenanceLimit() {
        final IllegalArgumentException exception = assertBuildThrows(
                newBuilder().writePath(writePath -> writePath
                        .segmentWriteCacheKeyLimit(10)
                        .maintenanceWriteCacheKeyLimit(15)
                        .indexBufferedWriteKeyLimit(14)));

        assertEquals(
                "Property 'indexBufferedWriteKeyLimit' must be greater than "
                        + "or equal to 'segmentWriteCacheKeyLimitDuringMaintenance'",
                exception.getMessage());
    }

    @Test
    void test_segmentSplitKeyThreshold_setsValue() {
        final IndexConfiguration<Integer, String> config = newBuilder()
                .writePath(writePath -> writePath
                        .segmentSplitKeyThreshold(100))
                .build();

        assertEquals(100, config.writePath().segmentSplitKeyThreshold());
    }

    @Test
    void test_segmentSplitKeyThreshold_rejectsZero() {
        final IllegalArgumentException exception = assertBuildThrows(
                newBuilder().writePath(writePath -> writePath
                        .segmentSplitKeyThreshold(0)));

        assertEquals("segmentSplitKeyThreshold must be >= 1",
                exception.getMessage());
    }

    @Test
    void test_hashFunctions_setsValue() {
        final IndexConfiguration<Integer, String> config = newBuilder()
                .bloomFilter(bloomFilter -> bloomFilter.hashFunctions(2))
                .build();

        assertEquals(2, config.bloomFilter().hashFunctions());
    }

    @Test
    void test_indexSizeBytes_setsValue() {
        final IndexConfiguration<Integer, String> config = newBuilder()
                .bloomFilter(bloomFilter -> bloomFilter.indexSizeBytes(1024))
                .build();

        assertEquals(1024, config.bloomFilter().indexSizeBytes());
    }

    @Test
    void test_falsePositiveProbability_setsValue() {
        final IndexConfiguration<Integer, String> config = newBuilder()
                .bloomFilter(bloomFilter -> bloomFilter
                        .falsePositiveProbability(0.01D))
                .build();

        assertEquals(0.01D,
                config.bloomFilter().falsePositiveProbability());
    }

    @Test
    void test_disabled_setsBloomFilterIndexSizeToZero() {
        final IndexConfiguration<Integer, String> config = newBuilder()
                .bloomFilter(IndexBloomFilterConfigurationBuilder::disabled)
                .build();

        assertEquals(0, config.bloomFilter().indexSizeBytes());
    }

    @Test
    void test_segmentThreads_setsValue() {
        final IndexConfiguration<Integer, String> config = newBuilder()
                .maintenance(maintenance -> maintenance.segmentThreads(2))
                .build();

        assertEquals(2, config.maintenance().segmentThreads());
    }

    @Test
    void test_indexThreads_setsValue() {
        final IndexConfiguration<Integer, String> config = newBuilder()
                .maintenance(maintenance -> maintenance.indexThreads(3))
                .build();

        assertEquals(3, config.maintenance().indexThreads());
    }

    @Test
    void test_registryLifecycleThreads_setsValue() {
        final IndexConfiguration<Integer, String> config = newBuilder()
                .maintenance(maintenance -> maintenance
                        .registryLifecycleThreads(4))
                .build();

        assertEquals(4, config.maintenance().registryLifecycleThreads());
    }

    @Test
    void test_busyBackoffMillis_setsValue() {
        final IndexConfiguration<Integer, String> config = newBuilder()
                .maintenance(maintenance -> maintenance.busyBackoffMillis(5))
                .build();

        assertEquals(5, config.maintenance().busyBackoffMillis());
    }

    @Test
    void test_busyTimeoutMillis_setsValue() {
        final IndexConfiguration<Integer, String> config = newBuilder()
                .maintenance(maintenance -> maintenance.busyTimeoutMillis(6))
                .build();

        assertEquals(6, config.maintenance().busyTimeoutMillis());
    }

    @Test
    void test_backgroundAutoEnabled_setsValue() {
        final IndexConfiguration<Integer, String> config = newBuilder()
                .maintenance(maintenance -> maintenance
                        .backgroundAutoEnabled(false))
                .build();

        assertFalse(config.maintenance().backgroundAutoEnabled());
    }

    @Test
    void test_diskBufferSizeBytes_setsValue() {
        final IndexConfiguration<Integer, String> config = newBuilder()
                .io(io -> io.diskBufferSizeBytes(2048)).build();

        assertEquals(2048, config.io().diskBufferSizeBytes());
    }

    @Test
    void test_logging_setValue_true() {
        final IndexConfiguration<Integer, String> config = newBuilder()
                .logging(logging -> logging.contextEnabled(true)).build();

        assertTrue(config.logging().contextEnabled());
    }

    @Test
    void test_logging_setValue_false() {
        final IndexConfiguration<Integer, String> config = newBuilder()
                .logging(logging -> logging.contextEnabled(false)).build();

        assertFalse(config.logging().contextEnabled());
    }

    @Test
    void test_logging_unset_keepsNull() {
        final IndexConfiguration<Integer, String> config = newBuilder()
                .build();

        assertNull(config.logging().contextEnabled());
    }

    @Test
    void test_logging_setValue_null() {
        final IndexConfiguration<Integer, String> config = newBuilder()
                .logging(logging -> logging.contextEnabled(null)).build();

        assertNull(config.logging().contextEnabled());
    }

    @Test
    void test_addEncodingFilter_filterRejectsNull() {
        final IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> newBuilder().filters(filters -> filters
                        .addEncodingFilter((ChunkFilter) null)));

        assertEquals("Property 'filter' must not be null.",
                exception.getMessage());
    }

    @Test
    void test_addEncodingFilter_classRejectsNull() {
        final IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> newBuilder().filters(filters -> filters
                        .addEncodingFilter((Class<? extends ChunkFilter>) null)));

        assertEquals("Property 'filterClass' must not be null.",
                exception.getMessage());
    }

    @Test
    void test_addEncodingFilter_specRejectsNull() {
        final IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> newBuilder().filters(filters -> filters
                        .addEncodingFilter((ChunkFilterSpec) null)));

        assertEquals("Property 'spec' must not be null.",
                exception.getMessage());
    }

    @Test
    void test_encodingFilterRegistrations_replacesValues() {
        final ChunkFilterRegistration registration = ChunkFilterRegistration.of(
                ChunkFilterSpecs.crc32(), ChunkFilterCrc32Writing::new);
        final IndexConfiguration<Integer, String> config = newBuilder()
                .filters(filters -> filters
                        .addEncodingFilter(ChunkFilterSpecs.doNothing())
                        .encodingFilterRegistrations(List.of(registration)))
                .build();

        assertEquals(List.of(ChunkFilterSpecs.crc32()),
                config.filters().encodingChunkFilterSpecs());
    }

    @Test
    void test_encodingFilterRegistrations_rejectsNullCollection() {
        final IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> newBuilder().filters(filters -> filters
                        .encodingFilterRegistrations(null)));

        assertEquals("Property 'registrations' must not be null.",
                exception.getMessage());
    }

    @Test
    void test_encodingFilterRegistrations_rejectsNullElement() {
        final Collection<ChunkFilterRegistration> registrations = Arrays
                .asList((ChunkFilterRegistration) null);

        final IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> newBuilder().filters(filters -> filters
                        .encodingFilterRegistrations(registrations)));

        assertEquals("Property 'registration' must not be null.",
                exception.getMessage());
    }

    @Test
    void test_encodingFilterSpecs_replacesValues() {
        final IndexConfiguration<Integer, String> config = newBuilder()
                .filters(filters -> filters
                        .addEncodingFilter(ChunkFilterSpecs.doNothing())
                        .encodingFilterSpecs(List.of(ChunkFilterSpecs.crc32())))
                .build();

        assertEquals(List.of(ChunkFilterSpecs.crc32()),
                config.filters().encodingChunkFilterSpecs());
    }

    @Test
    void test_encodingFilterSpecs_rejectsNullCollection() {
        final IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> newBuilder().filters(filters -> filters
                        .encodingFilterSpecs(null)));

        assertEquals("Property 'specs' must not be null.",
                exception.getMessage());
    }

    @Test
    void test_encodingFilterSpecs_rejectsNullElement() {
        final Collection<ChunkFilterSpec> specs = Arrays
                .asList((ChunkFilterSpec) null);

        final IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> newBuilder().filters(filters -> filters
                        .encodingFilterSpecs(specs)));

        assertEquals("Property 'spec' must not be null.",
                exception.getMessage());
    }

    @Test
    void test_encodingFilterClasses_replacesValues() {
        final IndexConfiguration<Integer, String> config = newBuilder()
                .filters(filters -> filters
                        .addEncodingFilter(ChunkFilterSpecs.doNothing())
                        .encodingFilterClasses(
                                List.of(ChunkFilterCrc32Writing.class)))
                .build();

        assertEquals(List.of(ChunkFilterSpecs.crc32()),
                config.filters().encodingChunkFilterSpecs());
    }

    @Test
    void test_encodingFilterClasses_rejectsNullCollection() {
        final IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> newBuilder().filters(filters -> filters
                        .encodingFilterClasses(null)));

        assertEquals("Property 'filterClasses' must not be null.",
                exception.getMessage());
    }

    @Test
    void test_encodingFilterClasses_rejectsNullElement() {
        final Collection<Class<? extends ChunkFilter>> classes = Arrays
                .asList((Class<? extends ChunkFilter>) null);

        final IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> newBuilder().filters(filters -> filters
                        .encodingFilterClasses(classes)));

        assertEquals("Property 'filterClass' must not be null.",
                exception.getMessage());
    }

    @Test
    void test_encodingFilters_replacesValues() {
        final IndexConfiguration<Integer, String> config = newBuilder()
                .filters(filters -> filters
                        .addEncodingFilter(ChunkFilterSpecs.crc32())
                        .encodingFilters(List.of(new ChunkFilterDoNothing())))
                .build();

        assertEquals(List.of(ChunkFilterSpecs.doNothing()),
                config.filters().encodingChunkFilterSpecs());
    }

    @Test
    void test_encodingFilters_rejectsNullCollection() {
        final IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> newBuilder().filters(filters -> filters
                        .encodingFilters(null)));

        assertEquals("Property 'filters' must not be null.",
                exception.getMessage());
    }

    @Test
    void test_encodingFilters_rejectsNullElement() {
        final Collection<ChunkFilter> filters = Arrays
                .asList((ChunkFilter) null);

        final IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> newBuilder().filters(section -> section
                        .encodingFilters(filters)));

        assertEquals("Property 'filter' must not be null.",
                exception.getMessage());
    }

    @Test
    void test_addDecodingFilter_filterRejectsNull() {
        final IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> newBuilder().filters(filters -> filters
                        .addDecodingFilter((ChunkFilter) null)));

        assertEquals("Property 'filter' must not be null.",
                exception.getMessage());
    }

    @Test
    void test_addDecodingFilter_classRejectsNull() {
        final IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> newBuilder().filters(filters -> filters
                        .addDecodingFilter((Class<? extends ChunkFilter>) null)));

        assertEquals("Property 'filterClass' must not be null.",
                exception.getMessage());
    }

    @Test
    void test_addDecodingFilter_specRejectsNull() {
        final IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> newBuilder().filters(filters -> filters
                        .addDecodingFilter((ChunkFilterSpec) null)));

        assertEquals("Property 'spec' must not be null.",
                exception.getMessage());
    }

    @Test
    void test_decodingFilterRegistrations_replacesValues() {
        final ChunkFilterRegistration registration = ChunkFilterRegistration.of(
                ChunkFilterSpecs.crc32(), ChunkFilterCrc32Writing::new);
        final IndexConfiguration<Integer, String> config = newBuilder()
                .filters(filters -> filters
                        .addDecodingFilter(ChunkFilterSpecs.doNothing())
                        .decodingFilterRegistrations(List.of(registration)))
                .build();

        assertEquals(List.of(ChunkFilterSpecs.crc32()),
                config.filters().decodingChunkFilterSpecs());
    }

    @Test
    void test_decodingFilterRegistrations_rejectsNullCollection() {
        final IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> newBuilder().filters(filters -> filters
                        .decodingFilterRegistrations(null)));

        assertEquals("Property 'registrations' must not be null.",
                exception.getMessage());
    }

    @Test
    void test_decodingFilterRegistrations_rejectsNullElement() {
        final Collection<ChunkFilterRegistration> registrations = Arrays
                .asList((ChunkFilterRegistration) null);

        final IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> newBuilder().filters(filters -> filters
                        .decodingFilterRegistrations(registrations)));

        assertEquals("Property 'registration' must not be null.",
                exception.getMessage());
    }

    @Test
    void test_decodingFilterSpecs_replacesValues() {
        final IndexConfiguration<Integer, String> config = newBuilder()
                .filters(filters -> filters
                        .addDecodingFilter(ChunkFilterSpecs.doNothing())
                        .decodingFilterSpecs(List.of(ChunkFilterSpecs.crc32())))
                .build();

        assertEquals(List.of(ChunkFilterSpecs.crc32()),
                config.filters().decodingChunkFilterSpecs());
    }

    @Test
    void test_decodingFilterSpecs_rejectsNullCollection() {
        final IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> newBuilder().filters(filters -> filters
                        .decodingFilterSpecs(null)));

        assertEquals("Property 'specs' must not be null.",
                exception.getMessage());
    }

    @Test
    void test_decodingFilterSpecs_rejectsNullElement() {
        final Collection<ChunkFilterSpec> specs = Arrays
                .asList((ChunkFilterSpec) null);

        final IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> newBuilder().filters(filters -> filters
                        .decodingFilterSpecs(specs)));

        assertEquals("Property 'spec' must not be null.",
                exception.getMessage());
    }

    @Test
    void test_decodingFilterClasses_replacesValues() {
        final IndexConfiguration<Integer, String> config = newBuilder()
                .filters(filters -> filters
                        .addDecodingFilter(ChunkFilterSpecs.doNothing())
                        .decodingFilterClasses(
                                List.of(ChunkFilterCrc32Validation.class)))
                .build();

        assertEquals(List.of(ChunkFilterSpecs.crc32()),
                config.filters().decodingChunkFilterSpecs());
    }

    @Test
    void test_decodingFilterClasses_rejectsNullCollection() {
        final IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> newBuilder().filters(filters -> filters
                        .decodingFilterClasses(null)));

        assertEquals("Property 'filterClasses' must not be null.",
                exception.getMessage());
    }

    @Test
    void test_decodingFilterClasses_rejectsNullElement() {
        final Collection<Class<? extends ChunkFilter>> classes = Arrays
                .asList((Class<? extends ChunkFilter>) null);

        final IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> newBuilder().filters(filters -> filters
                        .decodingFilterClasses(classes)));

        assertEquals("Property 'filterClass' must not be null.",
                exception.getMessage());
    }

    @Test
    void test_decodingFilters_replacesValues() {
        final IndexConfiguration<Integer, String> config = newBuilder()
                .filters(filters -> filters
                        .addDecodingFilter(ChunkFilterSpecs.crc32())
                        .decodingFilters(List.of(new ChunkFilterDoNothing())))
                .build();

        assertEquals(List.of(ChunkFilterSpecs.doNothing()),
                config.filters().decodingChunkFilterSpecs());
    }

    @Test
    void test_decodingFilters_rejectsNullCollection() {
        final IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> newBuilder().filters(filters -> filters
                        .decodingFilters(null)));

        assertEquals("Property 'filters' must not be null.",
                exception.getMessage());
    }

    @Test
    void test_decodingFilters_rejectsNullElement() {
        final Collection<ChunkFilter> filters = Arrays
                .asList((ChunkFilter) null);

        final IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> newBuilder().filters(section -> section
                        .decodingFilters(filters)));

        assertEquals("Property 'filter' must not be null.",
                exception.getMessage());
    }

    private static IndexConfigurationBuilder<Integer, String> newBuilder() {
        return IndexConfiguration.<Integer, String>builder();
    }

    private static IllegalArgumentException assertBuildThrows(
            final IndexConfigurationBuilder<Integer, String> builder) {
        return assertThrows(IllegalArgumentException.class, builder::build);
    }

}
