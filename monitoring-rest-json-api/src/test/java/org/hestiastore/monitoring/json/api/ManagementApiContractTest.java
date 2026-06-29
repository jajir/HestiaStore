package org.hestiastore.monitoring.json.api;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.RecordComponent;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

class ManagementApiContractTest {

    @Test
    void actionRequestRecordComponentsRemainStable() {
        assertRecordComponents(ActionRequest.class,
                List.of("requestId", "indexName"),
                List.of(String.class, String.class));
    }

    @Test
    void actionResponseRecordComponentsRemainStable() {
        assertRecordComponents(ActionResponse.class,
                List.of("requestId", "action", "status", "message",
                        "capturedAt"),
                List.of(String.class, ActionType.class, ActionStatus.class,
                        String.class, Instant.class));
    }

    @Test
    void jvmMetricsRecordComponentsRemainStable() {
        assertRecordComponents(JvmMetricsResponse.class,
                List.of("heapUsedBytes", "heapCommittedBytes",
                        "heapMaxBytes", "nonHeapUsedBytes", "gcCount",
                        "gcTimeMillis"),
                List.of(long.class, long.class, long.class, long.class,
                        long.class, long.class));
    }

    @Test
    void indexReportGroupedAccessorsRemainStable() throws Exception {
        assertFalse(IndexReportResponse.class.isRecord());
        final Constructor<IndexReportResponse> constructor =
                IndexReportResponse.class.getConstructor(String.class,
                        String.class, boolean.class,
                        OperationReportResponse.class,
                        RegistryCacheReportResponse.class,
                        ChunkStoreCacheReportResponse.class,
                        SegmentReportResponse.class,
                        WritePathReportResponse.class,
                        MaintenanceReportResponse.class,
                        SplitReportResponse.class,
                        LatencyReportResponse.class,
                        BloomFilterReportResponse.class,
                        WalReportResponse.class);
        assertNotNull(constructor);
        assertAccessor(IndexReportResponse.class, "indexName", String.class);
        assertAccessor(IndexReportResponse.class, "state", String.class);
        assertAccessor(IndexReportResponse.class, "ready", boolean.class);
        assertAccessor(IndexReportResponse.class, "operations",
                OperationReportResponse.class);
        assertAccessor(IndexReportResponse.class, "registryCache",
                RegistryCacheReportResponse.class);
        assertAccessor(IndexReportResponse.class, "chunkStoreCache",
                ChunkStoreCacheReportResponse.class);
        assertAccessor(IndexReportResponse.class, "segments",
                SegmentReportResponse.class);
        assertAccessor(IndexReportResponse.class, "writePath",
                WritePathReportResponse.class);
        assertAccessor(IndexReportResponse.class, "maintenance",
                MaintenanceReportResponse.class);
        assertAccessor(IndexReportResponse.class, "split",
                SplitReportResponse.class);
        assertAccessor(IndexReportResponse.class, "latency",
                LatencyReportResponse.class);
        assertAccessor(IndexReportResponse.class, "bloomFilter",
                BloomFilterReportResponse.class);
        assertAccessor(IndexReportResponse.class, "wal", WalReportResponse.class);
    }

    @Test
    void metricsRecordComponentsRemainStable() {
        assertRecordComponents(MetricsResponse.class,
                List.of("indexName", "state", "readOperationCount",
                        "putOperationCount", "deleteOperationCount",
                        "registryCacheHitCount", "registryCacheMissCount",
                        "registryCacheLoadCount",
                        "registryCacheEvictionCount", "registryCacheSize",
                        "registryCacheLimit",
                        "segmentCacheKeyLimitPerSegment",
                        "segmentWriteCacheKeyLimit",
                        "segmentWriteCacheKeyLimitDuringMaintenance",
                        "indexBufferedWriteKeyLimit",
                        "segmentCount", "segmentReadyCount",
                        "segmentMaintenanceCount", "segmentErrorCount",
                        "segmentClosedCount", "unloadedMappedSegmentCount",
                        "totalSegmentKeys", "totalSegmentCacheKeys",
                        "totalBufferedWriteKeys", "totalDeltaCacheFiles",
                        "compactRequestCount", "flushRequestCount",
                        "splitScheduleCount", "splitInFlightCount",
                        "maintenanceQueueSize", "maintenanceQueueCapacity",
                        "splitQueueSize", "splitQueueCapacity",
                        "readLatencyP50Micros", "readLatencyP95Micros",
                        "readLatencyP99Micros", "writeLatencyP50Micros",
                        "writeLatencyP95Micros", "writeLatencyP99Micros",
                        "bloomFilterHashFunctions",
                        "bloomFilterIndexSizeInBytes",
                        "bloomFilterProbabilityOfFalsePositive",
                        "bloomFilterRequestCount",
                        "bloomFilterRefusedCount",
                        "bloomFilterPositiveCount",
                        "bloomFilterFalsePositiveCount",
                        "jvmHeapUsedBytes", "jvmHeapCommittedBytes",
                        "jvmNonHeapUsedBytes", "jvmGcCount",
                        "jvmGcTimeMillis", "capturedAt"),
                List.of(String.class, String.class, long.class, long.class,
                        long.class, long.class, long.class, long.class,
                        long.class, int.class, int.class, int.class,
                        int.class, int.class, int.class, int.class, int.class,
                        int.class, int.class, int.class, int.class,
                        long.class, long.class, long.class, long.class,
                        long.class, long.class, long.class, int.class,
                        int.class, int.class, int.class, int.class, long.class,
                        long.class, long.class, long.class, long.class,
                        long.class, int.class, int.class, double.class,
                        long.class, long.class, long.class, long.class,
                        long.class, long.class, long.class, long.class,
                        long.class, Instant.class));
    }

    @Test
    void segmentRuntimeRecordComponentsRemainStable() {
        assertRecordComponents(SegmentRuntimeReportResponse.class,
                List.of("segmentId", "state", "numberOfKeysInDeltaCache",
                        "numberOfKeysInSegment", "numberOfKeysInScarceIndex",
                        "numberOfKeysInSegmentCache",
                        "numberOfKeysInWriteCache", "numberOfDeltaCacheFiles",
                        "compactRequestCount", "flushRequestCount",
                        "bloomFilterRequestCount", "bloomFilterRefusedCount",
                        "bloomFilterPositiveCount",
                        "bloomFilterFalsePositiveCount"),
                List.of(String.class, String.class, long.class, long.class,
                        long.class, long.class, int.class, int.class,
                        long.class, long.class, long.class, long.class,
                        long.class, long.class));
    }

    @Test
    void nodeReportRecordComponentsRemainStable() {
        assertRecordComponents(NodeReportResponse.class,
                List.of("jvm", "indexes", "capturedAt"),
                List.of(JvmMetricsResponse.class, List.class, Instant.class));
    }

    @Test
    void configPatchRequestRecordComponentsRemainStable() {
        assertRecordComponents(ConfigPatchRequest.class,
                List.of("values", "dryRun"),
                List.of(java.util.Map.class, boolean.class));
    }

    @Test
    void configViewResponseBeanAccessorsRemainStable() throws Exception {
        assertFalse(ConfigViewResponse.class.isRecord());
        assertNotNull(ConfigViewResponse.class.getConstructor());
        assertNotNull(ConfigViewResponse.class.getMethod("getIndexName"));
        assertNotNull(ConfigViewResponse.class.getMethod("getOriginal"));
        assertNotNull(ConfigViewResponse.class.getMethod("getCurrent"));
        assertNotNull(ConfigViewResponse.class.getMethod("getSupportedKeys"));
        assertNotNull(ConfigViewResponse.class.getMethod("getRevision"));
        assertNotNull(ConfigViewResponse.class.getMethod("getCapturedAt"));
    }

    @Test
    void errorResponseRecordComponentsRemainStable() {
        assertRecordComponents(ErrorResponse.class,
                List.of("code", "message", "requestId", "capturedAt"),
                List.of(String.class, String.class, String.class,
                        Instant.class));
    }

    @Test
    void actionTypeValuesRemainStable() {
        assertArrayEquals(new ActionType[] { ActionType.FLUSH, ActionType.COMPACT },
                ActionType.values());
    }

    @Test
    void actionStatusValuesRemainStable() {
        assertArrayEquals(
                new ActionStatus[] { ActionStatus.ACCEPTED, ActionStatus.COMPLETED,
                        ActionStatus.REJECTED, ActionStatus.FAILED },
                ActionStatus.values());
    }

    private void assertRecordComponents(final Class<?> recordType,
            final List<String> expectedNames, final List<Class<?>> expectedTypes) {
        final RecordComponent[] components = recordType.getRecordComponents();
        assertEquals(expectedNames.size(), components.length,
                "Unexpected component count for " + recordType.getSimpleName());
        for (int i = 0; i < components.length; i++) {
            final RecordComponent component = components[i];
            assertEquals(expectedNames.get(i), component.getName(),
                    "Unexpected component name at index " + i + " for "
                            + recordType.getSimpleName() + ". Current order: "
                            + Arrays.toString(
                                    Arrays.stream(components)
                                            .map(RecordComponent::getName)
                                            .toArray()));
            assertEquals(expectedTypes.get(i), component.getType(),
                    "Unexpected component type at index " + i + " for "
                            + recordType.getSimpleName());
        }
    }

    private void assertAccessor(final Class<?> type, final String accessorName,
            final Class<?> expectedReturnType) throws Exception {
        final Method method = type.getMethod(accessorName);
        assertEquals(expectedReturnType, method.getReturnType(),
                "Unexpected return type for " + type.getSimpleName() + "."
                        + accessorName + "()");
    }
}
