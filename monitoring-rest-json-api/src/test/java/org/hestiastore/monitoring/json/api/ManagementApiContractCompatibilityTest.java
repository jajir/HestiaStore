package org.hestiastore.monitoring.json.api;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.lang.reflect.RecordComponent;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

class ManagementApiContractCompatibilityTest {

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
    void indexReportRecordComponentsRemainStable() {
        assertRecordComponents(IndexReportResponse.class,
                List.of("indexName", "state", "ready", "getOperationCount",
                        "putOperationCount", "deleteOperationCount",
                        "registryCacheHitCount", "registryCacheMissCount",
                        "registryCacheLoadCount",
                        "registryCacheEvictionCount", "registryCacheSize",
                        "registryCacheLimit",
                        "segmentCacheKeyLimitPerSegment",
                        "maxNumberOfKeysInSegmentWriteCache",
                        "maxNumberOfKeysInSegmentWriteCacheDuringMaintenance",
                        "segmentCount", "segmentReadyCount",
                        "segmentMaintenanceCount", "segmentErrorCount",
                        "segmentClosedCount", "segmentBusyCount",
                        "totalSegmentKeys", "totalSegmentCacheKeys",
                        "totalWriteCacheKeys", "totalDeltaCacheFiles",
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
                        "bloomFilterFalsePositiveCount"),
                List.of(String.class, String.class, boolean.class, long.class,
                        long.class, long.class, long.class, long.class,
                        long.class, long.class, int.class, int.class,
                        int.class, int.class, int.class, int.class, int.class,
                        int.class, int.class, int.class, int.class, long.class,
                        long.class, long.class, long.class, long.class,
                        long.class, long.class, int.class, int.class,
                        int.class, int.class, int.class, long.class,
                        long.class, long.class, long.class, long.class,
                        long.class, int.class, int.class, double.class,
                        long.class, long.class, long.class, long.class));
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
}
