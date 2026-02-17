package org.hestiastore.management.api;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.lang.reflect.RecordComponent;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

class ManagementApiContractCompatibilityTest {

    @Test
    void actionRequestRecordComponentsRemainStable() {
        assertRecordComponents(ActionRequest.class, List.of("requestId"),
                List.of(String.class));
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
    void metricsResponseRecordComponentsRemainStable() {
        assertRecordComponents(MetricsResponse.class,
                List.of("indexName", "state", "getOperationCount",
                        "putOperationCount", "deleteOperationCount",
                        "capturedAt"),
                List.of(String.class, String.class, long.class, long.class,
                        long.class, Instant.class));
    }

    @Test
    void nodeStateResponseRecordComponentsRemainStable() {
        assertRecordComponents(NodeStateResponse.class,
                List.of("indexName", "state", "ready", "capturedAt"),
                List.of(String.class, String.class, boolean.class,
                        Instant.class));
    }

    @Test
    void configPatchRequestRecordComponentsRemainStable() {
        assertRecordComponents(ConfigPatchRequest.class,
                List.of("values", "dryRun"),
                List.of(java.util.Map.class, boolean.class));
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
