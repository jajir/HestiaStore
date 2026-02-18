package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class SegmentBuildResultTest {

    @Test
    void okCarriesValue() {
        final SegmentBuildResult<String> result = SegmentBuildResult
                .ok("value");

        assertEquals(SegmentBuildStatus.OK, result.getStatus());
        assertEquals("value", result.getValue());
        assertTrue(result.isOk());
    }

    @Test
    void busyGetValueThrowsWithStatusInMessage() {
        final SegmentBuildResult<String> result = SegmentBuildResult.busy();

        final IllegalStateException error = assertThrows(
                IllegalStateException.class, result::getValue);
        assertEquals(
                "Build result value is unavailable because segment build status is 'BUSY'. Check getStatus() before calling getValue().",
                error.getMessage());
    }
}
