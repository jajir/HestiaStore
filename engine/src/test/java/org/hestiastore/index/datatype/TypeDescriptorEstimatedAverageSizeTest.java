package org.hestiastore.index.datatype;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class TypeDescriptorEstimatedAverageSizeTest {

    @Test
    void fixedSizeDescriptorsExposeConstantEstimates() {
        assertEquals(1,
                new TypeDescriptorByte().getEstimatedAverageSizeInBytes()
                        .orElseThrow());
        assertEquals(4,
                new TypeDescriptorInteger().getEstimatedAverageSizeInBytes()
                        .orElseThrow());
        assertEquals(8,
                new TypeDescriptorLong().getEstimatedAverageSizeInBytes()
                        .orElseThrow());
        assertEquals(4,
                new TypeDescriptorFloat().getEstimatedAverageSizeInBytes()
                        .orElseThrow());
        assertEquals(8,
                new TypeDescriptorDouble().getEstimatedAverageSizeInBytes()
                        .orElseThrow());
        assertEquals(0,
                new TypeDescriptorNull().getEstimatedAverageSizeInBytes()
                        .orElseThrow());
    }

    @Test
    void boundedStringDescriptorsExposeConservativeEstimates() {
        assertEquals(128,
                new TypeDescriptorShortString()
                        .getEstimatedAverageSizeInBytes().orElseThrow());
        assertEquals(12,
                new TypeDescriptorFixedLengthString(12)
                        .getEstimatedAverageSizeInBytes().orElseThrow());
    }

    @Test
    void unboundedDescriptorsDoNotGuessAverageSize() {
        assertTrue(new TypeDescriptorString()
                .getEstimatedAverageSizeInBytes().isEmpty());
        assertTrue(new TypeDescriptorByteArray()
                .getEstimatedAverageSizeInBytes().isEmpty());
    }
}
