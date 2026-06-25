package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;

import org.junit.jupiter.api.Test;

class MemoryEstimateReportTest {

    @Test
    void linesAreDefensivelyCopiedAndImmutable() {
        final List<String> source = new ArrayList<>();
        source.add("first");
        final MemoryEstimateReport report = new MemoryEstimateReport(source,
                true, OptionalLong.of(7L));

        source.add("second");

        assertEquals(List.of("first"), report.lines());
        assertThrows(UnsupportedOperationException.class,
                () -> report.lines().add("third"));
    }

    @Test
    void textJoinsExactLines() {
        final MemoryEstimateReport report = new MemoryEstimateReport(
                List.of("alpha", "beta"), true, OptionalLong.of(9L));

        assertEquals("alpha" + System.lineSeparator() + "beta",
                report.text());
        assertEquals(report.text(), report.toString());
    }

    @Test
    void preservesCompleteIncompleteAndZeroTotals() {
        final MemoryEstimateReport complete = new MemoryEstimateReport(
                List.of("complete"), true, OptionalLong.of(12L));
        final MemoryEstimateReport incomplete = new MemoryEstimateReport(
                List.of("incomplete"), false, OptionalLong.empty());
        final MemoryEstimateReport zero = new MemoryEstimateReport(
                List.of("zero"), true, OptionalLong.of(0L));

        assertTrue(complete.isComplete());
        assertEquals(12L, complete.totalEstimatedBytes().orElseThrow());
        assertTrue(incomplete.totalEstimatedBytes().isEmpty());
        assertTrue(zero.isComplete());
        assertEquals(0L, zero.totalEstimatedBytes().orElseThrow());
    }

    @Test
    void constructorRejectsNullLines() {
        assertThrows(IllegalArgumentException.class,
                () -> new MemoryEstimateReport(null, false,
                        OptionalLong.empty()));
    }

    @Test
    void constructorRejectsNullTotalEstimatedBytes() {
        assertThrows(IllegalArgumentException.class,
                () -> new MemoryEstimateReport(List.of("line"), false,
                        null));
    }
}
