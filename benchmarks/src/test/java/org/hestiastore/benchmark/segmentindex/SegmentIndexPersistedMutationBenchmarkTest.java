package org.hestiastore.benchmark.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;

import org.hestiastore.index.segmentindex.SegmentIndex;
import org.junit.jupiter.api.Test;

class SegmentIndexPersistedMutationBenchmarkTest {

    @Test
    void resetIterationStateRestoresSeededBaselineBetweenIterations()
            throws Exception {
        final SegmentIndexPersistedMutationBenchmark benchmark = new SegmentIndexPersistedMutationBenchmark();
        configureBenchmark(benchmark);
        final SegmentIndexPersistedMutationBenchmark.MutationDiagnostics diagnostics = new SegmentIndexPersistedMutationBenchmark.MutationDiagnostics();

        benchmark.setup();
        try {
            benchmark.resetIterationState();
            diagnostics.captureIterationStart(benchmark);
            final int seededKeyCount = intField(benchmark, "seededKeyCount");
            assertEquals(seededKeyCount, intField(benchmark, "putSequence"));
            final SegmentIndex<Integer, String> firstIndex = currentIndex(
                    benchmark);
            assertNull(firstIndex.get(Integer.valueOf(seededKeyCount)));

            benchmark.putSync(diagnostics);

            assertNotNull(firstIndex.get(Integer.valueOf(seededKeyCount)));
            final Path firstIterationDir = currentIterationDir(benchmark);
            diagnostics.captureIterationEnd(benchmark);
            benchmark.flushAfterIteration();
            assertFalse(Files.exists(firstIterationDir));

            benchmark.resetIterationState();
            diagnostics.captureIterationStart(benchmark);
            assertEquals(seededKeyCount, intField(benchmark, "putSequence"));
            final SegmentIndex<Integer, String> secondIndex = currentIndex(
                    benchmark);
            assertNull(secondIndex.get(Integer.valueOf(seededKeyCount)));
            final Path secondIterationDir = currentIterationDir(benchmark);
            assertNotEquals(firstIterationDir, secondIterationDir);

            diagnostics.captureIterationEnd(benchmark);
            benchmark.flushAfterIteration();
            assertTrue(diagnostics.diag_directoryCountDelta >= 0L);
            assertTrue(diagnostics.diag_fileBytesDelta >= 0L);
            assertTrue(diagnostics.diag_fileCountDelta >= 0L);
            assertFalse(Files.exists(secondIterationDir));
        } finally {
            benchmark.tearDown();
        }
    }

    private void configureBenchmark(
            final SegmentIndexPersistedMutationBenchmark benchmark)
            throws Exception {
        setField(benchmark, "seededKeyCount", Integer.valueOf(8));
        setField(benchmark, "valueLength", Integer.valueOf(24));
        setField(benchmark, "snappy", Boolean.FALSE);
        setField(benchmark, "walMode", "off");
        setField(benchmark, "flushBatchSize", Integer.valueOf(4));
    }

    @SuppressWarnings("unchecked")
    private SegmentIndex<Integer, String> currentIndex(
            final SegmentIndexPersistedMutationBenchmark benchmark)
            throws Exception {
        return (SegmentIndex<Integer, String>) field(benchmark, "index");
    }

    private Path currentIterationDir(
            final SegmentIndexPersistedMutationBenchmark benchmark)
            throws Exception {
        return ((java.io.File) field(benchmark, "iterationDir")).toPath();
    }

    private int intField(final Object instance, final String fieldName)
            throws Exception {
        return ((Integer) field(instance, fieldName)).intValue();
    }

    private Object field(final Object instance, final String fieldName)
            throws Exception {
        final Field field = instance.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(instance);
    }

    private void setField(final Object instance, final String fieldName,
            final Object value) throws Exception {
        final Field field = instance.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(instance, value);
    }
}
