package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.hestiastore.index.AbstractDataTest;
import org.hestiastore.index.Entry;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentState;
import org.junit.jupiter.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractSegmentIndexTest extends AbstractDataTest {

    private final Logger logger = LoggerFactory
            .getLogger(AbstractSegmentIndexTest.class);

    /**
     * Simplify filling index with data.
     * 
     * @param <M>   key type
     * @param <N>   value type
     * @param seg   required index
     * @param entries required list of entries
     */
    protected <M, N> void writeEntries(final SegmentIndex<M, N> index,
            final List<Entry<M, N>> entries) {
        for (final Entry<M, N> entry : entries) {
            index.put(entry);
        }
    }

    /**
     * Open segment search and verify that found value for given key is equals
     * to expected value
     * 
     * @param <M>   key type
     * @param <N>   value type
     * @param seg   required segment
     * @param entries required list of entries of key and expected value
     */
    protected <M, N> void verifyIndexSearch(final SegmentIndex<M, N> index,
            final List<Entry<M, N>> entries) {
        entries.forEach(entry -> {
            final M key = entry.getKey();
            final N expectedValue = entry.getValue();
            assertEquals(expectedValue, index.get(key));
        });
    }

    /**
     * Open index search and verify that found value for given key is equals to
     * expecetd value
     * 
     * @param <M>   key type
     * @param <N>   value type
     * @param seg   required index
     * @param entries required list of expected data in index
     */
    protected <M, N> void verifyIndexData(final SegmentIndex<M, N> index,
            final List<Entry<M, N>> entries) {
        final List<Entry<M, N>> data = toList(
                index.getStream(SegmentWindow.unbounded()));
        assertEquals(entries.size(), data.size(),
                "Unexpected number of entries in index");
        for (int i = 0; i < entries.size(); i++) {
            final Entry<M, N> expectedPair = entries.get(i);
            final Entry<M, N> realPair = data.get(i);
            assertEquals(expectedPair, realPair);
        }
    }

    protected int numberOfFilesInDirectory(final Directory directory) {
        return (int) directory.getFileNames().count();
    }

    protected int numberOfFilesInDirectoryP(final Directory directory) {
        final AtomicInteger cx = new AtomicInteger(0);
        directory.getFileNames().forEach(fileName -> {
            logger.debug("Found file name {}", fileName);
            cx.incrementAndGet();
        });
        return cx.get();
    }

    protected void awaitMaintenanceIdle(final SegmentIndex<?, ?> index) {
        final long deadline = System.nanoTime()
                + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < deadline) {
            final SegmentRegistry<?, ?> registry = readSegmentRegistry(index);
            final Map<SegmentId, Segment<?, ?>> segments = readSegmentsMap(
                    registry);
            boolean idle = true;
            for (final Segment<?, ?> segment : segments.values()) {
                final SegmentState state = segment.getState();
                if (state == SegmentState.ERROR) {
                    Assertions.fail(
                            "Segment entered ERROR during maintenance");
                }
                if (state == SegmentState.MAINTENANCE_RUNNING
                        || state == SegmentState.FREEZE) {
                    idle = false;
                    break;
                }
            }
            if (idle) {
                return;
            }
            try {
                Thread.sleep(10);
            } catch (final InterruptedException ex) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        Assertions.fail("Timed out waiting for maintenance to finish");
    }

    @SuppressWarnings("unchecked")
    private static SegmentRegistry<?, ?> readSegmentRegistry(
            final SegmentIndex<?, ?> index) {
        try {
            final SegmentIndexImpl<?, ?> impl = unwrapSegmentIndex(index);
            final Field field = SegmentIndexImpl.class
                    .getDeclaredField("segmentRegistry");
            field.setAccessible(true);
            return (SegmentRegistry<?, ?>) field.get(impl);
        } catch (final ReflectiveOperationException ex) {
            throw new IllegalStateException(
                    "Unable to read segmentRegistry for test", ex);
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<SegmentId, Segment<?, ?>> readSegmentsMap(
            final SegmentRegistry<?, ?> registry) {
        try {
            final Field field = SegmentRegistry.class
                    .getDeclaredField("segments");
            field.setAccessible(true);
            return (Map<SegmentId, Segment<?, ?>>) field.get(registry);
        } catch (final ReflectiveOperationException ex) {
            throw new IllegalStateException(
                    "Unable to read segments map for test", ex);
        }
    }

    private static SegmentIndexImpl<?, ?> unwrapSegmentIndex(
            final SegmentIndex<?, ?> index) {
        Object current = index;
        while (!(current instanceof SegmentIndexImpl<?, ?>)) {
            try {
                final Field field = current.getClass()
                        .getDeclaredField("index");
                field.setAccessible(true);
                current = field.get(current);
            } catch (final ReflectiveOperationException ex) {
                throw new IllegalStateException(
                        "Unable to unwrap index for test", ex);
            }
        }
        return (SegmentIndexImpl<?, ?>) current;
    }

}
