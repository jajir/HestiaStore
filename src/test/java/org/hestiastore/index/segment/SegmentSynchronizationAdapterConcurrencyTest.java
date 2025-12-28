package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.EntryWriter;
import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.DirectoryFacade;
import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.Test;

class SegmentSynchronizationAdapterConcurrencyTest {

    private static final SegmentId NEW_SEGMENT_ID = SegmentId.of(2);

    @Test
    void read_operations_do_not_block_each_other() throws Exception {
        try (SegmentSynchronizationAdapter<Integer, String> segment = newAdapter()) {
            final EntryIterator<Integer, String> iterator = segment
                    .openIterator();
            final ExecutorService executor = Executors.newSingleThreadExecutor();
            try {
                final Future<String> future = executor
                        .submit(() -> segment.get(1));
                assertNull(future.get(1, TimeUnit.SECONDS));
            } finally {
                iterator.close();
                executor.shutdownNow();
            }
        }
    }

    @Test
    void write_operation_waits_for_reader_to_close() throws Exception {
        try (SegmentSynchronizationAdapter<Integer, String> segment = newAdapter()) {
            final EntryIterator<Integer, String> iterator = segment
                    .openIterator();
            final ExecutorService executor = Executors.newSingleThreadExecutor();
            final CountDownLatch started = new CountDownLatch(1);
            final CountDownLatch acquired = new CountDownLatch(1);
            final CountDownLatch closeSignal = new CountDownLatch(1);
            try {
                final Future<?> future = executor.submit(() -> {
                    started.countDown();
                    try (EntryWriter<Integer, String> writer = segment
                            .openDeltaCacheWriter()) {
                        acquired.countDown();
                        closeSignal.await();
                    }
                    return null;
                });
                assertTrue(started.await(1, TimeUnit.SECONDS),
                        "Writer task did not start");
                assertFalse(acquired.await(250, TimeUnit.MILLISECONDS),
                        "Writer acquired lock while reader was open");
                iterator.close();
                assertTrue(acquired.await(2, TimeUnit.SECONDS),
                        "Writer did not acquire lock after reader closed");
                closeSignal.countDown();
                future.get(2, TimeUnit.SECONDS);
            } finally {
                if (!iterator.wasClosed()) {
                    iterator.close();
                }
                closeSignal.countDown();
                executor.shutdownNow();
            }
        }
    }

    @Test
    void read_operation_waits_for_writer_to_close() throws Exception {
        try (SegmentSynchronizationAdapter<Integer, String> segment = newAdapter()) {
            final EntryWriter<Integer, String> writer = segment
                    .openDeltaCacheWriter();
            final ExecutorService executor = Executors.newSingleThreadExecutor();
            final CountDownLatch started = new CountDownLatch(1);
            final CountDownLatch acquired = new CountDownLatch(1);
            final CountDownLatch closeSignal = new CountDownLatch(1);
            try {
                final Future<?> future = executor.submit(() -> {
                    started.countDown();
                    try (EntryIterator<Integer, String> iterator = segment
                            .openIterator()) {
                        acquired.countDown();
                        closeSignal.await();
                    }
                    return null;
                });
                assertTrue(started.await(1, TimeUnit.SECONDS),
                        "Reader task did not start");
                assertFalse(acquired.await(250, TimeUnit.MILLISECONDS),
                        "Reader acquired lock while writer was open");
                writer.close();
                assertTrue(acquired.await(2, TimeUnit.SECONDS),
                        "Reader did not acquire lock after writer closed");
                closeSignal.countDown();
                future.get(2, TimeUnit.SECONDS);
            } finally {
                if (!writer.wasClosed()) {
                    writer.close();
                }
                closeSignal.countDown();
                executor.shutdownNow();
            }
        }
    }

    @Test
    void split_waits_for_writer_to_close() throws Exception {
        try (SegmentSynchronizationAdapter<Integer, String> segment = newAdapter()) {
            final SegmentSplitterPlan<Integer, String> plan = SegmentSplitterPlan
                    .fromPolicy(segment.getSegmentSplitterPolicy());
            final EntryWriter<Integer, String> writer = segment
                    .openDeltaCacheWriter();
            final ExecutorService executor = Executors.newSingleThreadExecutor();
            final CountDownLatch started = new CountDownLatch(1);
            final CountDownLatch completed = new CountDownLatch(1);
            final java.util.concurrent.atomic.AtomicReference<Throwable> error = new java.util.concurrent.atomic.AtomicReference<>();
            try {
                executor.submit(() -> {
                    started.countDown();
                    try {
                        segment.split(NEW_SEGMENT_ID, plan);
                    } catch (final Throwable t) {
                        error.set(t);
                    } finally {
                        completed.countDown();
                    }
                    return null;
                });
                assertTrue(started.await(1, TimeUnit.SECONDS),
                        "Split task did not start");
                assertFalse(completed.await(250, TimeUnit.MILLISECONDS),
                        "Split completed while writer was open");
                writer.close();
                assertTrue(completed.await(2, TimeUnit.SECONDS),
                        "Split did not finish after writer closed");
                final Throwable thrown = error.get();
                assertTrue(thrown instanceof IllegalStateException);
                assertEquals("Splitting failed. Number of keys is too low.",
                        thrown.getMessage());
            } finally {
                if (!writer.wasClosed()) {
                    writer.close();
                }
                executor.shutdownNow();
            }
        }
    }

    @Test
    void second_writer_waits_for_first_writer_to_close() throws Exception {
        try (SegmentSynchronizationAdapter<Integer, String> segment = newAdapter()) {
            final EntryWriter<Integer, String> writer = segment
                    .openDeltaCacheWriter();
            final ExecutorService executor = Executors.newSingleThreadExecutor();
            final CountDownLatch started = new CountDownLatch(1);
            final CountDownLatch acquired = new CountDownLatch(1);
            final CountDownLatch closeSignal = new CountDownLatch(1);
            try {
                final Future<?> future = executor.submit(() -> {
                    started.countDown();
                    try (EntryWriter<Integer, String> second = segment
                            .openDeltaCacheWriter()) {
                        acquired.countDown();
                        closeSignal.await();
                    }
                    return null;
                });
                assertTrue(started.await(1, TimeUnit.SECONDS),
                        "Second writer task did not start");
                assertFalse(acquired.await(250, TimeUnit.MILLISECONDS),
                        "Second writer acquired lock while first writer was open");
                writer.close();
                assertTrue(acquired.await(2, TimeUnit.SECONDS),
                        "Second writer did not acquire lock after close");
                closeSignal.countDown();
                future.get(2, TimeUnit.SECONDS);
            } finally {
                if (!writer.wasClosed()) {
                    writer.close();
                }
                closeSignal.countDown();
                executor.shutdownNow();
            }
        }
    }

    private static SegmentSynchronizationAdapter<Integer, String> newAdapter() {
        final Directory directory = new MemDirectory();
        final Segment<Integer, String> segment = Segment.<Integer, String>builder()//
                .withDirectoryFacade(DirectoryFacade.of(directory))//
                .withId(SegmentId.of(1))//
                .withKeyTypeDescriptor(new TypeDescriptorInteger())//
                .withValueTypeDescriptor(new TypeDescriptorShortString())//
                .withEncodingChunkFilters(List.of(new ChunkFilterDoNothing()))//
                .withDecodingChunkFilters(List.of(new ChunkFilterDoNothing()))//
                .build();
        return new SegmentSynchronizationAdapter<>(segment);
    }
}
