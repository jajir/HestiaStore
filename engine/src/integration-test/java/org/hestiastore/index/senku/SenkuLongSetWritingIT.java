package org.hestiastore.index.senku;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import org.hestiastore.index.Entry;
import org.hestiastore.index.datatype.NullValue;
import org.hestiastore.index.datatype.TypeDescriptorLong;
import org.hestiastore.index.datatype.TypeDescriptorNull;
import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.Test;

class SenkuLongSetWritingIT {

    @Test
    void parallelPrimitivePutsUseTheSameExactLifecycleAndRotation()
            throws Exception {
        final MemDirectory directory = new MemDirectory();
        final SenkuMergeFunctionRegistry<Long, NullValue> functions = new SenkuMergeFunctionRegistry<>();
        functions.register(SenkuMergeFunctions.longSet());
        final SenkuLongSetWriting writing = SenkuIndex
                .builder(directory, new TypeDescriptorLong(),
                        new TypeDescriptorNull(), functions)
                .shardCount(2).maxInMemoryEntries(128).maxKeysPerPage(32)
                .maxEntriesPerPart(1000).mergeFanIn(3).maintenanceThreads(2)
                .maintenanceQueueSize(8).diskIoBufferSize(8192)
                .createLongSet(Long::hashCode);
        final ExecutorService workers = Executors.newFixedThreadPool(4);
        final CountDownLatch start = new CountDownLatch(1);
        try {
            final List<? extends Future<?>> tasks = IntStream.range(0, 4)
                    .mapToObj(worker -> workers.submit(() -> {
                        try {
                            assertTrue(start.await(10, TimeUnit.SECONDS));
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw e;
                        }
                        for (long key = -100; key < 200; key++) {
                            writing.putLong(key);
                        }
                        return null;
                    })).toList();
            start.countDown();
            for (final Future<?> task : tasks) {
                task.get(20, TimeUnit.SECONDS);
            }
        } finally {
            start.countDown();
            workers.shutdownNow();
            assertTrue(workers.awaitTermination(10, TimeUnit.SECONDS));
        }
        try (SenkuReady<Long, NullValue> ready = writing.finishWriting();
                Stream<Entry<Long, NullValue>> entries = ready.openStream()) {
            assertEquals(LongStream.range(-100, 200).boxed().toList(),
                    entries.map(Entry::getKey).toList());
        }
    }
}
