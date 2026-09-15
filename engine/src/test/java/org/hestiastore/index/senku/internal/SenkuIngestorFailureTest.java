package org.hestiastore.index.senku.internal;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doThrow;

import java.time.Duration;
import java.util.concurrent.locks.ReentrantLock;

import org.hestiastore.index.IndexException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SenkuIngestorFailureTest {

    @Mock
    private SenkuFlushWriter<Integer, Long> flushWriter;

    private SenkuIngestor<Integer, Long> ingestor;

    @BeforeEach
    void setUp() {
        ingestor = new SenkuIngestor<>(new ReentrantLock(),
                (key, first, second) -> first + second, key -> key,
                flushWriter, 1, 1);
    }

    @Test
    void uncheckedFlushFailureClosesAdmissionAndAllowsShutdown() {
        final IllegalStateException cause = new IllegalStateException(
                "Unable to open flush directory.");
        doThrow(cause).when(flushWriter).write(anyLong(), anyList());

        final IndexException failure = assertThrows(IndexException.class,
                () -> ingestor.put(1, 1L));

        assertSame(cause, failure.getCause());
        assertSame(failure, assertThrows(IndexException.class,
                () -> ingestor.put(2, 2L)));
        assertTimeoutPreemptively(Duration.ofSeconds(5),
                ingestor::awaitFlushCompletion);
        assertSame(failure, assertThrows(IndexException.class,
                ingestor::stopAcceptingAndFlush));
    }

    @Test
    void indexFlushFailureRetainsItsIdentity() {
        final IndexException failure = new IndexException("Flush failed.");
        doThrow(failure).when(flushWriter).write(anyLong(), anyList());

        assertSame(failure, assertThrows(IndexException.class,
                () -> ingestor.put(1, 1L)));
        assertTimeoutPreemptively(Duration.ofSeconds(5),
                ingestor::awaitFlushCompletion);
    }

    @Test
    void idleFailureShutdownCanBeRepeatedWithoutPublishingEntries() {
        ingestor.setPaused(true);

        ingestor.fail();
        ingestor.awaitFlushCompletion();
        ingestor.fail();
        ingestor.awaitFlushCompletion();

        assertFalse(ingestor.isPaused());
        assertThrows(IndexException.class, () -> ingestor.put(1, 1L));
    }
}
