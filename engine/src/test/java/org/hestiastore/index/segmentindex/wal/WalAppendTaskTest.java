package org.hestiastore.index.segmentindex.wal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CompletionException;

import org.hestiastore.index.IndexException;
import org.junit.jupiter.api.Test;

class WalAppendTaskTest {

    @Test
    void appendTaskCompletesWrittenLsn() {
        final WalAppendTask<String, String> task = new WalAppendTask<>(
                WalRuntime.Operation.PUT, "key", "value");

        task.complete(11L);

        assertFalse(task.stop());
        assertEquals(WalRuntime.Operation.PUT, task.operation());
        assertEquals("key", task.key());
        assertEquals("value", task.value());
        assertEquals(11L, task.writtenLsn().join());
    }

    @Test
    void stopTaskMarksQueueStop() {
        final WalAppendTask<String, String> task = WalAppendTask.stopTask();

        assertTrue(task.stop());
    }

    @Test
    void appendTaskPropagatesFailure() {
        final WalAppendTask<String, String> task = new WalAppendTask<>(
                WalRuntime.Operation.DELETE, "key", null);

        task.fail(new IndexException("failed"));

        assertThrows(CompletionException.class, () -> task.writtenLsn().join());
    }
}
