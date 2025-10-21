package org.hestiastore.index;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class GuardedWriteTransactionTest {

    private static final class RecordingTransaction
            extends GuardedWriteTransaction<PairWriter<String, String>> {

        private final RecordingPairWriter delegateWriter = new RecordingPairWriter();
        private boolean committed;

        @Override
        protected PairWriter<String, String> doOpen() {
            return delegateWriter;
        }

        @Override
        protected void doCommit(final PairWriter<String, String> resource) {
            committed = true;
        }

        RecordingPairWriter delegateWriter() {
            return delegateWriter;
        }

        boolean wasCommitted() {
            return committed;
        }
    }

    private static final class RecordingPairWriter implements
            PairWriter<String, String> {
        int closeCount;
        int writeCount;

        @Override
        public void write(final Pair<String, String> pair) {
            writeCount++;
        }

        @Override
        public void close() {
            closeCount++;
        }
    }

    @Test
    void openWriter_twice_throws() {
        final RecordingTransaction tx = new RecordingTransaction();
        tx.open();
        assertThrows(IllegalStateException.class, tx::open);
    }

    @Test
    void commit_before_open_throws() {
        final RecordingTransaction tx = new RecordingTransaction();
        assertThrows(IllegalStateException.class, tx::commit);
    }

    @Test
    void commit_twice_throws() {
        final RecordingTransaction tx = new RecordingTransaction();
        tx.open().close();
        tx.commit();
        assertThrows(IllegalStateException.class, tx::commit);
    }

    @Test
    void writer_close_and_commit_delegate_once() {
        final RecordingTransaction tx = new RecordingTransaction();
        PairWriter<String, String> writer = tx.open();
        writer.write(Pair.of("k", "v"));
        writer.close();

        tx.commit();

        RecordingPairWriter delegate = tx.delegateWriter();
        assertEquals(1, delegate.writeCount);
        assertEquals(1, delegate.closeCount);
        assertEquals(true, tx.wasCommitted());
    }
}
