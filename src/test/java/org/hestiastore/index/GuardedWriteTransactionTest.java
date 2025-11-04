package org.hestiastore.index;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class GuardedWriteTransactionTest {

    private static final class NullResourceTransaction
            extends GuardedWriteTransaction<EntryWriter<String, String>> {

        @Override
        protected EntryWriter<String, String> doOpen() {
            return null;
        }

        @Override
        protected void doCommit(final EntryWriter<String, String> resource) {
        }
    }

    private static final class RecordingTransaction
            extends GuardedWriteTransaction<EntryWriter<String, String>> {

        private final RecordingEntryWriter delegateWriter = new RecordingEntryWriter();
        private boolean committed;

        @Override
        protected EntryWriter<String, String> doOpen() {
            return delegateWriter;
        }

        @Override
        protected void doCommit(final EntryWriter<String, String> resource) {
            committed = true;
        }

        RecordingEntryWriter delegateWriter() {
            return delegateWriter;
        }

        boolean wasCommitted() {
            return committed;
        }
    }

    private static final class RecordingEntryWriter extends
            AbstractCloseableResource implements EntryWriter<String, String> {
        int closeCount;
        int writeCount;

        @Override
        public void write(final Entry<String, String> entry) {
            writeCount++;
        }

        @Override
        protected void doClose() {
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
    void commit_requires_resource_to_be_closed() {
        final RecordingTransaction tx = new RecordingTransaction();
        tx.open();
        final Exception ex = assertThrows(IllegalStateException.class,
                tx::commit);
        assertEquals("Resource must be closed before commit", ex.getMessage());
    }

    @Test
    void open_requires_non_null_resource() {
        final NullResourceTransaction tx = new NullResourceTransaction();
        final Exception ex = assertThrows(IllegalArgumentException.class,
                tx::open);
        assertEquals("Property 'resource' must not be null.", ex.getMessage());
    }

    @Test
    void writer_close_and_commit_delegate_once() {
        final RecordingTransaction tx = new RecordingTransaction();
        EntryWriter<String, String> writer = tx.open();
        writer.write(Entry.of("k", "v"));
        writer.close();

        tx.commit();

        RecordingEntryWriter delegate = tx.delegateWriter();
        assertEquals(1, delegate.writeCount);
        assertEquals(1, delegate.closeCount);
        assertEquals(true, tx.wasCommitted());
    }
}
