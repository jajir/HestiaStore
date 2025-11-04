package org.hestiastore.index;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class GuardedEntryWriterTest {

    private static final class RecordingWriter<K, V>
            extends AbstractCloseableResource implements EntryWriter<K, V> {
        private int writeCount;
        private int closeCount;

        @Override
        public void write(final Entry<K, V> entry) {
            writeCount++;
        }

        @Override
        protected void doClose() {
            closeCount++;
        }
    }

    @Test
    void write_after_close_throws() {
        final RecordingWriter<String, String> delegate = new RecordingWriter<>();
        final GuardedEntryWriter<String, String> writer = new GuardedEntryWriter<>(
                delegate);

        writer.write(Entry.of("k", "v"));
        writer.close();

        assertEquals(1, delegate.closeCount);
        assertThrows(IllegalStateException.class,
                () -> writer.write(Entry.of("k2", "v2")));
    }

    @Test
    void close_twice_is_idempotent() {
        final GuardedEntryWriter<String, String> writer = new GuardedEntryWriter<>(
                new RecordingWriter<>());

        writer.close();
        assertThrows(IllegalStateException.class, writer::close);
    }
}
