package org.hestiastore.index;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class GuardedPairWriterTest {

    private static final class RecordingWriter<K, V> implements PairWriter<K, V> {
        private int writeCount;
        private int closeCount;

        @Override
        public void write(final Pair<K, V> pair) {
            writeCount++;
        }

        @Override
        public void close() {
            closeCount++;
        }
    }

    @Test
    void write_after_close_throws() {
        final RecordingWriter<String, String> delegate = new RecordingWriter<>();
        final GuardedPairWriter<String, String> writer = new GuardedPairWriter<>(
                delegate);

        writer.write(Pair.of("k", "v"));
        writer.close();

        assertEquals(1, delegate.closeCount);
        assertThrows(IllegalStateException.class,
                () -> writer.write(Pair.of("k2", "v2")));
    }

    @Test
    void close_twice_is_idempotent() {
        final GuardedPairWriter<String, String> writer = new GuardedPairWriter<>(
                new RecordingWriter<>());

        writer.close();
        assertDoesNotThrow(writer::close);
    }
}
