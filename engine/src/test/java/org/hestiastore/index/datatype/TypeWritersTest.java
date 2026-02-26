package org.hestiastore.index.datatype;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.directory.FileWriter;
import org.junit.jupiter.api.Test;

class TypeWritersTest {

    @Test
    void varLengthWriter_writesLengthPrefixAndPayload() {
        final VarLengthWriter<String> writer = new VarLengthWriter<>(
                new TypeDescriptorString().getTypeEncoder());
        final CollectingFileWriter fileWriter = new CollectingFileWriter();

        final int writtenBytes = writer.write(fileWriter, "abc");
        final byte[] data = fileWriter.toByteArray();

        assertEquals(7, writtenBytes);
        assertEquals(7, data.length);

        final int lengthPrefix = new TypeDescriptorInteger().getTypeDecoder()
                .decode(Arrays.copyOfRange(data, 0, 4));
        assertEquals(3, lengthPrefix);
        assertEquals("abc",
                new String(Arrays.copyOfRange(data, 4, 7),
                        StandardCharsets.ISO_8859_1));
    }

    @Test
    void varShortLengthWriter_respects127Boundary() {
        final VarShortLengthWriter<String> writer = new VarShortLengthWriter<>(
                new TypeDescriptorString().getTypeEncoder());
        final CollectingFileWriter fileWriter = new CollectingFileWriter();
        final String shortValue = "a".repeat(127);

        final int writtenBytes = writer.write(fileWriter, shortValue);
        final byte[] data = fileWriter.toByteArray();

        assertEquals(128, writtenBytes);
        assertEquals(127, data[0] & 0xFF);
        assertEquals(128, data.length);
    }

    @Test
    void varShortLengthWriter_rejectsLengthOver127() {
        final VarShortLengthWriter<String> writer = new VarShortLengthWriter<>(
                new TypeDescriptorString().getTypeEncoder());

        final IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> writer.write(new CollectingFileWriter(),
                        "a".repeat(128)));
        assertEquals("Converted type is too big", error.getMessage());
    }

    @Test
    void fixedLengthWriter_writesExactBytes() {
        final TypeDescriptorFixedLengthString descriptor = new TypeDescriptorFixedLengthString(
                4);
        final FixedLengthWriter<String> writer = new FixedLengthWriter<>(
                descriptor.getTypeEncoder());
        final CollectingFileWriter fileWriter = new CollectingFileWriter();

        final int writtenBytes = writer.write(fileWriter, "ABCD");
        final byte[] data = fileWriter.toByteArray();

        assertEquals(4, writtenBytes);
        assertArrayEquals("ABCD".getBytes(StandardCharsets.ISO_8859_1), data);
    }

    private static final class CollectingFileWriter
            extends AbstractCloseableResource implements FileWriter {

        private final ByteArrayOutputStream out = new ByteArrayOutputStream();

        @Override
        public void write(final byte b) {
            out.write(b);
        }

        @Override
        public void write(final byte[] bytes) {
            out.write(bytes, 0, bytes.length);
        }

        byte[] toByteArray() {
            return out.toByteArray();
        }

        @Override
        protected void doClose() {
            // no-op
        }
    }
}
