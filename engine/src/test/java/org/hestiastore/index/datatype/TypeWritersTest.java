package org.hestiastore.index.datatype;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

    @Test
    void varLengthWriter_rejectsNegativeDeclaredLength() {
        final VarLengthWriter<String> writer = new VarLengthWriter<>(
                new TypeEncoder<String>() {
                    @Override
                    public int bytesLength(final String value) {
                        return -1;
                    }

                    @Override
                    public int toBytes(final String value,
                            final byte[] destination) {
                        throw new IllegalStateException("must not be called");
                    }
                });

        final IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> writer.write(new CollectingFileWriter(), "abc"));
        assertTrue(error.getMessage().contains("payloadLength"));
    }

    @Test
    void varLengthWriter_writesPayloadViaRangeWrite() {
        final VarLengthWriter<String> writer = new VarLengthWriter<>(
                new TypeDescriptorString().getTypeEncoder());
        final ProbeFileWriter fileWriter = new ProbeFileWriter();

        final int writtenBytes = writer.write(fileWriter, "abcdef");

        assertEquals(10, writtenBytes);
        assertEquals(1, fileWriter.arrayWrites);
        assertEquals(1, fileWriter.rangeWrites);
        assertEquals(4, fileWriter.lastArrayLength);
        assertEquals(0, fileWriter.lastRangeOffset);
        assertEquals(6, fileWriter.lastRangeLength);
    }

    @Test
    void varShortLengthWriter_writesPayloadViaRangeWrite() {
        final VarShortLengthWriter<String> writer = new VarShortLengthWriter<>(
                new TypeDescriptorShortString().getTypeEncoder());
        final ProbeFileWriter fileWriter = new ProbeFileWriter();

        final int writtenBytes = writer.write(fileWriter, "abc");

        assertEquals(4, writtenBytes);
        assertEquals(0, fileWriter.arrayWrites);
        assertEquals(1, fileWriter.byteWrites);
        assertEquals(1, fileWriter.rangeWrites);
        assertEquals(0, fileWriter.lastRangeOffset);
        assertEquals(3, fileWriter.lastRangeLength);
    }

    @Test
    void fixedLengthWriter_writesPayloadViaRangeWrite() {
        final FixedLengthWriter<String> writer = new FixedLengthWriter<>(
                new TypeDescriptorFixedLengthString(4).getTypeEncoder());
        final ProbeFileWriter fileWriter = new ProbeFileWriter();

        final int writtenBytes = writer.write(fileWriter, "ABCD");

        assertEquals(4, writtenBytes);
        assertEquals(0, fileWriter.arrayWrites);
        assertEquals(0, fileWriter.byteWrites);
        assertEquals(1, fileWriter.rangeWrites);
        assertEquals(0, fileWriter.lastRangeOffset);
        assertEquals(4, fileWriter.lastRangeLength);
    }

    @Test
    void integerTypeWriter_writesViaRangeWrite() {
        final ProbeFileWriter fileWriter = new ProbeFileWriter();

        final int writtenBytes = new TypeDescriptorInteger().getTypeWriter()
                .write(fileWriter, 123);

        assertEquals(4, writtenBytes);
        assertEquals(0, fileWriter.arrayWrites);
        assertEquals(0, fileWriter.byteWrites);
        assertEquals(1, fileWriter.rangeWrites);
        assertEquals(4, fileWriter.lastRangeLength);
    }

    @Test
    void longTypeWriter_writesViaRangeWrite() {
        final ProbeFileWriter fileWriter = new ProbeFileWriter();

        final int writtenBytes = new TypeDescriptorLong().getTypeWriter()
                .write(fileWriter, 123L);

        assertEquals(8, writtenBytes);
        assertEquals(0, fileWriter.arrayWrites);
        assertEquals(0, fileWriter.byteWrites);
        assertEquals(1, fileWriter.rangeWrites);
        assertEquals(8, fileWriter.lastRangeLength);
    }

    @Test
    void floatTypeWriter_writesViaRangeWrite() {
        final ProbeFileWriter fileWriter = new ProbeFileWriter();

        final int writtenBytes = new TypeDescriptorFloat().getTypeWriter()
                .write(fileWriter, 1.25f);

        assertEquals(4, writtenBytes);
        assertEquals(0, fileWriter.arrayWrites);
        assertEquals(0, fileWriter.byteWrites);
        assertEquals(1, fileWriter.rangeWrites);
        assertEquals(4, fileWriter.lastRangeLength);
    }

    @Test
    void doubleTypeWriter_writesViaRangeWrite() {
        final ProbeFileWriter fileWriter = new ProbeFileWriter();

        final int writtenBytes = new TypeDescriptorDouble().getTypeWriter()
                .write(fileWriter, 1.25d);

        assertEquals(8, writtenBytes);
        assertEquals(0, fileWriter.arrayWrites);
        assertEquals(0, fileWriter.byteWrites);
        assertEquals(1, fileWriter.rangeWrites);
        assertEquals(8, fileWriter.lastRangeLength);
    }

    @Test
    void varShortLengthWriter_rejectsDeclaredAndWrittenLengthMismatch() {
        final VarShortLengthWriter<String> writer = new VarShortLengthWriter<>(
                new TypeEncoder<String>() {
                    @Override
                    public int bytesLength(final String value) {
                        return 3;
                    }

                    @Override
                    public int toBytes(final String value,
                            final byte[] destination) {
                        destination[0] = 'a';
                        destination[1] = 'b';
                        return 2;
                    }
                });

        final IllegalStateException error = assertThrows(
                IllegalStateException.class,
                () -> writer.write(new CollectingFileWriter(), "abc"));
        assertTrue(error.getMessage().contains("declared"));
    }

    @Test
    void fixedLengthWriter_rejectsDeclaredAndWrittenLengthMismatch() {
        final FixedLengthWriter<String> writer = new FixedLengthWriter<>(
                new TypeEncoder<String>() {
                    @Override
                    public int bytesLength(final String value) {
                        return 4;
                    }

                    @Override
                    public int toBytes(final String value,
                            final byte[] destination) {
                        destination[0] = 'A';
                        destination[1] = 'B';
                        destination[2] = 'C';
                        return 3;
                    }
                });

        final IllegalStateException error = assertThrows(
                IllegalStateException.class,
                () -> writer.write(new CollectingFileWriter(), "ABCD"));
        assertTrue(error.getMessage().contains("declared"));
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

    private static final class ProbeFileWriter
            extends AbstractCloseableResource implements FileWriter {

        private int byteWrites = 0;
        private int arrayWrites = 0;
        private int rangeWrites = 0;
        private int lastArrayLength = -1;
        private int lastRangeOffset = -1;
        private int lastRangeLength = -1;

        @Override
        public void write(final byte b) {
            byteWrites++;
        }

        @Override
        public void write(final byte[] bytes) {
            arrayWrites++;
            lastArrayLength = bytes.length;
        }

        @Override
        public void write(final byte[] bytes, final int offset,
                final int length) {
            rangeWrites++;
            lastRangeOffset = offset;
            lastRangeLength = length;
        }

        @Override
        protected void doClose() {
            // no-op
        }
    }
}
