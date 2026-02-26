package org.hestiastore.index.datatype;

import java.util.Comparator;
import java.util.List;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.FileReader;
import org.hestiastore.index.directory.FileWriter;
import org.hestiastore.index.directory.MemFileReader;

/**
 * Type descriptor for {@link CompositeValue} that encodes and decodes a fixed
 * sequence of element types.
 *
 * <p>
 * Elements are serialized by delegating to each element type descriptor in
 * order. Comparison follows the same order and returns the first non-zero
 * result, mirroring lexicographic ordering. A tombstone value is composed from
 * the element tombstones.
 * </p>
 */
public class TypeDescriptorComposite implements TypeDescriptor<CompositeValue> {

    private final TypeEncoder<CompositeValue> convertorToBytes = new TypeEncoder<CompositeValue>() {
        @Override
        public int bytesLength(final CompositeValue object) {
            final CountingFileWriter writer = new CountingFileWriter();
            writeElements(writer, object);
            return writer.writtenBytes();
        }

        @Override
        public int toBytes(final CompositeValue object,
                final byte[] destination) {
            final FixedBufferFileWriter writer = new FixedBufferFileWriter(
                    destination);
            writeElements(writer, object);
            return writer.writtenBytes();
        }
    };
    private final VarLengthReader<CompositeValue> varLengthReader = new VarLengthReader<>(
            getTypeDecoder());
    private final VarLengthWriter<CompositeValue> varLengthWriter = new VarLengthWriter<>(
            convertorToBytes);
    private final List<TypeDescriptor<?>> elementTypes;
    private final CompositeValue tombstoneValue;

    TypeDescriptorComposite(final List<TypeDescriptor<?>> elementTypes) {
        this.elementTypes = Vldtn.requireNonNull(elementTypes, "elementTypes");
        if (elementTypes.isEmpty()) {
            throw new IllegalArgumentException("Element types cannot be empty");
        }
        final Object[] tmp = new Object[elementTypes.size()];
        for (int i = 0; i < elementTypes.size(); i++) {
            tmp[i] = elementTypes.get(i).getTombstone();
        }
        this.tombstoneValue = CompositeValue.of(tmp);
    }

    @Override
    public TypeEncoder<CompositeValue> getTypeEncoder() {
        return convertorToBytes;
    }

    @Override
    public TypeDecoder<CompositeValue> getTypeDecoder() {
        return this::fromBytes;
    }

    @Override
    public TypeReader<CompositeValue> getTypeReader() {
        return varLengthReader::read;
    }

    @Override
    public TypeWriter<CompositeValue> getTypeWriter() {
        return varLengthWriter::write;
    }

    /**
     * Compare elements one by one in order, and return the result of the first
     * non-zero comparison.
     * 
     * @return result of object comparison
     * @throws IndexException Throw an IndexException if the sizes of the two
     *                        CompositeValue instances differ.
     * @throws IndexException Throw an IndexException if any element in the
     *                        composite is null.
     * @throws IndexException Throw an exception if any element’s type does not
     *                        match the corresponding expected type from
     *                        elementTypes.
     */
    @Override
    public Comparator<CompositeValue> getComparator() {
        return this::compareCompositeValues;
    }

    @Override
    public CompositeValue getTombstone() {
        return tombstoneValue;
    }

    private int compareCompositeValues(final CompositeValue a,
            final CompositeValue b) {
        validateCompositeSize(a, b);
        for (int i = 0; i < elementTypes.size(); i++) {
            final int result = compareElementAt(a, b, i);
            if (result != 0) {
                return result;
            }
        }
        return 0;
    }

    private void validateCompositeSize(final CompositeValue a,
            final CompositeValue b) {
        if (a.size() != elementTypes.size()
                || b.size() != elementTypes.size()) {
            throw new IndexException(
                    "CompositeValue size does not match expected elementTypes size");
        }
    }

    @SuppressWarnings("unchecked")
    private int compareElementAt(final CompositeValue a, final CompositeValue b,
            final int index) {
        final Object valA = a.get(index);
        final Object valB = b.get(index);
        ensureValuesAreNotNull(index, valA, valB);
        final Object expectedValue = tombstoneValue.get(index);
        ensureValuesHaveExpectedType(index, valA, valB, expectedValue);
        final Comparator<Object> cmp = ((TypeDescriptor<Object>) elementTypes
                .get(index)).getComparator();
        return cmp.compare(valA, valB);
    }

    private void ensureValuesAreNotNull(final int index, final Object valA,
            final Object valB) {
        if (valA == null || valB == null) {
            throw new IndexException(
                    "CompositeValue contains null at index " + index);
        }
    }

    private void ensureValuesHaveExpectedType(final int index,
            final Object valA, final Object valB, final Object expectedValue) {
        if (expectedValue == null) {
            return;
        }
        final Class<?> expectedClass = expectedValue.getClass();
        if (!expectedClass.isInstance(valA)
                || !expectedClass.isInstance(valB)) {
            throw new IndexException("Element at index " + index
                    + " is not of expected type: " + expectedClass.getName());
        }
    }

    @SuppressWarnings("unchecked")
    public byte[] toBytes(final CompositeValue value) {
        final ByteArrayWriter byteArrayWriter = new ByteArrayWriter();
        writeElements(byteArrayWriter, value);
        return byteArrayWriter.toByteArray();
    }

    @SuppressWarnings("unchecked")
    public CompositeValue fromBytes(final byte[] bytes) {
        Vldtn.requireNonNull(bytes, "bytes");
        final FileReader fileReader = new MemFileReader(bytes);
        final Object[] out = new Object[elementTypes.size()];
        for (int i = 0; i < elementTypes.size(); i++) {
            final TypeReader<Object> convertor = ((TypeDescriptor<Object>) elementTypes
                    .get(i)).getTypeReader();
            out[i] = convertor.read(fileReader);
        }
        return new CompositeValue(out);
    }

    @SuppressWarnings("unchecked")
    private void writeElements(final FileWriter writer,
            final CompositeValue value) {
        for (int i = 0; i < elementTypes.size(); i++) {
            final TypeWriter<Object> typeWriter = ((TypeDescriptor<Object>) elementTypes
                    .get(i)).getTypeWriter();
            typeWriter.write(writer, value.get(i));
        }
    }

    private static final class CountingFileWriter
            extends AbstractCloseableResource implements FileWriter {

        private long bytesWritten = 0L;

        @Override
        public void write(final byte b) {
            add(1);
        }

        @Override
        public void write(final byte[] bytes) {
            add(Vldtn.requireNonNull(bytes, "bytes").length);
        }

        int writtenBytes() {
            return (int) bytesWritten;
        }

        @Override
        protected void doClose() {
            // no-op
        }

        private void add(final int value) {
            bytesWritten += value;
            if (bytesWritten > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("Converted type is too big");
            }
        }
    }

    private static final class FixedBufferFileWriter
            extends AbstractCloseableResource implements FileWriter {

        private final byte[] destination;
        private int position = 0;

        FixedBufferFileWriter(final byte[] destination) {
            this.destination = Vldtn.requireNonNull(destination, "destination");
        }

        @Override
        public void write(final byte b) {
            ensureCapacity(1);
            destination[position++] = b;
        }

        @Override
        public void write(final byte[] bytes) {
            final byte[] in = Vldtn.requireNonNull(bytes, "bytes");
            ensureCapacity(in.length);
            System.arraycopy(in, 0, destination, position, in.length);
            position += in.length;
        }

        int writtenBytes() {
            return position;
        }

        @Override
        protected void doClose() {
            // no-op
        }

        private void ensureCapacity(final int numberOfBytesToWrite) {
            final int requiredSize = position + numberOfBytesToWrite;
            if (requiredSize > destination.length) {
                throw new IllegalArgumentException(String.format(
                        "Destination buffer too small. Required '%s' but was '%s'",
                        requiredSize, destination.length));
            }
        }
    }

}
