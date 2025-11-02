package org.hestiastore.index.datatype;

import java.util.Comparator;
import java.util.List;

import org.hestiastore.index.bytes.ByteSequence;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.FileReader;
import org.hestiastore.index.directory.MemFileReader;

public class TypeDescriptorComposite implements TypeDescriptor<CompositeValue> {

    private final VarLengthReader<CompositeValue> varLengthReader = new VarLengthReader<>(
            getConvertorFromBytes());
    private final VarLengthWriter<CompositeValue> varLengthWriter = new VarLengthWriter<>(
            this::toBytesBuffer);
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
    public ConvertorToBytes<CompositeValue> getConvertorToBytes() {
        return this::toBytesBuffer;
    }

    @Override
    public ConvertorFromBytes<CompositeValue> getConvertorFromBytes() {
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
     * @param a required first object to compare
     * @param b required second object to compare
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
        return (a, b) -> {
            if (a.size() != elementTypes.size()
                    || b.size() != elementTypes.size()) {
                throw new IndexException(
                        "CompositeValue size does not match expected elementTypes size");
            }

            for (int i = 0; i < elementTypes.size(); i++) {
                Object valA = a.get(i);
                Object valB = b.get(i);

                if (valA == null || valB == null) {
                    throw new IndexException(
                            "CompositeValue contains null at index " + i);
                }

                Class<?> expectedClass = elementTypes.get(i).getClass();
                if (!expectedClass.isInstance(valA)
                        || !expectedClass.isInstance(valB)) {
                    throw new IndexException("Element at index " + i
                            + " is not of expected type: "
                            + expectedClass.getName());
                }

                Comparator<Object> cmp = elementDescriptor(i).getComparator();
                int result = cmp.compare(valA, valB);
                if (result != 0) {
                    return result;
                }
            }

            return 0;
        };
    }

    @Override
    public CompositeValue getTombstone() {
        return tombstoneValue;
    }

    private ByteSequence toBytesBuffer(final CompositeValue value) {
        try (ByteSequenceAccumulator accumulator = new ByteSequenceAccumulator()) {
            for (int i = 0; i < elementTypes.size(); i++) {
                final TypeDescriptor<Object> descriptor = elementDescriptor(i);
                final TypeWriter<Object> writer = descriptor.getTypeWriter();
                writer.write(accumulator, value.get(i));
            }
            return accumulator.toBytes();
        }
    }

    public CompositeValue fromBytes(final ByteSequence bytes) {
        Vldtn.requireNonNull(bytes, "bytes");
        final FileReader fileReader = new MemFileReader(bytes);
        final Object[] out = new Object[elementTypes.size()];
        for (int i = 0; i < elementTypes.size(); i++) {
            final TypeDescriptor<Object> descriptor = elementDescriptor(i);
            final TypeReader<Object> reader = descriptor.getTypeReader();
            out[i] = reader.read(fileReader);
        }
        return new CompositeValue(out);
    }

    @SuppressWarnings("unchecked")
    private TypeDescriptor<Object> elementDescriptor(final int index) {
        return (TypeDescriptor<Object>) elementTypes.get(index);
    }

}
