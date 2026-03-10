package org.hestiastore.index.datatype;

import static org.hestiastore.index.datatype.NullValue.NULL;
import static org.hestiastore.index.datatype.NullValue.TOMBSTONE;

import java.util.Comparator;

import org.hestiastore.index.Vldtn;

/**
 * Descriptor for {@link NullValue} markers.
 */
public class TypeDescriptorNull implements TypeDescriptor<NullValue> {

    /**
     * Returns comparator where all values are treated as equal.
     *
     * @return comparator always returning zero
     */
    @Override
    public Comparator<NullValue> getComparator() {
        return (o1, o2) -> 0;
    }

    /**
     * Returns reader that always yields {@link NullValue#NULL}.
     *
     * @return reader
     */
    @Override
    public TypeReader<NullValue> getTypeReader() {
        return fileReader -> NULL;
    }

    /**
     * Returns writer that writes zero bytes.
     *
     * @return writer
     */
    @Override
    public TypeWriter<NullValue> getTypeWriter() {
        return (fileWriter, object) -> 0;
    }

    /**
     * Returns decoder that always yields {@link NullValue#NULL}.
     *
     * @return decoder
     */
    @Override
    public TypeDecoder<NullValue> getTypeDecoder() {
        return byters -> NULL;
    }

    /**
     * Returns encoder that writes zero bytes.
     *
     * @return encoder
     */
    @Override
    public TypeEncoder<NullValue> getTypeEncoder() {
        return new TypeEncoder<NullValue>() {
            @Override
            public EncodedBytes encode(final NullValue value,
                    final byte[] reusableBuffer) {
                return new EncodedBytes(
                        Vldtn.requireNonNull(reusableBuffer, "reusableBuffer"),
                        0);
            }
        };
    }

    /**
     * Returns tombstone marker.
     *
     * @return tombstone value
     */
    @Override
    public NullValue getTombstone() {
        return TOMBSTONE;
    }

}
