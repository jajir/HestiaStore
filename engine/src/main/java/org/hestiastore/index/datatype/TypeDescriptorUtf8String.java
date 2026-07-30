package org.hestiastore.index.datatype;

import java.util.Comparator;

/**
 * Descriptor for strict UTF-8 strings prefixed by their canonical unsigned
 * LEB128 encoded byte length.
 *
 * <p>
 * The length prefix describes UTF-8 payload bytes, not Java UTF-16 code units
 * or displayed characters.
 * </p>
 */
public class TypeDescriptorUtf8String implements TypeDescriptor<String> {

    private static final Utf8StringCodec CODEC = Utf8StringCodec.INSTANCE;

    /**
     * Tombstone value reserved for delete semantics.
     */
    public static final String TOMBSTONE_VALUE = "(*&^%$#@!)-1eaa9b2c-3c11-11ee-be56-0242ac120002";

    /**
     * Creates a general strict UTF-8 string descriptor.
     */
    public TypeDescriptorUtf8String() {
    }

    /**
     * Returns the strict UTF-8 decoder.
     *
     * @return decoder
     */
    @Override
    public TypeDecoder<String> getTypeDecoder() {
        return CODEC;
    }

    /**
     * Returns the strict UTF-8 encoder.
     *
     * @return encoder
     */
    @Override
    public TypeEncoder<String> getTypeEncoder() {
        return CODEC;
    }

    /**
     * Returns the unsigned LEB128 length writer.
     *
     * @return writer
     */
    @Override
    public TypeWriter<String> getTypeWriter() {
        return new VarIntLengthWriter<String>(getTypeEncoder());
    }

    /**
     * Returns the unsigned LEB128 length reader.
     *
     * @return reader
     */
    @Override
    public TypeReader<String> getTypeReader() {
        return new VarIntLengthReader<String>(getTypeDecoder());
    }

    /**
     * Returns natural Java string ordering.
     *
     * @return comparator
     */
    @Override
    public Comparator<String> getComparator() {
        return String::compareTo;
    }

    /**
     * Returns the tombstone marker.
     *
     * @return tombstone value
     */
    @Override
    public String getTombstone() {
        return TOMBSTONE_VALUE;
    }

    /**
     * Returns whether the object has the same descriptor type.
     *
     * @param obj object to compare
     * @return {@code true} when both objects have the same descriptor type
     */
    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        return obj != null && getClass() == obj.getClass();
    }

    /**
     * Returns a stable descriptor-type hash code.
     *
     * @return hash code
     */
    @Override
    public int hashCode() {
        return getClass().hashCode();
    }
}
