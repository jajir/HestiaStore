package org.hestiastore.index.datatype;

import java.util.OptionalInt;

/**
 * Descriptor for strict UTF-8 strings with an unsigned one-byte payload length
 * and a maximum payload of 255 encoded bytes.
 *
 * <p>
 * The limit applies to UTF-8 payload bytes, not Java UTF-16 code units or
 * displayed characters.
 * </p>
 */
public final class TypeDescriptorTinyUtf8String
        extends TypeDescriptorUtf8String {

    private static final int MAX_SERIALIZED_BYTES = 1
            + UnsignedByteLengthWriter.MAX_PAYLOAD_BYTES;

    /**
     * Creates a strict UTF-8 string descriptor limited to 255 payload bytes.
     */
    public TypeDescriptorTinyUtf8String() {
    }

    /**
     * Returns the unsigned-byte length writer.
     *
     * @return writer
     */
    @Override
    public TypeWriter<String> getTypeWriter() {
        return new UnsignedByteLengthWriter<String>(getTypeEncoder());
    }

    /**
     * Returns the unsigned-byte length reader.
     *
     * @return reader
     */
    @Override
    public TypeReader<String> getTypeReader() {
        return new UnsignedByteLengthReader<String>(getTypeDecoder());
    }

    /**
     * Returns the conservative maximum serialized size including the prefix.
     *
     * @return 256 bytes
     */
    @Override
    public OptionalInt getEstimatedAverageSizeInBytes() {
        return OptionalInt.of(MAX_SERIALIZED_BYTES);
    }
}
