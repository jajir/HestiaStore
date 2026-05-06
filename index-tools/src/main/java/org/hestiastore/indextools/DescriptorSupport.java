package org.hestiastore.indextools;

import java.util.Arrays;
import java.util.Base64;

import org.hestiastore.index.datatype.EncodedBytes;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.configuration.types.DataTypeDescriptorRegistry;

final class DescriptorSupport {

    private DescriptorSupport() {
    }

    static DescriptorPair fromConfiguration(
            final IndexConfiguration<?, ?> configuration) {
        final var identity = configuration.identity();
        return new DescriptorPair(identity.keyClass(),
                identity.valueClass(),
                identity.keyTypeDescriptor(),
                identity.valueTypeDescriptor(),
                castDescriptor(DataTypeDescriptorRegistry
                        .makeInstance(identity.keyTypeDescriptor())),
                castDescriptor(DataTypeDescriptorRegistry
                        .makeInstance(identity.valueTypeDescriptor())));
    }

    private static TypeDescriptor<Object> castDescriptor(
            final TypeDescriptor<?> descriptor) {
        @SuppressWarnings("unchecked")
        final TypeDescriptor<Object> typedDescriptor = (TypeDescriptor<Object>) descriptor;
        return typedDescriptor;
    }

    static byte[] encode(final TypeDescriptor<Object> descriptor,
            final Object value) {
        final EncodedBytes encoded = descriptor.getTypeEncoder().encode(value,
                new byte[64]);
        return Arrays.copyOf(encoded.getBytes(), encoded.getLength());
    }

    static ValueEnvelope toEnvelope(final DescriptorBinding binding,
            final Object value, final TextValueCodecRegistry textCodecs) {
        final String text = textCodecs.toText(value, binding.getJavaClass(),
                binding.getDescriptorClassName());
        final ValueEnvelope envelope = new ValueEnvelope();
        if (text != null) {
            envelope.setRepresentation("text");
            envelope.setValue(text);
            return envelope;
        }
        envelope.setRepresentation("base64");
        envelope.setValue(Base64.getEncoder()
                .encodeToString(encode(binding.getDescriptor(), value)));
        return envelope;
    }

    static Object fromEnvelope(final DescriptorBinding binding,
            final ValueEnvelope envelope,
            final TextValueCodecRegistry textCodecs) {
        if ("text".equals(envelope.getRepresentation())) {
            return textCodecs.fromText(envelope.getValue(),
                    binding.getJavaClass(),
                    binding.getDescriptorClassName());
        }
        if ("base64".equals(envelope.getRepresentation())) {
            final byte[] bytes = Base64.getDecoder().decode(envelope.getValue());
            return binding.getDescriptor().getTypeDecoder().decode(bytes);
        }
        throw new IllegalArgumentException(
                "Unsupported envelope representation: "
                        + envelope.getRepresentation());
    }

    static final class DescriptorPair {

        private final DescriptorBinding keyBinding;
        private final DescriptorBinding valueBinding;

        DescriptorPair(final Class<?> keyClass, final Class<?> valueClass,
                final String keyDescriptorClassName,
                final String valueDescriptorClassName,
                final TypeDescriptor<Object> keyDescriptor,
                final TypeDescriptor<Object> valueDescriptor) {
            this.keyBinding = new DescriptorBinding(keyClass,
                    keyDescriptorClassName, keyDescriptor);
            this.valueBinding = new DescriptorBinding(valueClass,
                    valueDescriptorClassName, valueDescriptor);
        }

        DescriptorBinding key() {
            return keyBinding;
        }

        DescriptorBinding value() {
            return valueBinding;
        }
    }

    static final class DescriptorBinding {

        private final Class<?> javaClass;
        private final String descriptorClassName;
        private final TypeDescriptor<Object> descriptor;

        DescriptorBinding(final Class<?> javaClass,
                final String descriptorClassName,
                final TypeDescriptor<Object> descriptor) {
            this.javaClass = javaClass;
            this.descriptorClassName = descriptorClassName;
            this.descriptor = descriptor;
        }

        Class<?> getJavaClass() {
            return javaClass;
        }

        String getDescriptorClassName() {
            return descriptorClassName;
        }

        TypeDescriptor<Object> getDescriptor() {
            return descriptor;
        }
    }
}
