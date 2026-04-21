package org.hestiastore.indextools;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.ServiceLoader;

import org.hestiastore.index.datatype.ByteArray;
import org.hestiastore.index.datatype.NullValue;

final class TextValueCodecRegistry {

    private final List<TextValueCodec> codecs = new ArrayList<>();

    TextValueCodecRegistry() {
        codecs.add(new BuiltInScalarCodec());
        final ServiceLoader<TextValueCodec> loader = ServiceLoader
                .load(TextValueCodec.class);
        for (final TextValueCodec codec : loader) {
            codecs.add(codec);
        }
    }

    String toText(final Object value, final Class<?> javaClass,
            final String descriptorClassName) {
        for (final TextValueCodec codec : codecs) {
            if (codec.supports(javaClass, descriptorClassName)) {
                return codec.toText(value);
            }
        }
        return null;
    }

    Object fromText(final String value, final Class<?> javaClass,
            final String descriptorClassName) {
        for (final TextValueCodec codec : codecs) {
            if (codec.supports(javaClass, descriptorClassName)) {
                return codec.fromText(value, javaClass, descriptorClassName);
            }
        }
        throw new IllegalArgumentException(String.format(
                "No text codec is available for class '%s' and descriptor '%s'.",
                javaClass.getName(), descriptorClassName));
    }

    private static final class BuiltInScalarCodec implements TextValueCodec {

        @Override
        public boolean supports(final Class<?> javaClass,
                final String descriptorClassName) {
            return String.class.equals(javaClass)
                    || Integer.class.equals(javaClass)
                    || Long.class.equals(javaClass)
                    || Float.class.equals(javaClass)
                    || Double.class.equals(javaClass)
                    || Byte.class.equals(javaClass)
                    || ByteArray.class.equals(javaClass)
                    || NullValue.class.equals(javaClass);
        }

        @Override
        public String toText(final Object value) {
            if (value instanceof ByteArray byteArray) {
                return Base64.getEncoder().encodeToString(byteArray.getBytes());
            }
            if (value instanceof NullValue) {
                return "NULL";
            }
            return String.valueOf(value);
        }

        @Override
        public Object fromText(final String value, final Class<?> javaClass,
                final String descriptorClassName) {
            if (String.class.equals(javaClass)) {
                return value;
            }
            if (Integer.class.equals(javaClass)) {
                return Integer.valueOf(value);
            }
            if (Long.class.equals(javaClass)) {
                return Long.valueOf(value);
            }
            if (Float.class.equals(javaClass)) {
                return Float.valueOf(value);
            }
            if (Double.class.equals(javaClass)) {
                return Double.valueOf(value);
            }
            if (Byte.class.equals(javaClass)) {
                return Byte.valueOf(value);
            }
            if (ByteArray.class.equals(javaClass)) {
                return ByteArray.of(Base64.getDecoder().decode(value));
            }
            if (NullValue.class.equals(javaClass)) {
                return NullValue.NULL;
            }
            throw new IllegalArgumentException(
                    "Unsupported built-in text codec target: " + javaClass);
        }
    }
}
