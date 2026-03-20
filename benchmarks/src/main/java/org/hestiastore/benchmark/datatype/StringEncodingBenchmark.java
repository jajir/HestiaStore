package org.hestiastore.benchmark.datatype;

import java.util.concurrent.TimeUnit;

import org.hestiastore.index.datatype.EncodedBytes;
import org.hestiastore.index.datatype.TypeDescriptorString;
import org.hestiastore.index.datatype.TypeEncoder;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Measures string key encoding cost for legacy two-pass flow vs single-pass
 * encoding API.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@State(Scope.Thread)
public class StringEncodingBenchmark {

    private static final String ASCII_ALPHABET = "abcdefghijklmnopqrstuvwxyz0123456789";
    private static final String[] MIXED_TOKENS = new String[] { "A", "€", "🙂",
            "M", "á" };

    @Param({ "32", "128", "512", "2048" })
    private int encodedKeyBytes;

    @Param({ "ASCII", "MIXED" })
    private String keyProfile;

    private TypeEncoder<String> singlePassEncoder;

    private LegacyIso88591Encoder legacyEncoder;

    private String key;

    private byte[] reusableBuffer;

    private byte[] legacyReusableBuffer;

    @Setup(Level.Trial)
    public void setup() {
        singlePassEncoder = new TypeDescriptorString().getTypeEncoder();
        legacyEncoder = new LegacyIso88591Encoder();
        key = createKey();
        reusableBuffer = new byte[64];
        legacyReusableBuffer = new byte[64];
    }

    @Benchmark
    public int legacyTwoPass() {
        final int declaredLength = legacyEncoder.bytesLength(key);
        if (legacyReusableBuffer.length < declaredLength) {
            legacyReusableBuffer = new byte[declaredLength];
        }
        final int written = legacyEncoder.toBytes(key, legacyReusableBuffer);
        return written + legacyReusableBuffer[written - 1];
    }

    @Benchmark
    public int singlePassEncode() {
        final EncodedBytes encoded = singlePassEncoder.encode(key,
                reusableBuffer);
        reusableBuffer = encoded.getBytes();
        final int written = encoded.getLength();
        return written + reusableBuffer[written - 1];
    }

    private String createKey() {
        if ("ASCII".equals(keyProfile)) {
            return createAsciiKey(encodedKeyBytes);
        }
        if ("MIXED".equals(keyProfile)) {
            return createMixedKey(encodedKeyBytes);
        }
        throw new IllegalArgumentException(
                "Unsupported key profile '" + keyProfile + "'");
    }

    private String createAsciiKey(final int length) {
        final StringBuilder builder = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            builder.append(ASCII_ALPHABET.charAt(i % ASCII_ALPHABET.length()));
        }
        return builder.toString();
    }

    private String createMixedKey(final int tokensCount) {
        final StringBuilder builder = new StringBuilder(tokensCount * 2);
        for (int i = 0; i < tokensCount; i++) {
            builder.append(MIXED_TOKENS[i % MIXED_TOKENS.length]);
        }
        return builder.toString();
    }

    private static final class LegacyIso88591Encoder {
        private static final byte REPLACEMENT_BYTE = (byte) '?';

        int bytesLength(final String object) {
            int out = 0;
            int index = 0;
            while (index < object.length()) {
                final char c = object.charAt(index);
                if (c <= 0x00FF) {
                    out++;
                    index++;
                    continue;
                }
                out++;
                index += consumesSurrogatePair(object, index, c) ? 2 : 1;
            }
            return out;
        }

        int toBytes(final String object, final byte[] destination) {
            final int required = bytesLength(object);
            if (destination.length < required) {
                throw new IllegalArgumentException(String.format(
                        "Destination buffer too small. Required '%s' but was '%s'",
                        required, destination.length));
            }
            int out = 0;
            int index = 0;
            while (index < object.length()) {
                final char c = object.charAt(index);
                if (c <= 0x00FF) {
                    destination[out++] = (byte) c;
                    index++;
                    continue;
                }
                destination[out++] = REPLACEMENT_BYTE;
                index += consumesSurrogatePair(object, index, c) ? 2 : 1;
            }
            return out;
        }

        private static boolean consumesSurrogatePair(final String object,
                final int index, final char currentChar) {
            return Character.isHighSurrogate(currentChar)
                    && index + 1 < object.length()
                    && Character.isLowSurrogate(object.charAt(index + 1));
        }
    }
}
