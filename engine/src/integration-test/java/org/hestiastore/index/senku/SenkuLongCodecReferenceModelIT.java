package org.hestiastore.index.senku;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import org.hestiastore.index.Entry;
import org.hestiastore.index.chunkentryfile.KeyPageCodec;
import org.hestiastore.index.chunkentryfile.KeyPageCodecs;
import org.hestiastore.index.chunkstore.Compression;
import org.hestiastore.index.datatype.NullValue;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.datatype.TypeDescriptorLong;
import org.hestiastore.index.datatype.TypeDescriptorNull;
import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class SenkuLongCodecReferenceModelIT {

    @ParameterizedTest
    @MethodSource("formats")
    void repeatedKeysMatchMapAcrossFlushesMergesAndReopen(
            final KeyPageCodec<Long> codec, final Compression compression,
            final long[] keys) {
        final var directory = new MemDirectory();
        final var values = new TypeDescriptorLong();
        final var functions = new SenkuMergeFunctionRegistry<Long, Long>();
        functions.register((key, left, right) -> left + right);
        final Map<Long, Long> expected = new TreeMap<>();
        final var writing = builder(directory, values, functions, codec,
                compression).create();
        for (int repeat = 1; repeat <= 3; repeat++) {
            for (int index = keys.length - 1; index >= 0; index--) {
                writing.put(keys[index], (long) repeat);
                expected.merge(keys[index], (long) repeat, Long::sum);
            }
        }
        try (var ready = writing.finishWriting()) {
            assertContents(expected, ready);
        }
        try (var ready = SenkuIndex.open(directory, new TypeDescriptorLong(),
                values, 1024)) {
            assertContents(expected, ready);
        }
    }

    @ParameterizedTest
    @MethodSource("formats")
    void primitiveBatchSliceMatchesSetAcrossFlushesMergesAndReopen(
            final KeyPageCodec<Long> codec, final Compression compression,
            final long[] keys) {
        final var directory = new MemDirectory();
        final var values = new TypeDescriptorNull();
        final var functions = new SenkuMergeFunctionRegistry<Long, NullValue>();
        functions.register(SenkuMergeFunctions.longSet());
        final Map<Long, NullValue> expected = new TreeMap<>();
        final long[] batch = new long[2 * keys.length + 2];
        for (int index = 0; index < keys.length; index++) {
            batch[index + 1] = keys[index];
            batch[keys.length + index + 1] = keys[keys.length - 1 - index];
            expected.put(keys[index], NullValue.NULL);
        }
        // Invalid fixed-weight sentinel keys also verify that the slice is exact.
        batch[0] = Long.MIN_VALUE;
        batch[batch.length - 1] = Long.MAX_VALUE;
        final var writing = builder(directory, values, functions, codec,
                compression).createLongSet(key -> Integer.MIN_VALUE);
        writing.putLongs(batch, 1, batch.length - 2);
        writing.putLongs(batch, batch.length, 0);
        try (var ready = writing.finishWriting()) {
            assertContents(expected, ready);
        }
        try (var ready = SenkuIndex.open(directory, new TypeDescriptorLong(),
                values, 1024)) {
            assertContents(expected, ready);
        }
    }

    private static <V> SenkuIndexBuilder<Long, V> builder(
            final MemDirectory directory, final TypeDescriptor<V> values,
            final SenkuMergeFunctionRegistry<Long, V> functions,
            final KeyPageCodec<Long> codec, final Compression compression) {
        return SenkuIndex.builder(directory, new TypeDescriptorLong(), values,
                functions).keyPageCodec(codec).compression(compression)
                .shardHashFunction(key -> Integer.MIN_VALUE).shardCount(3)
                .maxInMemoryEntries(3).maxKeysPerPage(2).maxEntriesPerPart(3)
                .mergeFanIn(2).maintenanceThreads(2).maintenanceQueueSize(1)
                .diskIoBufferSize(1024);
    }

    private static <V> void assertContents(final Map<Long, V> expected,
            final SenkuReady<Long, V> ready) {
        final List<Entry<Long, V>> entries = expected.entrySet().stream()
                .map(entry -> Entry.of(entry.getKey(), entry.getValue()))
                .toList();
        assertEquals(expected.size(), ready.recordCount());
        assertEquals(expected.size(),
                ready.longKeySummary().orElseThrow().recordCount());
        try (var stream = ready.openStream()) {
            assertEquals(entries, stream.toList());
        }
    }

    private static Stream<Arguments> formats() {
        final long[] signedKeys = { Long.MIN_VALUE, Long.MIN_VALUE + 1, -129,
                -128, -1, 0, 1, 127, 128, Long.MAX_VALUE - 1, Long.MAX_VALUE };
        final long[] rankedKeys = LongStream.range(0, 256)
                .filter(key -> Long.bitCount(key) == 4).toArray();
        return Stream.of(Compression.none(), Compression.zstd(3))
                .flatMap(compression -> Stream.of(
                        Arguments.of(KeyPageCodecs.<Long>prefix(), compression,
                                signedKeys),
                        Arguments.of(KeyPageCodecs.longDeltaVarint(),
                                compression, signedKeys),
                        Arguments.of(KeyPageCodecs.longFixedWeightDeltaVarint(
                                8, 4, new long[0], 0), compression, rankedKeys)));
    }
}
