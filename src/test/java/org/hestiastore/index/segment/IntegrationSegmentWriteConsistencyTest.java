package org.hestiastore.index.segment;

import static org.hestiastore.index.segment.AbstractSegmentTest.verifySegmentData;
import static org.hestiastore.index.AbstractDataTest.verifyNumberOfFiles;

import java.util.List;
import java.util.stream.IntStream;

import org.hestiastore.index.Entry;
import org.hestiastore.index.chunkstore.ChunkFilterCrc32Validation;
import org.hestiastore.index.chunkstore.ChunkFilterCrc32Writing;
import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.chunkstore.ChunkFilterMagicNumberValidation;
import org.hestiastore.index.chunkstore.ChunkFilterMagicNumberWriting;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class IntegrationSegmentWriteConsistencyTest {

    private static final TypeDescriptorShortString TDS = new TypeDescriptorShortString();
    private static final TypeDescriptorInteger TDI = new TypeDescriptorInteger();
    private static final SegmentId SEGMENT_ID_1 = SegmentId.of(27);
    private final List<String> values = List.of("aaa", "bbb", "ccc", "ddd",
            "eee", "fff");
    private final List<Entry<Integer, String>> data = IntStream
            .range(0, values.size() - 1)
            .mapToObj(i -> Entry.of(i, values.get(i))).toList();
    private final List<Entry<Integer, String>> updatedData = IntStream
            .range(0, values.size() - 1)
            .mapToObj(i -> Entry.of(i, values.get(i + 1))).toList();

    private Directory directory;
    private Segment<Integer, String> segment;

    @BeforeEach
    void setup() {
        directory = new MemDirectory();
        segment = makeSegment(directory, SEGMENT_ID_1);
    }

    @AfterEach
    void tearDown() {
        segment = null;
        directory = null;
    }

    @Test
    void write_then_flush_reads_back() {
        data.forEach(entry -> segment.put(entry.getKey(), entry.getValue()));
        segment.flush();

        verifySegmentData(segment, data);
        verifyNumberOfFiles(directory, 2);
    }

    @Test
    void write_without_flush_reads_from_cache() {
        data.forEach(entry -> segment.put(entry.getKey(), entry.getValue()));

        verifySegmentData(segment, data);
        // no data are flushed to disk
        verifyNumberOfFiles(directory, 0);
    }

    @Test
    void write_close_reopen_reads_back() {
        data.forEach(entry -> segment.put(entry.getKey(), entry.getValue()));
        segment.flush();
        segment.close();

        final Segment<Integer, String> reopened = makeSegment(directory,
                SEGMENT_ID_1);
        verifySegmentData(reopened, data);
        reopened.close();
        verifyNumberOfFiles(directory, 2);
    }

    @Test
    void test_writing_updated_values() {
        data.forEach(entry -> segment.put(entry.getKey(), entry.getValue()));
        segment.flush();
        verifySegmentData(segment, data);
        segment.close();

        final Segment<Integer, String> segment2 = makeSegment(directory,
                SEGMENT_ID_1);
        updatedData.forEach(
                entry -> segment2.put(entry.getKey(), entry.getValue()));
        segment2.flush();
        verifySegmentData(segment2, updatedData);
        segment2.close();
        verifyNumberOfFiles(directory, 3);
    }

    private Segment<Integer, String> makeSegment(final Directory directory,
            final SegmentId id) {
        return Segment.<Integer, String>builder().withAsyncDirectory(
                org.hestiastore.index.directory.async.AsyncDirectoryAdapter
                        .wrap(directory))//
                .withId(id)//
                .withKeyTypeDescriptor(TDI)//
                .withValueTypeDescriptor(TDS)//
                .withMaxNumberOfKeysInSegmentChunk(2)//
                .withBloomFilterIndexSizeInBytes(0)//
                .withEncodingChunkFilters(//
                        List.of(new ChunkFilterMagicNumberWriting(), //
                                new ChunkFilterCrc32Writing(), //
                                new ChunkFilterDoNothing()//
                        ))//
                .withDecodingChunkFilters(//
                        List.of(new ChunkFilterMagicNumberValidation(), //
                                new ChunkFilterCrc32Validation(), //
                                new ChunkFilterDoNothing()//
                        ))//
                .build();
    }

}
