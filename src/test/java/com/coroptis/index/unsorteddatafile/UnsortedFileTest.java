package com.coroptis.index.unsorteddatafile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.coroptis.index.Pair;
import com.coroptis.index.PairIterator;
import com.coroptis.index.PairWriter;
import com.coroptis.index.datatype.TypeDescriptor;
import com.coroptis.index.datatype.TypeDescriptorInteger;
import com.coroptis.index.datatype.TypeDescriptorString;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.directory.MemDirectory;

public class UnsortedFileTest {

    private final Logger logger = LoggerFactory
            .getLogger(UnsortedFileTest.class);

    private final TypeDescriptor<Integer> tdi = new TypeDescriptorInteger();
    private final TypeDescriptor<String> tds = new TypeDescriptorString();

    @Test
    public void test_in_mem_unsorted_index() throws Exception {
        final Directory dir = new MemDirectory();
        final UnsortedDataFile<Integer, String> unsorted = new UnsortedDataFile<>(
                dir, "duck", tdi.getTypeWriter(), tds.getTypeWriter(),
                tdi.getTypeReader(), tds.getTypeReader());
        assertNotNull(unsorted);

        try (PairWriter<Integer, String> writer = unsorted.openWriter()) {
            writer.put(Pair.of(4, "here"));
            writer.put(Pair.of(-12, "we"));
            writer.put(Pair.of(98, "go"));
        }

        try (PairIterator<Integer, String> reader = unsorted.openIterator()) {
            while (reader.hasNext()) {
                logger.debug(reader.readCurrent().get().toString());
                reader.next();
            }
        }

        try (UnsortedDataFileStreamer<Integer, String> streamer = unsorted
                .openStreamer()) {
            streamer.stream().forEach(pair -> {
                logger.debug(pair.toString());
            });
        }
    }

    @Test
    public void test_stream_non_exesting_file() throws Exception {
        final Directory dir = new MemDirectory();
        final UnsortedDataFile<Integer, String> unsorted = new UnsortedDataFile<>(
                dir, "giraffe", tdi.getTypeWriter(), tds.getTypeWriter(),
                tdi.getTypeReader(), tds.getTypeReader());
        assertNotNull(unsorted);

        try (UnsortedDataFileStreamer<Integer, String> streamer = unsorted
                .openStreamer()) {
            final long count = streamer.stream().count();
            assertEquals(0, count);
        }
    }

}
