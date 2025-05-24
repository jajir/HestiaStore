package com.hestiastore.index.unsorteddatafile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hestiastore.index.Pair;
import com.hestiastore.index.PairIterator;
import com.hestiastore.index.PairWriter;
import com.hestiastore.index.datatype.TypeDescriptor;
import com.hestiastore.index.datatype.TypeDescriptorInteger;
import com.hestiastore.index.datatype.TypeDescriptorString;
import com.hestiastore.index.directory.Directory;
import com.hestiastore.index.directory.MemDirectory;

public class IntegrationUnsortedDataFileTest {

    private final Logger logger = LoggerFactory
            .getLogger(IntegrationUnsortedDataFileTest.class);

    private final TypeDescriptor<Integer> tdi = new TypeDescriptorInteger();
    private final TypeDescriptor<String> tds = new TypeDescriptorString();

    @Test
    public void test_in_mem_unsorted_index() throws Exception {
        final Directory dir = new MemDirectory();
        final UnsortedDataFile<Integer, String> unsorted = UnsortedDataFile
                .<Integer, String>builder().withDirectory(dir)//
                .withFileName("duck")//
                .withKeyWriter(tdi.getTypeWriter())//
                .withKeyReader(tdi.getTypeReader())//
                .withValueWriter(tds.getTypeWriter())//
                .withValueReader(tds.getTypeReader())//
                .build();
        assertNotNull(unsorted);

        try (PairWriter<Integer, String> writer = unsorted.openWriter()) {
            writer.put(Pair.of(4, "here"));
            writer.put(Pair.of(-12, "we"));
            writer.put(Pair.of(98, "go"));
        }

        try (PairIterator<Integer, String> reader = unsorted.openIterator()) {
            while (reader.hasNext()) {
                final Pair<Integer, String> current = reader.next();
                logger.debug(current.toString());
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
        final UnsortedDataFile<Integer, String> unsorted = UnsortedDataFile
                .<Integer, String>builder().withDirectory(dir)//
                .withFileName("giraffe")//
                .withKeyWriter(tdi.getTypeWriter())//
                .withValueWriter(tds.getTypeWriter())//
                .withKeyReader(tdi.getTypeReader())//
                .withValueReader(tds.getTypeReader())//
                .build();
        assertNotNull(unsorted);

        try (UnsortedDataFileStreamer<Integer, String> streamer = unsorted
                .openStreamer()) {
            final long count = streamer.stream().count();
            assertEquals(0, count);
        }
    }

}
