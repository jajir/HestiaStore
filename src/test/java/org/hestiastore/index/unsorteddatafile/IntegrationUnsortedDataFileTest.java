package org.hestiastore.index.unsorteddatafile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.EntryIteratorStreamer;
import org.hestiastore.index.EntryWriter;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.DirectoryFacade;
import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class IntegrationUnsortedDataFileTest {

    private final Logger logger = LoggerFactory
            .getLogger(IntegrationUnsortedDataFileTest.class);

    private final TypeDescriptor<Integer> tdi = new TypeDescriptorInteger();
    private final TypeDescriptor<String> tds = new TypeDescriptorShortString();

    @Test
    void test_in_mem_unsorted_index() {
        final Directory dir = new MemDirectory();
        final UnsortedDataFile<Integer, String> unsorted = UnsortedDataFile
                .<Integer, String>builder()
                .withDirectoryFacade(DirectoryFacade.of(dir))//
                .withFileName("duck")//
                .withKeyWriter(tdi.getTypeWriter())//
                .withKeyReader(tdi.getTypeReader())//
                .withValueWriter(tds.getTypeWriter())//
                .withValueReader(tds.getTypeReader())//
                .build();
        assertNotNull(unsorted);

        final UnsortedDataFileWriterTx<Integer, String> writerTx = unsorted
                .openWriterTx();
        try (EntryWriter<Integer, String> writer = writerTx.open()) {
            writer.write(Entry.of(4, "here"));
            writer.write(Entry.of(-12, "we"));
            writer.write(Entry.of(98, "go"));
        }
        writerTx.commit();

        try (EntryIterator<Integer, String> reader = unsorted.openIterator()) {
            while (reader.hasNext()) {
                final Entry<Integer, String> current = reader.next();
                logger.debug(current.toString());
            }
        }

        try (EntryIteratorStreamer<Integer, String> streamer = unsorted
                .openStreamer()) {
            streamer.stream().forEach(entry -> {
                logger.debug(entry.toString());
            });
        }
    }

    @Test
    void test_stream_non_exesting_file() {
        final Directory dir = new MemDirectory();
        final UnsortedDataFile<Integer, String> unsorted = UnsortedDataFile
                .<Integer, String>builder()
                .withDirectoryFacade(DirectoryFacade.of(dir))//
                .withFileName("giraffe")//
                .withKeyWriter(tdi.getTypeWriter())//
                .withValueWriter(tds.getTypeWriter())//
                .withKeyReader(tdi.getTypeReader())//
                .withValueReader(tds.getTypeReader())//
                .build();
        assertNotNull(unsorted);

        try (EntryIteratorStreamer<Integer, String> streamer = unsorted
                .openStreamer()) {
            final long count = streamer.stream().count();
            assertEquals(0, count);
        }
    }

}
