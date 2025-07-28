package org.coroptis.index.it;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.hestiastore.index.datatype.NullValue;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.sst.Index;
import org.hestiastore.index.sst.IndexConfiguration;
import org.junit.jupiter.api.Test;

/**
 * Test Void data type
 */
public class IndexSetIT {
    @Test
    void test_create_1() {
        // Create an in-memory file system abstraction
        final Directory directory = new MemDirectory();

        // Prepare index configuration
        final IndexConfiguration<String, NullValue> conf = IndexConfiguration
                .<String, NullValue>builder()//
                .withKeyClass(String.class)//
                .withValueClass(NullValue.class)//
                .withName("test_index") //
                .build();

        // create new index
        Index<String, NullValue> index = Index
                .<String, NullValue>create(directory, conf);

        // Do some work with the index
        index.put("Hello", NullValue.NULL);

        NullValue value = index.get("Hello");
        assertNotNull(value);

        assertNull(index.get("test"));
        System.out.println("Value for 'Hello': " + value);

        index.close();
    }
}
