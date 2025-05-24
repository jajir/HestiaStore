package com.hestiastore.index.sst;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

import org.junit.jupiter.api.BeforeEach;

import com.hestiastore.index.datatype.TypeDescriptorInteger;
import com.hestiastore.index.datatype.TypeDescriptorString;
import com.hestiastore.index.directory.Directory;
import com.hestiastore.index.directory.MemDirectory;

/**
 * Test insert random key value pairs than randomly change them and finally
 * verify the correctness of data.
 */
public class IntegrationRandomDataTest {

    final Directory directory = new MemDirectory();
    final TypeDescriptorString tds = new TypeDescriptorString();
    final TypeDescriptorInteger tdi = new TypeDescriptorInteger();

    private Index<String, Integer> index;
    private Map<String, Integer> referenceMap;
    private Random random;

    @BeforeEach
    public void setUp() {
        index = makeSstIndex(false);
        referenceMap = new HashMap<>();
        random = new Random();
    }

    private final static int TEST_ITEMS = 10000;
    private final static int TEST_UPDATE_LOOPS = 2;
    private final static int TEST_STRING_LENGTH = 10;

    public void testRandomOperations() {
        // Step 1: Insert random key-value pairs
        for (int i = 0; i < TEST_ITEMS; i++) {
            final String key = generateRandomString();
            final int value = random.nextInt();
            index.put(key, value);
            referenceMap.put(key, value);
        }

        // Step 2: Randomly change some entries
        for (int i = 0; i < TEST_UPDATE_LOOPS; i++) {
            final List<String> keys = referenceMap.keySet().stream()
                    .collect(Collectors.toList());
            for (final String key : keys) {
                if (random.nextInt(10) % 3 == 0) {
                    // key is deleted
                    index.delete(key);
                    referenceMap.remove(key);
                } else if (random.nextInt(10) % 3 == 1) {
                    // new key is inserted
                    final String newKey = generateRandomString();
                    final int newValue = random.nextInt();
                    index.put(newKey, newValue);
                    referenceMap.put(newKey, newValue);
                } else {
                    // key is updated
                    final int newValue = random.nextInt();
                    index.put(key, newValue);
                    referenceMap.put(key, newValue);
                }
            }
        }

        // Step 3: Verify the correctness of the remaining entries
        for (Map.Entry<String, Integer> entry : referenceMap.entrySet()) {
            assertEquals(entry.getValue(), index.get(entry.getKey()));
        }

    }

    private String generateRandomString() {
        final StringBuilder sb = new StringBuilder(TEST_STRING_LENGTH);
        for (int i = 0; i < TEST_STRING_LENGTH; i++) {
            final char c = (char) (random.nextInt(26) + 'a');
            sb.append(c);
        }
        return sb.toString();
    }

    private Index<String, Integer> makeSstIndex(boolean withLog) {
        final IndexConfiguration<String, Integer> conf = IndexConfiguration
                .<String, Integer>builder()//
                .withKeyClass(String.class)//
                .withValueClass(Integer.class)//
                .withKeyTypeDescriptor(tds) //
                .withValueTypeDescriptor(tdi) //
                .withMaxNumberOfKeysInSegment(2) //
                .withMaxNumberOfKeysInSegmentCache(1L) //
                .withMaxNumberOfKeysInSegmentIndexPage(2) //
                .withMaxNumberOfKeysInCache(2) //
                .withBloomFilterIndexSizeInBytes(1000) //
                .withBloomFilterNumberOfHashFunctions(2) //
                .withLogEnabled(withLog) //
                .build();
        return Index.create(directory, conf);
    }
}