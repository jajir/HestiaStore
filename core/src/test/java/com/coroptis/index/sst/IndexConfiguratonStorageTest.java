package com.coroptis.index.sst;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.coroptis.index.directory.Directory;
import com.coroptis.index.directory.MemDirectory;

public class IndexConfiguratonStorageTest {

    private Directory directory;

    private IndexConfiguratonStorage<String, Long> storage;

    @Test
    void test_save_and_load() {
    }

    @BeforeEach
    void setup() {
        directory = new MemDirectory();
        storage = new IndexConfiguratonStorage<>(directory);
    }

    void tearDown() {
        storage = null;
        directory = null;
    }

}
