package com.coroptis.index.sst;

import java.util.Objects;

import com.coroptis.index.directory.Directory;
import com.coroptis.index.directory.Props;

public class IndexConfiguratonStorage {

    private final static String CONFIGURATION_FILENAME = "index-configuration.properties";

    private final Directory directory;

    IndexConfiguratonStorage(final Directory directory) {
        this.directory = Objects.requireNonNull(directory,
                "Directory cannot be null");
    }

    IndexConfiguration load() {
        final Props props = new Props(directory, CONFIGURATION_FILENAME);

        return null;
    }

}
