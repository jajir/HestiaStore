package com.coroptis.index.log;

import java.util.Objects;

import com.coroptis.index.CloseablePairReader;
import com.coroptis.index.datatype.TypeDescriptor;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.unsorteddatafile.UnsortedDataFile;

public class LogFilesManager<K, V> {

    private final Directory directory;
    private final TypeDescriptor<LoggedKey<K>> keyTypeDescriptor;
    private final TypeDescriptor<V> valueTypeDescriptor;

    LogFilesManager(final Directory directory,
            final TypeDescriptor<LoggedKey<K>> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor) {
        this.directory = Objects.requireNonNull(directory);
        this.keyTypeDescriptor = Objects.requireNonNull(keyTypeDescriptor);
        this.valueTypeDescriptor = Objects.requireNonNull(valueTypeDescriptor);
    }

    UnsortedDataFile<LoggedKey<K>, V> getLogFile(final String name) {
        final UnsortedDataFile<LoggedKey<K>, V> out = UnsortedDataFile
                .<LoggedKey<K>, V>builder()//
                .withDirectory(directory)//
                .withFileName(name)//
                .withKeyWriter(keyTypeDescriptor.getTypeWriter())//
                .withKeyReader(keyTypeDescriptor.getTypeReader())//
                .withValueWriter(valueTypeDescriptor.getTypeWriter())//
                .withValueReader(valueTypeDescriptor.getTypeReader())//
                .build();
        return out;
    }

    CloseablePairReader<LoggedKey<K>, V> openReader(final String name) {
        UnsortedDataFile<LoggedKey<K>, V> log = getLogFile(name);
        return log.openReader();
    }

}
