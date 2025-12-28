package org.hestiastore.index.properties;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.Directory.Access;
import org.hestiastore.index.directory.async.AsyncDirectory;
import org.hestiastore.index.directory.FileReader;
import org.hestiastore.index.directory.FileWriter;
import org.hestiastore.index.directory.async.AsyncFileReaderBlockingAdapter;
import org.hestiastore.index.directory.async.AsyncFileWriterBlockingAdapter;

/**
 * {@link PropertyStore} backed by the {@link Directory} abstraction. It loads
 * all properties into memory and persists changes atomically through explicit
 * transactions.
 */
public final class PropertyStoreimpl implements PropertyStore {

    private final AsyncDirectory directoryFacade;
    private final String fileName;
    private final Properties properties = new Properties();
    private final PropertyConverters converters = new PropertyConverters();

    public PropertyStoreimpl(final AsyncDirectory directoryFacade,
            final String fileName, final boolean force) {
        this.directoryFacade = Vldtn.requireNonNull(directoryFacade,
                "directoryFacade");
        this.fileName = Vldtn.requireNonNull(fileName, "fileName");
        loadIfPresent(force);
    }

    public static PropertyStoreimpl fromAsyncDirectory(
            final AsyncDirectory directoryFacade,
            final String fileName, final boolean force) {
        return new PropertyStoreimpl(directoryFacade, fileName, force);
    }

    private void loadIfPresent(final boolean force) {
        if (!directoryFacade.isFileExistsAsync(fileName)
                .toCompletableFuture().join()) {
            if (force) {
                throw new IndexException("File " + fileName
                        + " does not exist in directory " + directoryFacade);
            }
            return;
        }
        try {
            final byte[] bytes = readEntireFile();
            properties.load(new ByteArrayInputStream(bytes));
        } catch (IOException e) {
            throw new IndexException(e.getMessage(), e);
        }
    }

    private byte[] readEntireFile() {
        try (FileReader reader = new AsyncFileReaderBlockingAdapter(
                directoryFacade.getFileReaderAsync(fileName)
                        .toCompletableFuture().join())) {
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            final byte[] buffer = new byte[256];
            int read = reader.read(buffer);
            while (read != -1) {
                baos.write(buffer, 0, read);
                read = reader.read(buffer);
            }
            return baos.toByteArray();
        }
    }

    @Override
    public PropertyTransaction beginTransaction() {
        synchronized (properties) {
            return new PropertyTransaction(this, properties);
        }
    }

    @Override
    public PropertyView snapshot() {
        synchronized (properties) {
            final Map<String, String> copy = new HashMap<>();
            for (final String key : properties.stringPropertyNames()) {
                copy.put(key, properties.getProperty(key));
            }
            return new PropertyViewImpl(copy, converters);
        }
    }

    void writeToDisk(final Properties propsToWrite) {
        final byte[] bytes = convertToBytes(propsToWrite);
        try (FileWriter writer = new AsyncFileWriterBlockingAdapter(
                directoryFacade.getFileWriterAsync(fileName, Access.OVERWRITE)
                        .toCompletableFuture().join())) {
            writer.write(bytes);
        }
    }

    private byte[] convertToBytes(final Properties propertiesToConvert) {
        try {
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            propertiesToConvert.store(baos, "Property file");
            return baos.toByteArray();
        } catch (IOException e) {
            throw new IndexException(e.getMessage(), e);
        }
    }

}
