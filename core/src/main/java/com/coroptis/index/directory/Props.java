package com.coroptis.index.directory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Objects;
import java.util.Properties;

import com.coroptis.index.IndexException;
import com.coroptis.index.directory.Directory.Access;

public class Props {

    private final Directory directory;
    private final String fileName;
    private final Properties properties = new Properties();

    // TODO make force true by default, remove in in future
    public Props(final Directory directory, final String fileName) {
        this(directory, fileName, false);
    }

    public Props(final Directory directory, final String fileName,
            final boolean force) {
        this.directory = Objects.requireNonNull(directory);
        this.fileName = Objects.requireNonNull(fileName);
        tryToReadProps(force);
    }

    private void tryToReadProps(boolean force) {
        if (!directory.isFileExists(fileName)) {
            if (force) {
                throw new IndexException("File " + fileName
                        + " does not exist in directory " + directory);
            }
            return;
        }
        try {
            final byte buff[] = readEntireFile(directory, fileName);
            properties.load(new ByteArrayInputStream(buff));
        } catch (IOException e) {
            throw new IndexException(e.getMessage(), e);
        }
    }

    public void setString(final String propertyKey, final String value) {
        properties.put(propertyKey, value);
    }

    public String getString(final String propertyKey) {
        return properties.getProperty(propertyKey);
    }

    public void setInt(final String propertyKey, final int value) {
        properties.put(propertyKey, String.valueOf(value));
    }

    public int getInt(final String propertyKey) {
        if (properties.getProperty(propertyKey) == null) {
            return 0;
        }
        return Integer.parseInt(properties.getProperty(propertyKey));
    }

    public void setLong(final String propertyKey, final long value) {
        properties.put(propertyKey, String.valueOf(value));
    }

    public long getLong(final String propertyKey) {
        if (properties.getProperty(propertyKey) == null) {
            return 0L;
        }
        return Long.parseLong(properties.getProperty(propertyKey));
    }

    public void setDouble(final String propertyKey, final double value) {
        properties.put(propertyKey, String.valueOf(value));
    }

    public double getDouble(final String propertyKey) {
        if (properties.getProperty(propertyKey) == null) {
            return 0D;
        }
        return Double.parseDouble(properties.getProperty(propertyKey));
    }

    public void setBoolean(final String propertyKey, final boolean value) {
        properties.put(propertyKey, String.valueOf(value));
    }

    public boolean getBoolean(final String propertyKey) {
        if (properties.getProperty(propertyKey) == null) {
            return false;
        }
        return Boolean.parseBoolean(properties.getProperty(propertyKey));
    }

    public void writeData() {
        final byte buff[] = convertPropsToByteArray();
        try (FileWriter fileWriter = directory.getFileWriter(fileName,
                Access.OVERWRITE)) {
            fileWriter.write(buff);
        }
    }

    private byte[] convertPropsToByteArray() {
        try {
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            properties.store(baos, "Property file");
            return baos.toByteArray();
        } catch (IOException e) {
            throw new IndexException(e.getMessage(), e);
        }
    }

    private byte[] readEntireFile(final Directory directory,
            final String fileName) {
        try (FileReader fileReader = directory.getFileReader(fileName)) {
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            byte buff[] = new byte[128];
            int readedBytes = fileReader.read(buff);
            while (readedBytes != -1) {
                baos.write(buff, 0, readedBytes);
                readedBytes = fileReader.read(buff);
            }
            return baos.toByteArray();
        }
    }

}
