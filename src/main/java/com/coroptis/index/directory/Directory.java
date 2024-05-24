package com.coroptis.index.directory;

import java.util.Objects;
import java.util.stream.Stream;

public interface Directory {

    enum Access {
        APPEND, OVERWRITE
    }

    FileReader getFileReader(String fileName);

    FileReaderSeekable getFileReaderSeekable(String fileName);

    /**
     * Opens writer to file. When file already exists than method override it.
     * 
     * @param fileName required file name in this directory
     * @return return {@link FileWriter} or exception is thrown
     */
    default FileWriter getFileWriter(final String fileName) {
        return getFileWriter(
                Objects.requireNonNull(fileName,
                        () -> String.format("File name is null.")),
                Access.OVERWRITE);
    }

    /**
     * It overwrite already existing file or create new one and close it. In all
     * cases result file size is zero.
     * 
     * @param fileName required file name
     */
    default void touch(final String fileName) {
        final FileWriter fileWriter = getFileWriter(fileName);
        fileWriter.close();
    }

    boolean isFileExists(final String fileName);

    FileWriter getFileWriter(String fileName, Access access);

    boolean deleteFile(String fileName);

    Stream<String> getFileNames();

    void renameFile(String currentFileName, String newFileName);

    /**
     * Get file lock. If given file already exist, it means that file is locked
     * and returned object will be in state lock.
     * 
     * @param fileName required lock file names
     * @return file lock object
     */
    FileLock getLock(String fileName);

}
