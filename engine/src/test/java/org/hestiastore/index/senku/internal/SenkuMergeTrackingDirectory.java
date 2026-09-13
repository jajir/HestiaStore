package org.hestiastore.index.senku.internal;

import java.util.ArrayList;
import java.util.List;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.directory.FileReaderSeekable;
import org.hestiastore.index.directory.MemDirectory;

/** In-memory merge fixture retaining readers for exact cleanup assertions. */
final class SenkuMergeTrackingDirectory extends MemDirectory {
    final List<FileReaderSeekable> readers = new ArrayList<>();
    IndexException closeFailure;

    @Override
    public FileReaderSeekable getFileReaderSeekable(final String fileName) {
        final FileReaderSeekable reader = new SenkuMergeTrackingReader(
                getFileSequence(fileName), closeFailure);
        readers.add(reader);
        return reader;
    }
}
