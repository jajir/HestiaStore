package org.hestiastore.index.directory;

import java.io.File;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;

/**
 * Abstract implementation of {@link Directory} interface.
 * 
 * This abstract class add some java.io.File suport methods.
 */
public abstract class AbstractDirectory implements Directory {

    protected void assureThatFileExists(final File file) {
        Vldtn.requireNonNull(file, "file");
        if (!file.exists()) {
            throw new IndexException(String.format("File '%s' doesn't exists.",
                    file.getAbsolutePath()));
        }
    }

}
