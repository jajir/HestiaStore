package org.hestiastore.index.datablockfile;

import org.hestiastore.index.AbstractCloseableResource;

/**
 * Data block reader that does not read any data.
 */
public class DataBlockReaderEmpty extends AbstractCloseableResource
        implements DataBlockReader {

    @Override
    public DataBlock read() {
        // It's empty reader, it doesn't read any data.
        return null;
    }

    @Override
    protected void doClose() {
        // Intentionally no-op
    }

}
