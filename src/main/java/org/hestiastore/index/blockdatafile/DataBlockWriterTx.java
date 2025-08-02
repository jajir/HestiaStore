package org.hestiastore.index.blockdatafile;

import org.hestiastore.index.Commitable;

public class DataBlockWriterTx implements Commitable {

    public DataBlockWriter openWriter() {
        throw new UnsupportedOperationException("Method not implemented yet.");
    }

    @Override
    public void commit() {
        // Implementation for committing the transaction
        throw new UnsupportedOperationException("Method not implemented yet.");
    }

}
