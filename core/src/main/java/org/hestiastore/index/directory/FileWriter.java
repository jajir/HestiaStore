package org.hestiastore.index.directory;

import org.hestiastore.index.CloseableResource;

public interface FileWriter extends CloseableResource {

    void write(byte b);

    void write(byte bytes[]);

}
