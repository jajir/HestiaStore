package com.hestiastore.index.directory;

import com.hestiastore.index.CloseableResource;

public interface FileWriter extends CloseableResource {

    void write(byte b);

    void write(byte bytes[]);

}
