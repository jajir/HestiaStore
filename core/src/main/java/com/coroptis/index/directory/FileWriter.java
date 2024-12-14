package com.coroptis.index.directory;

import com.coroptis.index.CloseableResource;

public interface FileWriter extends CloseableResource {

    void write(byte b);

    void write(byte bytes[]);

}
