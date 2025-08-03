package org.hestiastore.index.chunkstore;

public class Chunk {

    static final int HEADER_SIZE = 32;

    /**
     * Size of cell in bytes. Cell is smalles addresable unit in chunk store.
     */
    static final int CELL_SIZE = 16;

    /**
     * "theodora" in ASCII
     */
    static final long MAGIC_NUMBER = 0x7468656F646F7261L;

    /**
     * Version of the chunk format. It is used to identify the format of the
     * chunk and ensure compatibility between different versions of the chunk
     * format.
     * 
     * real version is remotely related to main library version when it's
     * introduced.
     */
    static final int VERSION = 0xff_00_00_05;

}
