package org.hestiastore.index.directory;

public interface FileLock {

    boolean isLocked();

    void lock();

    void unlock();

}
