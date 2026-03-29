package org.hestiastore.index.segmentindex.wal;

import java.io.File;
import java.lang.reflect.Field;
import java.nio.file.Path;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;

/**
 * Creates storage adapters for WAL runtime.
 */
final class WalStorageFactory {

    private WalStorageFactory() {
    }

    static WalStorage create(final Directory walDirectory) {
        final Directory directory = Vldtn.requireNonNull(walDirectory,
                "walDirectory");
        if (directory instanceof MemDirectory memDirectory) {
            return new WalStorageMem(memDirectory);
        }
        final Path fsPath = tryResolveFsPath(directory);
        if (fsPath != null) {
            return new WalStorageFs(fsPath);
        }
        return new WalStorageDirectory(directory);
    }

    private static Path tryResolveFsPath(final Directory directory) {
        Class<?> type = directory.getClass();
        while (type != null) {
            try {
                final Field directoryField = type.getDeclaredField("directory");
                directoryField.setAccessible(true);
                final Object value = directoryField.get(directory);
                if (value instanceof File file) {
                    return file.toPath();
                }
            } catch (final NoSuchFieldException ex) {
                type = type.getSuperclass();
                continue;
            } catch (final ReflectiveOperationException ex) {
                return null;
            }
            type = type.getSuperclass();
        }
        return null;
    }
}
