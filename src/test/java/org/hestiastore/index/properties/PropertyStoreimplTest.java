package org.hestiastore.index.properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.async.AsyncDirectory;
import org.junit.jupiter.api.Test;

class PropertyStoreimplTest {

    @Test
    void constructor_nullDirectory_throws() {
        final IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> new PropertyStoreimpl((AsyncDirectory) null,
                        "file.properties", false));
        assertEquals("Property 'directoryFacade' must not be null.",
                ex.getMessage());
    }

    @Test
    void constructor_nullFileName_throws() {
        final Directory directory = mock(Directory.class);
        final IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> new PropertyStoreimpl(org.hestiastore.index.directory.async.AsyncDirectoryAdapter.wrap(directory), null,
                        false));
        assertEquals("Property 'fileName' must not be null.", ex.getMessage());
    }
}
