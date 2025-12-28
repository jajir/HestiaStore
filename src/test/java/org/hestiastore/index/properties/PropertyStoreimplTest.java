package org.hestiastore.index.properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.DirectoryFacade;
import org.junit.jupiter.api.Test;

class PropertyStoreimplTest {

    @Test
    void constructor_nullDirectory_throws() {
        final IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> new PropertyStoreimpl((DirectoryFacade) null,
                        "file.properties", false));
        assertEquals("Property 'directoryFacade' must not be null.",
                ex.getMessage());
    }

    @Test
    void constructor_nullFileName_throws() {
        final Directory directory = mock(Directory.class);
        final IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> new PropertyStoreimpl(DirectoryFacade.of(directory), null,
                        false));
        assertEquals("Property 'fileName' must not be null.", ex.getMessage());
    }
}
