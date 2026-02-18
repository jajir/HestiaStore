package org.hestiastore.index;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class AbstractCloseableResourceTest {

    private static final class Probe extends AbstractCloseableResource {

        @Override
        protected void doClose() {
        }
    }

    @Test
    void closeMarksResourceAsClosed() {
        Probe probe = new Probe();
        assertFalse(probe.wasClosed());

        probe.close();

        assertTrue(probe.wasClosed());
    }

    @Test
    void secondCloseThrows() {
        Probe probe = new Probe();
        probe.close();

        IllegalStateException ex = assertThrows(IllegalStateException.class,
                probe::close);
        assertTrue(ex.getMessage().contains("already closed"));
    }
}
