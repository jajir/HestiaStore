package org.hestiastore.index.chunkstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.function.Supplier;

import org.junit.jupiter.api.Test;

class ChunkFilterRegistrationTest {

    @Test
    void ofStoresSpecAndSupplier() {
        final ChunkFilterSpec spec = ChunkFilterSpecs.doNothing();
        final Supplier<? extends ChunkFilter> supplier = ChunkFilterDoNothing::new;

        final ChunkFilterRegistration registration = ChunkFilterRegistration
                .of(spec, supplier);

        assertSame(spec, registration.getSpec());
        assertSame(supplier, registration.getSupplier());
        assertEquals(spec.toString(), registration.toString());
    }

    @Test
    void equalsAndHashCodeIncludeSupplierIdentity() {
        final ChunkFilterSpec spec = ChunkFilterSpecs.crc32();
        final Supplier<? extends ChunkFilter> supplier = ChunkFilterDoNothing::new;

        final ChunkFilterRegistration left = ChunkFilterRegistration.of(spec,
                supplier);
        final ChunkFilterRegistration right = ChunkFilterRegistration.of(spec,
                supplier);
        final ChunkFilterRegistration differentSupplier = ChunkFilterRegistration
                .of(spec, ChunkFilterMagicNumberWriting::new);

        assertEquals(left, right);
        assertEquals(left.hashCode(), right.hashCode());
        assertNotEquals(left, differentSupplier);
    }

    @Test
    void ofRejectsNullValues() {
        final Supplier<? extends ChunkFilter> supplier = ChunkFilterDoNothing::new;
        final IllegalArgumentException nullSpec = assertThrows(
                IllegalArgumentException.class,
                () -> ChunkFilterRegistration.of(null, supplier));
        assertEquals("Property 'spec' must not be null.",
                nullSpec.getMessage());

        final IllegalArgumentException nullSupplier = assertThrows(
                IllegalArgumentException.class,
                () -> ChunkFilterRegistration.of(ChunkFilterSpecs.doNothing(),
                        null));
        assertEquals("Property 'supplier' must not be null.",
                nullSupplier.getMessage());
    }
}
