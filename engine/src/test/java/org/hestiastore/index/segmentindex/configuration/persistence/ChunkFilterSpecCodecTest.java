package org.hestiastore.index.segmentindex.configuration.persistence;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.hestiastore.index.chunkstore.ChunkFilterMagicNumberWriting;
import org.hestiastore.index.chunkstore.ChunkFilterSpec;
import org.hestiastore.index.chunkstore.ChunkFilterSpecs;
import org.junit.jupiter.api.Test;

class ChunkFilterSpecCodecTest {

    @Test
    void serializeAndParseRoundTripEscapesReservedCharacters() {
        final List<ChunkFilterSpec> specs = List.of(
                ChunkFilterSpec.ofProvider("crypto/provider")
                        .withParameter("key|ref", "orders,main=value")
                        .withParameter("region", "eu west"),
                ChunkFilterSpecs.doNothing());

        final String serialized = ChunkFilterSpecCodec.serialize(specs);
        final List<ChunkFilterSpec> parsed = ChunkFilterSpecCodec
                .parse(serialized);

        assertTrue(serialized.contains("p="));
        assertEquals(specs, parsed);
    }

    @Test
    void parseReturnsEmptyListForBlankValues() {
        assertEquals(List.of(), ChunkFilterSpecCodec.parse(null));
        assertEquals(List.of(), ChunkFilterSpecCodec.parse(""));
        assertEquals(List.of(), ChunkFilterSpecCodec.parse("   "));
        assertEquals("", ChunkFilterSpecCodec.serialize(List.of()));
    }

    @Test
    void parseSupportsLegacyBuiltInAndUnknownClassNameTokens() {
        final List<ChunkFilterSpec> parsed = ChunkFilterSpecCodec.parse(
                ChunkFilterMagicNumberWriting.class.getName() + ","
                        + LegacyCustomChunkFilter.class.getName());

        assertEquals(
                List.of(ChunkFilterSpecs.magicNumber(),
                        ChunkFilterSpecs
                                .javaClass(LegacyCustomChunkFilter.class
                                        .getName())),
                parsed);
    }

    @Test
    void parseRejectsMalformedParameterTokens() {
        final IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> ChunkFilterSpecCodec.parse("p=test|broken"));

        assertEquals("Invalid chunk filter parameter token 'p=test|broken'",
                exception.getMessage());
    }
}
