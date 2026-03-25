package org.hestiastore.index.chunkstore;

import java.util.function.Supplier;

/**
 * Creates encoding and decoding chunk filter suppliers for a logical filter
 * family identified by a stable provider id.
 *
 * <p>
 * <strong>Why {@code Supplier&lt;ChunkFilter&gt;} is not enough</strong>
 * </p>
 *
 * <p>
 * Java generics are invariant:
 * </p>
 * <ul>
 * <li>{@code ChunkFilterCrc32Writing} is a subtype of {@link ChunkFilter}</li>
 * <li>{@code Supplier<ChunkFilterCrc32Writing>} is <em>not</em> a subtype of
 * {@code Supplier<ChunkFilter>}</li>
 * </ul>
 *
 * <pre>
 * {@code
 * Supplier<ChunkFilterCrc32Writing> supplier = ChunkFilterCrc32Writing::new;
 * }
 * </pre>
 *
 * <p>
 * This cannot be passed to a method declared as:
 * </p>
 *
 * <pre>
 * {@code
 * void use(Supplier<ChunkFilter> supplier)
 * }
 * </pre>
 *
 * <p>
 * but it can be passed to:
 * </p>
 *
 * <pre>
 * {@code
 * void use(Supplier<? extends ChunkFilter> supplier)
 * }
 * </pre>
 *
 * <p>
 * That is why this API uses {@code Supplier<? extends ChunkFilter>} at its
 * boundaries. HestiaStore only calls {@link Supplier#get()} and consumes the
 * returned value as a {@link ChunkFilter}, so the wildcard accurately models a
 * producer of concrete filter implementations.
 * </p>
 */
public interface ChunkFilterProvider {

    /**
     * Returns the stable provider id used in persisted metadata.
     *
     * @return provider id
     */
    String getProviderId();

    /**
     * Creates the runtime supplier for the encoding side of the filter.
     *
     * @param spec persisted filter spec
     * @return encoding supplier
     */
    Supplier<? extends ChunkFilter> createEncodingSupplier(ChunkFilterSpec spec);

    /**
     * Creates the runtime supplier for the decoding side of the filter.
     *
     * @param spec persisted filter spec
     * @return decoding supplier
     */
    Supplier<? extends ChunkFilter> createDecodingSupplier(ChunkFilterSpec spec);
}
