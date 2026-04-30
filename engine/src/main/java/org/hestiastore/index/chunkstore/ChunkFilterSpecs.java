package org.hestiastore.index.chunkstore;

import org.hestiastore.index.Vldtn;

/**
 * Helpers for working with persisted chunk filter specs.
 *
 * <p>
 * This class centralizes the mapping between built-in chunk filter
 * implementations and their stable persisted provider ids. It also contains
 * canonicalization helpers used when reading legacy class-name based metadata.
 * </p>
 */
public final class ChunkFilterSpecs {

    private ChunkFilterSpecs() {
    }

    /**
     * Returns built-in CRC32 spec.
     *
     * @return CRC32 spec
     */
    public static ChunkFilterSpec crc32() {
        return ChunkFilterSpec
                .ofProvider(ChunkFilterProviderResolver.PROVIDER_ID_CRC32);
    }

    /**
     * Returns built-in magic-number spec.
     *
     * @return magic-number spec
     */
    public static ChunkFilterSpec magicNumber() {
        return ChunkFilterSpec.ofProvider(
                ChunkFilterProviderResolver.PROVIDER_ID_MAGIC_NUMBER);
    }

    /**
     * Returns built-in Snappy spec.
     *
     * @return Snappy spec
     */
    public static ChunkFilterSpec snappy() {
        return ChunkFilterSpec
                .ofProvider(ChunkFilterProviderResolver.PROVIDER_ID_SNAPPY);
    }

    /**
     * Returns built-in XOR spec.
     *
     * @return XOR spec
     */
    public static ChunkFilterSpec xor() {
        return ChunkFilterSpec
                .ofProvider(ChunkFilterProviderResolver.PROVIDER_ID_XOR);
    }

    /**
     * Returns built-in do-nothing spec.
     *
     * @return do-nothing spec
     */
    public static ChunkFilterSpec doNothing() {
        return ChunkFilterSpec.ofProvider(
                ChunkFilterProviderResolver.PROVIDER_ID_DO_NOTHING);
    }

    /**
     * Returns reflection-backed class-name spec.
     *
     * @param filterClass filter class
     * @return class-name spec
     */
    public static ChunkFilterSpec javaClass(
            final Class<? extends ChunkFilter> filterClass) {
        final Class<? extends ChunkFilter> requiredClass = Vldtn
                .requireNonNull(filterClass, "filterClass");
        return javaClass(requiredClass.getName());
    }

    /**
     * Returns reflection-backed class-name spec.
     *
     * @param className filter class name
     * @return class-name spec
     */
    public static ChunkFilterSpec javaClass(final String className) {
        return ChunkFilterSpec
                .ofProvider(ChunkFilterProviderResolver.PROVIDER_ID_JAVA_CLASS)
                .withParameter(ChunkFilterProviderResolver.PARAM_CLASS_NAME,
                        Vldtn.requireNotBlank(className, "className"));
    }

    /**
     * Canonicalizes a persisted legacy class name into the matching built-in
     * provider spec when possible. Unknown classes stay on the reflective
     * {@code java-class} provider.
     *
     * @param className persisted filter class name
     * @return canonical filter spec suitable for current provider-based
     *         persistence
     */
    public static ChunkFilterSpec fromPersistedClassName(final String className) {
        final String requiredClassName = Vldtn.requireNotBlank(className,
                "className");
        final ChunkFilterSpec builtInSpec = resolveBuiltInSpecByClassName(
                requiredClassName);
        if (builtInSpec != null) {
            return builtInSpec;
        }
        return javaClass(requiredClassName);
    }

    /**
     * Canonicalizes a spec so logically equivalent built-in filters use the same
     * provider id regardless of whether they originated from legacy class names
     * or the new provider model.
     *
     * @param spec filter spec to canonicalize
     * @return canonicalized spec
     */
    public static ChunkFilterSpec canonicalize(final ChunkFilterSpec spec) {
        final ChunkFilterSpec requiredSpec = Vldtn.requireNonNull(spec, "spec");
        if (!ChunkFilterProviderResolver.PROVIDER_ID_JAVA_CLASS
                .equals(requiredSpec.getProviderId())) {
            return requiredSpec;
        }
        final String className = requiredSpec
                .getParameter(ChunkFilterProviderResolver.PARAM_CLASS_NAME);
        if (className == null || className.isBlank()) {
            return requiredSpec;
        }
        return fromPersistedClassName(className);
    }

    /**
     * Resolves the spec that best represents an encoding filter class.
     *
     * @param filterClass encoding filter class
     * @return persisted filter spec
     */
    public static ChunkFilterSpec forEncodingFilter(
            final Class<? extends ChunkFilter> filterClass) {
        final Class<? extends ChunkFilter> requiredClass = Vldtn
                .requireNonNull(filterClass, "filterClass");
        if (ChunkFilterDoNothing.class.equals(requiredClass)) {
            return doNothing();
        }
        if (ChunkFilterCrc32Writing.class.equals(requiredClass)) {
            return crc32();
        }
        if (ChunkFilterMagicNumberWriting.class.equals(requiredClass)) {
            return magicNumber();
        }
        if (ChunkFilterSnappyCompress.class.equals(requiredClass)) {
            return snappy();
        }
        if (ChunkFilterXorEncrypt.class.equals(requiredClass)) {
            return xor();
        }
        return javaClass(requiredClass);
    }

    /**
     * Resolves the spec that best represents a decoding filter class.
     *
     * @param filterClass decoding filter class
     * @return persisted filter spec
     */
    public static ChunkFilterSpec forDecodingFilter(
            final Class<? extends ChunkFilter> filterClass) {
        final Class<? extends ChunkFilter> requiredClass = Vldtn
                .requireNonNull(filterClass, "filterClass");
        if (ChunkFilterDoNothing.class.equals(requiredClass)) {
            return doNothing();
        }
        if (ChunkFilterCrc32Validation.class.equals(requiredClass)) {
            return crc32();
        }
        if (ChunkFilterMagicNumberValidation.class.equals(requiredClass)) {
            return magicNumber();
        }
        if (ChunkFilterSnappyDecompress.class.equals(requiredClass)) {
            return snappy();
        }
        if (ChunkFilterXorDecrypt.class.equals(requiredClass)) {
            return xor();
        }
        return javaClass(requiredClass);
    }

    /**
     * Resolves the spec that best represents an encoding filter instance.
     *
     * @param filter encoding filter instance
     * @return persisted filter spec, or a reflective {@code java-class} spec
     *         for unknown/custom filters
     */
    public static ChunkFilterSpec forEncodingFilter(final ChunkFilter filter) {
        final ChunkFilter requiredFilter = Vldtn.requireNonNull(filter,
                "filter");
        return forEncodingFilter(requiredFilter.getClass());
    }

    /**
     * Resolves the spec that best represents a decoding filter instance.
     *
     * @param filter decoding filter instance
     * @return persisted filter spec, or a reflective {@code java-class} spec
     *         for unknown/custom filters
     */
    public static ChunkFilterSpec forDecodingFilter(final ChunkFilter filter) {
        final ChunkFilter requiredFilter = Vldtn.requireNonNull(filter,
                "filter");
        return forDecodingFilter(requiredFilter.getClass());
    }

    private static ChunkFilterSpec resolveBuiltInSpecByClassName(
            final String className) {
        if (ChunkFilterDoNothing.class.getName().equals(className)) {
            return doNothing();
        }
        if (ChunkFilterCrc32Writing.class.getName().equals(className)
                || ChunkFilterCrc32Validation.class.getName()
                        .equals(className)) {
            return crc32();
        }
        if (ChunkFilterMagicNumberWriting.class.getName().equals(className)
                || ChunkFilterMagicNumberValidation.class.getName()
                        .equals(className)) {
            return magicNumber();
        }
        if (ChunkFilterSnappyCompress.class.getName().equals(className)
                || ChunkFilterSnappyDecompress.class.getName()
                        .equals(className)) {
            return snappy();
        }
        if (ChunkFilterXorEncrypt.class.getName().equals(className)
                || ChunkFilterXorDecrypt.class.getName().equals(className)) {
            return xor();
        }
        return null;
    }
}
