package org.hestiastore.index.segmentindex;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.hestiastore.index.Vldtn;

/**
 * Define contract, that define index configuration.
 * 
 * @author honza
 *
 */
class IndexConfigurationRegistry {
    /**
     * memory attribute could be null.
     * 
     * @author honza
     *
     */
    static final class Key {

        private final Class<?> clazz;

        private final String memory;

        /**
         * Creates a registry key for the provided class and memory identifier.
         *
         * @param clazz  class used as the key
         * @param memory optional memory descriptor
         * @return registry key
         */
        static Key of(final Class<?> clazz, final String memory) {
            return new Key(clazz, memory);
        }

        private Key(final Class<?> clazz, final String memory) {
            this.clazz = Vldtn.requireNonNull(clazz, "clazz");
            this.memory = memory;
        }

        /** {@inheritDoc} */
        @Override
        public int hashCode() {
            return Objects.hash(clazz, memory);
        }

        /** {@inheritDoc} */
        @Override
        public boolean equals(final Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            final Key other = (Key) obj;
            return Objects.equals(clazz, other.clazz)
                    && Objects.equals(memory, other.memory);
        }

    }

    private static final Map<Key, IndexConfigurationContract> confs = new HashMap<>();

    static {
        addTypeDefaultConf(Integer.class,
                new IndexConfigurationDefaultInteger());
        addTypeDefaultConf(Long.class, new IndexConfigurationDefaultInteger());
        addTypeDefaultConf(String.class, new IndexConfigurationDefaultString());
    }

    /**
     * Registers a default configuration for the provided class.
     *
     * @param <T>               type handled by the defaults
     * @param clazz             class to associate with defaults
     * @param typeConfiguration default configuration
     */
    static <T> void addTypeDefaultConf(final Class<T> clazz,
            final IndexConfigurationContract typeConfiguration) {
        Vldtn.requireNonNull(clazz, "clazz");
        Vldtn.requireNonNull(typeConfiguration, "typeConfiguration");
        add(clazz, null, typeConfiguration);
    }

    /**
     * Registers a configuration for the provided class and memory descriptor.
     *
     * @param <T>               type handled by the configuration
     * @param clazz             class to associate with the configuration
     * @param memory            optional memory descriptor
     * @param typeConfiguration configuration to register
     */
    static <T> void add(final Class<T> clazz, final String memory,
            final IndexConfigurationContract typeConfiguration) {
        Vldtn.requireNonNull(clazz, "");
        Vldtn.requireNonNull(typeConfiguration, "typeConfiguration");
        confs.put(Key.of(clazz, memory), typeConfiguration);
    }

    /**
     * Returns the registered configuration for the provided class.
     *
     * @param <T>   type handled by the configuration
     * @param clazz class to look up
     * @return optional configuration for the class
     */
    static <T> Optional<IndexConfigurationContract> get(
            final Class<T> clazz) {
        return get(clazz, null);
    }

    /**
     * Returns the registered configuration for the provided class and memory
     * descriptor.
     *
     * @param <T>   type handled by the configuration
     * @param clazz class to look up
     * @param memory optional memory descriptor
     * @return optional configuration for the key
     */
    static <T> Optional<IndexConfigurationContract> get(
            final Class<T> clazz, final String memory) {
        Vldtn.requireNonNull(clazz, "class");
        return Optional.ofNullable(confs.get(Key.of(clazz, memory)));
    }

    private IndexConfigurationRegistry() {
        // Private constructor to prevent instantiation
    }

}
