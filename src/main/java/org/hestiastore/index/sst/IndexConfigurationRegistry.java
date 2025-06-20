package org.hestiastore.index.sst;

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
public class IndexConfigurationRegistry {
    /**
     * memory attribute could be null.
     * 
     * @author honza
     *
     */
    public static class Key {

        private final Class<?> clazz;

        private final String memory;

        public static final Key of(final Class<?> clazz, final String memory) {
            return new Key(clazz, memory);
        }

        private Key(final Class<?> clazz, final String memory) {
            this.clazz = Vldtn.requireNonNull(clazz, "clazz");
            this.memory = memory;
        }

        @Override
        public int hashCode() {
            return Objects.hash(clazz, memory);
        }

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

    private static final Map<Key, IndexConfigurationDefault> confs = new HashMap<>();

    static {
        addTypeDefaultConf(Integer.class,
                new IndexConfigurationDefaultInteger());
        addTypeDefaultConf(Long.class, new IndexConfigurationDefaultInteger());
        addTypeDefaultConf(String.class, new IndexConfigurationDefaultString());
    }

    public static final <T> void addTypeDefaultConf(final Class<T> clazz,
            final IndexConfigurationDefault typeConfiguration) {
        Vldtn.requireNonNull(clazz, "clazz");
        Vldtn.requireNonNull(typeConfiguration, "typeConfiguration");
        add(clazz, null, typeConfiguration);
    }

    public static final <T> void add(final Class<T> clazz, final String memory,
            final IndexConfigurationDefault typeConfiguration) {
        Vldtn.requireNonNull(clazz, "");
        Vldtn.requireNonNull(typeConfiguration, "typeConfiguration");
        confs.put(Key.of(clazz, memory), typeConfiguration);
    }

    public static final <T> Optional<IndexConfigurationDefault> get(
            final Class<T> clazz) {
        return get(clazz, null);
    }

    public static final <T> Optional<IndexConfigurationDefault> get(
            final Class<T> clazz, final String memory) {
        Vldtn.requireNonNull(clazz, "class");
        return Optional.ofNullable(confs.get(Key.of(clazz, memory)));
    }

    private IndexConfigurationRegistry() {
        // Private constructor to prevent instantiation
    }

}
