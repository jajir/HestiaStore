package com.coroptis.index.sst;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Define contract, that define index configuration.
 * 
 * @author honza
 *
 */
public class BuilderConfigurationRegistry {

    /**
     * memory attribute could be null.
     * 
     * @author honza
     *
     */
    public static class Key {

        private final Class<?> clazz;

        private final String memory;

        public final static Key of(final Class<?> clazz, final String memory) {
            return new Key(clazz, memory);
        }

        private Key(final Class<?> clazz, final String memory) {
            this.clazz = Objects.requireNonNull(clazz);
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

    private final static Map<Key, BuilderConfiguration> confs = new HashMap<>();

    static {
        addTypeDefaultConf(Integer.class, new BuilderConfigurationInteger());
        addTypeDefaultConf(Long.class, new BuilderConfigurationInteger());
    }

    public static final <T> void addTypeDefaultConf(final Class<T> clazz,
            final BuilderConfiguration typeConfiguration) {
        Objects.requireNonNull(clazz);
        Objects.requireNonNull(typeConfiguration);
        add(clazz, null, typeConfiguration);
    }

    public static final <T> void add(final Class<T> clazz, final String memory,
            final BuilderConfiguration typeConfiguration) {
        Objects.requireNonNull(clazz);
        Objects.requireNonNull(typeConfiguration);
        confs.put(Key.of(clazz, memory), typeConfiguration);
    }

    public static final <T> Optional<BuilderConfiguration> get(
            final Class<T> clazz) {
        return get(clazz, null);
    }

    public static final <T> Optional<BuilderConfiguration> get(
            final Class<T> clazz, final String memory) {
        Objects.requireNonNull(clazz, "Class can't be null");
        return Optional.ofNullable(confs.get(Key.of(clazz, memory)));
    }
}
