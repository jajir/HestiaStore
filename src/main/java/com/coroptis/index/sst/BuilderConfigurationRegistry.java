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

    private final static Map<Class<?>, BuilderConfiguration> confs = new HashMap<>();

    static {
        addTypeConf(Integer.class, new BuilderConfigurationInteger());
        addTypeConf(Long.class, new BuilderConfigurationInteger());
    }

    public static final <T> void addTypeConf(final Class<T> clazz,
            final BuilderConfiguration typeConfiguration) {
        Objects.requireNonNull(clazz);
        Objects.requireNonNull(typeConfiguration);
        confs.put(clazz, typeConfiguration);
    }

    public static final <T> Optional<BuilderConfiguration> getTypeCofiguration(
            final Class<T> clazz) {
        Objects.requireNonNull(clazz);
        return Optional.ofNullable(confs.get(clazz));
    }
}
