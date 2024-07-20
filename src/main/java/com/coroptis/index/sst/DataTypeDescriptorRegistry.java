package com.coroptis.index.sst;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import com.coroptis.index.datatype.TypeDescriptor;
import com.coroptis.index.datatype.TypeDescriptorInteger;
import com.coroptis.index.datatype.TypeDescriptorLong;
import com.coroptis.index.datatype.TypeDescriptorString;

/**
 * Class hold data type definitions in a static way. So for most common types it
 * will simplify index construction
 * 
 * @author honza
 *
 */
public class DataTypeDescriptorRegistry {

    private final static Map<Class<?>, TypeDescriptor<?>> descriptors = new HashMap<>();

    static {
        addTypeDescriptor(Integer.class, new TypeDescriptorInteger());
        addTypeDescriptor(Long.class, new TypeDescriptorLong());
        addTypeDescriptor(String.class, new TypeDescriptorString());
    }

    public static final <T> void addTypeDescriptor(final Class<T> clazz,
            final TypeDescriptor<T> typeDescriptor) {
        Objects.requireNonNull(clazz);
        Objects.requireNonNull(typeDescriptor);
        descriptors.put(clazz, typeDescriptor);
    }

    @SuppressWarnings("unchecked")
    public static final <T> Optional<TypeDescriptor<T>> getTypeDescriptorO(
            final Class<T> clazz) {
        Objects.requireNonNull(clazz);
        return Optional.ofNullable((TypeDescriptor<T>) descriptors.get(clazz));
    }

    public static final <T> TypeDescriptor<T> getTypeDescriptor(
            final Class<T> clazz) {
        Objects.requireNonNull(clazz);
        return DataTypeDescriptorRegistry.getTypeDescriptorO(clazz)
                .orElseThrow(() -> new IllegalStateException(String.format(
                        "There is not data type descriptor"
                                + " in registry for class '%s'",
                        clazz)));

    }

}
