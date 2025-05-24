package com.hestiastore.index.sst;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import com.hestiastore.index.IndexException;
import com.hestiastore.index.Vldtn;
import com.hestiastore.index.datatype.TypeDescriptor;
import com.hestiastore.index.datatype.TypeDescriptorInteger;
import com.hestiastore.index.datatype.TypeDescriptorLong;
import com.hestiastore.index.datatype.TypeDescriptorString;

/**
 * Class hold data type definitions in a static way. So for most common types it
 * will simplify index construction
 * 
 * @author honza
 *
 */
public class DataTypeDescriptorRegistry {

    private final static Map<Class<?>, String> descriptors = new HashMap<>();

    static {
        addTypeDescriptor(Integer.class, new TypeDescriptorInteger());
        addTypeDescriptor(Long.class, new TypeDescriptorLong());
        addTypeDescriptor(String.class, new TypeDescriptorString());
    }

    public static final <T> void addTypeDescriptor(final Class<T> clazz,
            final TypeDescriptor<T> typeDescriptor) {
        Objects.requireNonNull(clazz);
        Objects.requireNonNull(typeDescriptor);
        descriptors.put(clazz, typeDescriptor.getClass().getName());
    }

    public static final <T> void addTypeDescriptor(final Class<T> clazz,
            final String typeDescriptor) {
        Objects.requireNonNull(clazz);
        Objects.requireNonNull(typeDescriptor);
        descriptors.put(clazz, typeDescriptor);
    }

    public static final <T> String getTypeDescriptor(final Class<T> clazz) {
        Objects.requireNonNull(clazz);
        final String typeDescriptor = descriptors.get(clazz);
        if (typeDescriptor == null) {
            throw new IllegalStateException(
                    String.format("There is not data type descriptor"
                            + " in registry for class '%s'", clazz));
        }
        return typeDescriptor;

    }

    @SuppressWarnings("unchecked")
    public static <N> TypeDescriptor<N> makeInstance(String className) {
        Vldtn.requireNonNull(className, "className");
        try {
            // Load class by name
            final Class<?> clazz = Class.forName(className);

            // Instantiate using no-argument constructor
            Object instance = clazz.getDeclaredConstructor().newInstance();

            // Verify the created instance
            if (instance instanceof TypeDescriptor) {
                return (TypeDescriptor<N>) instance;
            } else {
                throw new IndexException(String.format(
                        "Class '%s' does not implement TypeDescriptor",
                        className));
            }
        } catch (ClassNotFoundException e) {
            throw new IndexException(String.format(
                    "Unable to find class '%s'. "
                            + "Make sure the class is in the classpath.",
                    className), e);
        } catch (NoSuchMethodException e) {
            throw new IndexException(String.format(
                    "In class '%s' there is no public default (no-args) costructor.",
                    className), e);
        } catch (ReflectiveOperationException e) {
            throw new IndexException(String.format(
                    "Unable to create instance of class '%s'.", className), e);
        }
    }

}
