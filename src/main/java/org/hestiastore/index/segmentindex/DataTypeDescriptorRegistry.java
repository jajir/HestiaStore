package org.hestiastore.index.segmentindex;

import java.util.HashMap;
import java.util.Map;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.ByteArray;
import org.hestiastore.index.datatype.NullValue;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.datatype.TypeDescriptorByteArray;
import org.hestiastore.index.datatype.TypeDescriptorDouble;
import org.hestiastore.index.datatype.TypeDescriptorFloat;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorLong;
import org.hestiastore.index.datatype.TypeDescriptorNull;
import org.hestiastore.index.datatype.TypeDescriptorShortString;

/**
 * Class hold data type definitions in a static way. So for most common types it
 * will simplify index construction
 * 
 * @author honza
 *
 */
public class DataTypeDescriptorRegistry {

    private static final String CLAZZ = "clazz";
    private static final Map<Class<?>, String> descriptors = new HashMap<>();

    static {
        addTypeDescriptor(Integer.class, new TypeDescriptorInteger());
        addTypeDescriptor(Long.class, new TypeDescriptorLong());
        addTypeDescriptor(String.class, new TypeDescriptorShortString());
        addTypeDescriptor(Float.class, new TypeDescriptorFloat());
        addTypeDescriptor(Double.class, new TypeDescriptorDouble());
        addTypeDescriptor(ByteArray.class, new TypeDescriptorByteArray());
        addTypeDescriptor(NullValue.class, new TypeDescriptorNull());
    }

    /**
     * Registers a type descriptor instance for the provided class.
     *
     * @param <T>            type being registered
     * @param clazz          class that maps to the descriptor
     * @param typeDescriptor descriptor instance
     */
    public static final <T> void addTypeDescriptor(final Class<T> clazz,
            final TypeDescriptor<T> typeDescriptor) {
        Vldtn.requireNonNull(clazz, CLAZZ);
        Vldtn.requireNonNull(typeDescriptor, "typeDescriptor");
        descriptors.put(clazz, typeDescriptor.getClass().getName());
    }

    /**
     * Registers a type descriptor class name for the provided class.
     *
     * @param <T>            type being registered
     * @param clazz          class that maps to the descriptor
     * @param typeDescriptor descriptor class name
     */
    public static final <T> void addTypeDescriptor(final Class<T> clazz,
            final String typeDescriptor) {
        Vldtn.requireNonNull(clazz, CLAZZ);
        Vldtn.requireNonNull(typeDescriptor, "typeDescriptor");
        descriptors.put(clazz, typeDescriptor);
    }

    /**
     * Returns the registered descriptor class name for the provided class.
     *
     * @param <T>   type being looked up
     * @param clazz class that maps to the descriptor
     * @return descriptor class name
     */
    public static final <T> String getTypeDescriptor(final Class<T> clazz) {
        Vldtn.requireNonNull(clazz, CLAZZ);
        final String typeDescriptor = descriptors.get(clazz);
        if (typeDescriptor == null) {
            throw new IllegalStateException(
                    String.format("There is not data type descriptor"
                            + " in registry for class '%s'", clazz));
        }
        return typeDescriptor;

    }

    /**
     * Creates a descriptor instance from the provided class name.
     *
     * @param <N>       type represented by the descriptor
     * @param className descriptor class name
     * @return instantiated type descriptor
     */
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

    private DataTypeDescriptorRegistry() {
        // Prevent instantiation
    }

}
