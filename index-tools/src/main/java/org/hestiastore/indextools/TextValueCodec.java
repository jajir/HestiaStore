package org.hestiastore.indextools;

/**
 * Optional classpath extension used by the offline CLI to render custom values
 * as human-readable text and to parse them back during JSONL import.
 */
public interface TextValueCodec {

    /**
     * Returns whether this codec supports the supplied Java type and
     * descriptor.
     *
     * @param javaClass java type used by the index configuration
     * @param descriptorClassName fully qualified descriptor class name
     * @return {@code true} when this codec can serialize and parse the value
     */
    boolean supports(Class<?> javaClass, String descriptorClassName);

    /**
     * Converts one logical value into a text representation.
     *
     * @param value logical value
     * @return text form written into JSONL
     */
    String toText(Object value);

    /**
     * Parses one text representation back into a logical value.
     *
     * @param value text form from JSONL
     * @param javaClass java type used by the index configuration
     * @param descriptorClassName fully qualified descriptor class name
     * @return parsed logical value
     */
    Object fromText(String value, Class<?> javaClass, String descriptorClassName);
}
