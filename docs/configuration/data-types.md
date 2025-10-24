# Data types

HestiaStore supports a variety of data types for storing keys and values in a binary-efficient and consistent manner. Each data type is associated with a `TypeDescriptor`, which handles serialization, deserialization, comparison, and hashing logic.

Below is a list of the supported data types and their characteristics.

| Java Class           | TypeDescriptor Class              | Max Length (Bytes) | Notes |
|----------------------|-----------------------------------|---------------------|-------|
| `java.lang.Byte`     | `TypeDescriptorByte`              | 1                   | Two's complement representation |
| `java.lang.Integer`  | `TypeDescriptorInteger`           | 4                   | Big-endian encoding |
| `java.lang.Long`     | `TypeDescriptorLong`              | 8                   | Big-endian encoding |
| `java.lang.Float`    | `TypeDescriptorFloat`             | 4                   | IEEE 754 format |
| `java.lang.Double`   | `TypeDescriptorDouble`            | 8                   | IEEE 754 format |
| `java.lang.String`   | `TypeDescriptorShortString`       | 128                 | UTF-8 encoding, prefixed with 1-byte length, it's default string type descriptor |
| `java.lang.String`   | `TypeDescriptorString`            | 2 GB                | UTF-8 encoding, prefixed with 4-byte length |
| `org.hestiastore.index.datatype.ByteArray` | `TypeDescriptorByteArray`   | n                   | Raw bytes, length determined by actual data |
| `org.hestiastore.index.datatype.NullValue` | `TypeDescriptorNullValue`   | 0                   | Usefulll when value is not needed. Doesn't occupy any space. |
| `org.hestiastore.index.datatype.CompositeValue` | `TypeDescriptorCompositeValue`   | n                   | Represents multiple values.  |

## Custom Data Types

HestiaStore allows advanced users to define custom `TypeDescriptor` implementations for handling specialized serialization strategies or complex types. Main usecases:

* Allows to store new data type.
* Introduce some specific encoding. it could lead to space saving.
* Limit data to some exact size

To create a new data type:

1. Implement the `TypeDescriptor<T>` interface. It's just a collection of simple interfaces which allows store data type to bytes and restore it from byte array
2. Optionallly register it using `org.hestiastore.index.sst.DataTypeDescriptorRegistry.addTypeDescriptor(Class, descriptor)`.

### How to use new Data Type

During Index configuration new data type descriptor can by directly used:

```java
IndexConfiguration<Integer, Integer> conf = IndexConfiguration
    .<Integer, Integer>builder()//
    .withKeyClass(Integer.class)//
    .withValueClass(MySuperDataType.class)//
        ...
    .withValueTypeDescriptor(new TypeDescriptorMySuperDataType()) //
        ...
    .build();

Index<Integer, Integer> index = Index.<Integer, Integer>create(directory, conf);
```

## Notes

- All numeric types use big-endian byte order for consistent sorting and comparison.
- `ByteArray` is a wrapper class designed to support `equals()`, `hashCode()`, and lexicographic comparison. It can be used for binary blobs or hash digests.