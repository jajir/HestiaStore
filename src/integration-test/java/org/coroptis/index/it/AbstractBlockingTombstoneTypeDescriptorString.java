package org.coroptis.index.it;

import java.util.Comparator;

import org.hestiastore.index.datatype.ConvertorFromBytes;
import org.hestiastore.index.datatype.ConvertorToBytes;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.datatype.TypeDescriptorString;
import org.hestiastore.index.datatype.TypeReader;
import org.hestiastore.index.datatype.TypeWriter;

/**
 * Extensions of this abstract class can stop execution of getTombstone() call
 * until released by test code. It allows to verify more complex concurrency
 * scenarios.
 */
abstract class AbstractBlockingTombstoneTypeDescriptorString
        implements TypeDescriptor<String> {

    protected final TypeDescriptorString delegate = new TypeDescriptorString();

    @Override
    public Comparator<String> getComparator() {
        return delegate.getComparator();
    }

    @Override
    public TypeReader<String> getTypeReader() {
        return delegate.getTypeReader();
    }

    @Override
    public TypeWriter<String> getTypeWriter() {
        return delegate.getTypeWriter();
    }

    @Override
    public ConvertorFromBytes<String> getConvertorFromBytes() {
        return delegate.getConvertorFromBytes();
    }

    @Override
    public ConvertorToBytes<String> getConvertorToBytes() {
        return delegate.getConvertorToBytes();
    }
}
