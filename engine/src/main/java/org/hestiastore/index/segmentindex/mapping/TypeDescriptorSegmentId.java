package org.hestiastore.index.segmentindex.mapping;

import java.util.Comparator;

import org.hestiastore.index.datatype.EncodedBytes;
import org.hestiastore.index.datatype.TypeDecoder;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeEncoder;
import org.hestiastore.index.datatype.TypeReader;
import org.hestiastore.index.datatype.TypeWriter;
import org.hestiastore.index.segment.SegmentId;

/**
 * Class define new type dataType SegmentId. It allows to seamlessly store into
 * different index files.
 * 
 * @author honza
 *
 */
final class TypeDescriptorSegmentId implements TypeDescriptor<SegmentId> {

    private static final TypeDescriptorInteger tdi = new TypeDescriptorInteger();

    /** {@inheritDoc} */
    @Override
    public Comparator<SegmentId> getComparator() {
        return (segId1, segId2) -> segId2.getId() - segId1.getId();
    }

    /** {@inheritDoc} */
    @Override
    public TypeReader<SegmentId> getTypeReader() {
        return fileReader -> {
            final Integer id = tdi.getTypeReader().read(fileReader);
            if (id == null) {
                return null;
            }
            return SegmentId.of(id);
        };
    }

    /** {@inheritDoc} */
    @Override
    public TypeWriter<SegmentId> getTypeWriter() {
        return (writer, object) -> {
            return tdi.getTypeWriter().write(writer, object.getId());
        };
    }

    /** {@inheritDoc} */
    @Override
    public TypeDecoder<SegmentId> getTypeDecoder() {
        return bytes -> SegmentId
                .of(tdi.getTypeDecoder().decode(bytes));
    }

    /** {@inheritDoc} */
    @Override
    public TypeEncoder<SegmentId> getTypeEncoder() {
        return new TypeEncoder<SegmentId>() {
            @Override
            public EncodedBytes encode(final SegmentId segmentId,
                    final byte[] reusableBuffer) {
                return tdi.getTypeEncoder().encode(segmentId.getId(),
                        reusableBuffer);
            }
        };
    }

    /** {@inheritDoc} */
    @Override
    public SegmentId getTombstone() {
        return SegmentId.of(TypeDescriptorInteger.TOMBSTONE_VALUE);
    }

}
