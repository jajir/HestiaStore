package org.hestiastore.index.segmentindex;

import java.util.Comparator;

import org.hestiastore.index.datatype.ConvertorFromBytes;
import org.hestiastore.index.datatype.ConvertorToBytes;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
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
public class TypeDescriptorSegmentId implements TypeDescriptor<SegmentId> {

    private static final TypeDescriptorInteger tdi = new TypeDescriptorInteger();

    @Override
    public Comparator<SegmentId> getComparator() {
        return (segId1, segId2) -> segId2.getId() - segId1.getId();
    }

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

    @Override
    public TypeWriter<SegmentId> getTypeWriter() {
        return (writer, object) -> {
            return tdi.getTypeWriter().write(writer, object.getId());
        };
    }

    @Override
    public ConvertorFromBytes<SegmentId> getConvertorFromBytes() {
        return bytes -> SegmentId
                .of(tdi.getConvertorFromBytes().fromBytes(bytes));
    }

    @Override
    public ConvertorToBytes<SegmentId> getConvertorToBytes() {
        return segmentId -> tdi.getConvertorToBytes()
                .toBytes(segmentId.getId());
    }

    @Override
    public SegmentId getTombstone() {
        return SegmentId.of(TypeDescriptorInteger.TOMBSTONE_VALUE);
    }

}
