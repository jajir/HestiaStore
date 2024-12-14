package com.coroptis.index.sst;

import java.util.Comparator;

import com.coroptis.index.datatype.ConvertorFromBytes;
import com.coroptis.index.datatype.ConvertorToBytes;
import com.coroptis.index.datatype.TypeDescriptor;
import com.coroptis.index.datatype.TypeDescriptorInteger;
import com.coroptis.index.datatype.TypeReader;
import com.coroptis.index.datatype.TypeWriter;
import com.coroptis.index.segment.SegmentId;

/**
 * Class define new type dataType SegmentId. It allows to seamlessly store into
 * different index files.
 * 
 * @author honza
 *
 */
public class TypeDescriptorSegmentId implements TypeDescriptor<SegmentId> {

    private final static TypeDescriptorInteger tdi = new TypeDescriptorInteger();

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
