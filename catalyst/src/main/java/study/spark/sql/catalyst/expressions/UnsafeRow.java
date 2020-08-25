package study.spark.sql.catalyst.expressions;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import study.spark.unsafe.Platform;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import static study.spark.unsafe.Platform.BYTE_ARRAY_OFFSET;

/**
 * An Unsafe implementation of Row which is backed by raw memory instead of Java objects.
 *
 * Each tuple has three parts: [null bit set] [values] [variable length portion]
 *
 * The bit set is used for null tracking and is aligned to 8-byte word boundaries.  It stores
 * one bit per field.
 *
 * In the `values` region, we store one 8-byte word per field. For fields that hold fixed-length
 * primitive types, such as long, double, or int, we store the value directly in the word. For
 * fields with non-primitive or variable-length values, we store a relative offset (w.r.t. the
 * base address of the row) that points to the beginning of the variable-length field, and length
 * (they are combined into a long).
 *
 * Instances of `UnsafeRow` act as pointers to row data stored in this format.
 */
public final class UnsafeRow extends MutableRow implements Externalizable, KryoSerializable {

    //////////////////////////////////////////////////////////////////////////////
    // Static methods
    //////////////////////////////////////////////////////////////////////////////

    public static int calculateBitSetWidthInBytes(int numFields) {
        return ((numFields + 63)/ 64) * 8;
    }


    private Object baseObject;
    private long baseOffset;

    /** The number of fields in this row, used for calculating the bitset width (and in assertions) */
    private int numFields;

    /** The size of this row's backing data, in bytes) */
    private int sizeInBytes;

    /** The width of the null tracking bit set, in bytes */
    private int bitSetWidthInBytes;


    /**
     * Returns the underlying bytes for this UnsafeRow.
     */
    public byte[] getBytes() {
        if (baseObject instanceof byte[] && baseOffset == Platform.BYTE_ARRAY_OFFSET
                && (((byte[]) baseObject).length == sizeInBytes)) {
            return (byte[]) baseObject;
        } else {
            byte[] bytes = new byte[sizeInBytes];
            Platform.copyMemory(baseObject, baseOffset, bytes, Platform.BYTE_ARRAY_OFFSET, sizeInBytes);
            return bytes;
        }
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        byte[] bytes = getBytes();
        out.writeInt(bytes.length);
        out.writeInt(this.numFields);
        out.write(bytes);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.baseOffset = BYTE_ARRAY_OFFSET;
        this.sizeInBytes = in.readInt();
        this.numFields = in.readInt();
        this.bitSetWidthInBytes = calculateBitSetWidthInBytes(numFields);
        this.baseObject = new byte[sizeInBytes];
        in.readFully((byte[]) baseObject);
    }

    @Override
    public void write(Kryo kryo, Output out) {
        byte[] bytes = getBytes();
        out.writeInt(bytes.length);
        out.writeInt(this.numFields);
        out.write(bytes);
    }

    @Override
    public void read(Kryo kryo, Input in) {
        this.baseOffset = BYTE_ARRAY_OFFSET;
        this.sizeInBytes = in.readInt();
        this.numFields = in.readInt();
        this.bitSetWidthInBytes = calculateBitSetWidthInBytes(numFields);
        this.baseObject = new byte[sizeInBytes];
        in.read((byte[]) baseObject);
    }
}
