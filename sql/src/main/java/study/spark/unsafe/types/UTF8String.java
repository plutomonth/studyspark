package study.spark.unsafe.types;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import study.spark.unsafe.Platform;

import javax.annotation.Nonnull;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import static study.spark.unsafe.Platform.*;

/**
 * A UTF-8 String for internal Spark use.
 * <p>
 * A String encoded in UTF-8 as an Array[Byte], which can be used for comparison,
 * search, see http://en.wikipedia.org/wiki/UTF-8 for details.
 * <p>
 * Note: This is not designed for general use cases, should not be used outside SQL.
 */
public final class UTF8String implements Comparable<UTF8String>, Externalizable, KryoSerializable {

    // These are only updated by readExternal() or read()
    @Nonnull
    private Object base;
    private long offset;    private int numBytes;

    @Override
    public int compareTo(@Nonnull final UTF8String other) {
        int len = Math.min(numBytes, other.numBytes);
        // TODO: compare 8 bytes as unsigned long
        for (int i = 0; i < len; i ++) {
            // In UTF-8, the byte should be unsigned, so we should compare them as unsigned int.
            int res = (getByte(i) & 0xFF) - (other.getByte(i) & 0xFF);
            if (res != 0) {
                return res;
            }
        }
        return numBytes - other.numBytes;
    }


    /**
     * Returns the underline bytes, will be a copy of it if it's part of another array.
     */
    public byte[] getBytes() {
        // avoid copy if `base` is `byte[]`
        if (offset == BYTE_ARRAY_OFFSET && base instanceof byte[]
                && ((byte[]) base).length == numBytes) {
            return (byte[]) base;
        } else {
            byte[] bytes = new byte[numBytes];
            copyMemory(base, offset, bytes, BYTE_ARRAY_OFFSET, numBytes);
            return bytes;
        }
    }

    /**
     * Returns the byte at position `i`.
     */
    private byte getByte(int i) {
        return Platform.getByte(base, offset + i);
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        byte[] bytes = getBytes();
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        offset = BYTE_ARRAY_OFFSET;
        numBytes = in.readInt();
        base = new byte[numBytes];
        in.readFully((byte[]) base);
    }

    @Override
    public void write(Kryo kryo, Output out) {
        byte[] bytes = getBytes();
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    @Override
    public void read(Kryo kryo, Input in) {
        this.offset = BYTE_ARRAY_OFFSET;
        this.numBytes = in.readInt();
        this.base = new byte[numBytes];
        in.read((byte[]) base);
    }

}
