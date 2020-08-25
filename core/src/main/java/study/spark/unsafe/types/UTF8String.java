package study.spark.unsafe.types;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import study.spark.unsafe.Platform;
import study.spark.unsafe.array.ByteArrayMethods;

import javax.annotation.Nonnull;
import java.io.*;

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
    private long offset;
    private int numBytes;

    private static int[] bytesOfCodePointInUTF8 = {2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
            2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
            3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
            4, 4, 4, 4, 4, 4, 4, 4,
            5, 5, 5, 5,
            6, 6};

    protected UTF8String(Object base, long offset, int numBytes) {
        this.base = base;
        this.offset = offset;
        this.numBytes = numBytes;
    }

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

    private boolean matchAt(final UTF8String s, int pos) {
        if (s.numBytes + pos > numBytes || pos < 0) {
            return false;
        }
        return ByteArrayMethods.arrayEquals(base, offset + pos, s.base, s.offset, s.numBytes);
    }

    /**
     * Returns the number of bytes for a code point with the first byte as `b`
     * @param b The first byte of a code point
     */
    private static int numBytesForFirstByte(final byte b) {
        final int offset = (b & 0xFF) - 192;
        return (offset >= 0) ? bytesOfCodePointInUTF8[offset] : 1;
    }


    public boolean startsWith(final UTF8String prefix) {
        return matchAt(prefix, 0);
    }

    public boolean endsWith(final UTF8String suffix) {
        return matchAt(suffix, numBytes - suffix.numBytes);
    }

    /**
     * Returns the number of code points in it.
     */
    public int numChars() {
        int len = 0;
        for (int i = 0; i < numBytes; i += numBytesForFirstByte(getByte(i))) {
            len += 1;
        }
        return len;
    }

    /**
     * Returns whether this contains `substring` or not.
     */
    public boolean contains(final UTF8String substring) {
        if (substring.numBytes == 0) {
            return true;
        }

        byte first = substring.getByte(0);
        for (int i = 0; i <= numBytes - substring.numBytes; i++) {
            if (getByte(i) == first && matchAt(substring, i)) {
                return true;
            }
        }
        return false;
    }


    /**
     * Creates an UTF8String from byte array, which should be encoded in UTF-8.
     *
     * Note: `bytes` will be hold by returned UTF8String.
     */
    public static UTF8String fromBytes(byte[] bytes) {
        if (bytes != null) {
            return new UTF8String(bytes, BYTE_ARRAY_OFFSET, bytes.length);
        } else {
            return null;
        }
    }

    @Override
    public boolean equals(final Object other) {
        if (other instanceof UTF8String) {
            UTF8String o = (UTF8String) other;
            if (numBytes != o.numBytes) {
                return false;
            }
            return ByteArrayMethods.arrayEquals(base, offset, o.base, o.offset, numBytes);
        } else {
            return false;
        }
    }

    /**
     * Creates an UTF8String from String.
     */
    public static UTF8String fromString(String str) {
        if (str == null) return null;
        try {
            return fromBytes(str.getBytes("utf-8"));
        } catch (UnsupportedEncodingException e) {
            // Turn the exception into unchecked so we can find out about it at runtime, but
            // don't need to add lots of boilerplate code everywhere.
            throwException(e);
            return null;
        }
    }

    @Override
    public int hashCode() {
        int result = 1;
        for (int i = 0; i < numBytes; i ++) {
            result = 31 * result + getByte(i);
        }
        return result;
    }

    @Override
    public String toString() {
        try {
            return new String(getBytes(), "utf-8");
        } catch (UnsupportedEncodingException e) {
            // Turn the exception into unchecked so we can find out about it at runtime, but
            // don't need to add lots of boilerplate code everywhere.
            throwException(e);
            return "unknown";  // we will never reach here.
        }
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
