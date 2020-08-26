package study.spark.unsafe.bitset;

import study.spark.unsafe.Platform;

/**
 * Methods for working with fixed-size uncompressed bitsets.
 *
 * We assume that the bitset data is word-aligned (that is, a multiple of 8 bytes in length).
 *
 * Each bit occupies exactly one bit of storage.
 */
public final class BitSetMethods {

    private static final long WORD_SIZE = 8;

    /**
     * Returns {@code true} if the bit is set at the specified index.
     */
    public static boolean isSet(Object baseObject, long baseOffset, int index) {
        assert index >= 0 : "index (" + index + ") should >= 0";
        final long mask = 1L << (index & 0x3f);  // mod 64 and shift
        final long wordOffset = baseOffset + (index >> 6) * WORD_SIZE;
        final long word = Platform.getLong(baseObject, wordOffset);
        return (word & mask) != 0;
    }
}
