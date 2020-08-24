package study.spark.unsafe.array;

import study.spark.unsafe.Platform;

public class ByteArrayMethods {

    /**
     * Optimized byte array equality check for byte arrays.
     * @return true if the arrays are equal, false otherwise
     */
    public static boolean arrayEquals(
            Object leftBase, long leftOffset, Object rightBase, long rightOffset, final long length) {
        int i = 0;
        while (i <= length - 8) {
            if (Platform.getLong(leftBase, leftOffset + i) !=
                    Platform.getLong(rightBase, rightOffset + i)) {
                return false;
            }
            i += 8;
        }
        while (i < length) {
            if (Platform.getByte(leftBase, leftOffset + i) !=
                    Platform.getByte(rightBase, rightOffset + i)) {
                return false;
            }
            i += 1;
        }
        return true;
    }
}
