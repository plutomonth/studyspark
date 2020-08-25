package study.spark.unsafe.types;

import org.junit.Test;

import java.io.UnsupportedEncodingException;

import static org.junit.Assert.*;
import static study.spark.unsafe.types.UTF8String.fromBytes;
import static study.spark.unsafe.types.UTF8String.fromString;

public class UTF8StringSuite {
    private static void checkBasic(String str, int len) throws UnsupportedEncodingException {
        UTF8String s1 = fromString(str);
        UTF8String s2 = fromBytes(str.getBytes("utf8"));
        assertEquals(s1.numChars(), len);
        assertEquals(s2.numChars(), len);

        assertEquals(s1.toString(), str);
        assertEquals(s2.toString(), str);
        assertEquals(s1, s2);

        assertEquals(s1.hashCode(), s2.hashCode());

        assertEquals(0, s1.compareTo(s2));

        assertTrue(s1.contains(s2));
        assertTrue(s2.contains(s1));
        assertTrue(s1.startsWith(s1));
        assertTrue(s1.endsWith(s1));
    }

    @Test
    public void basicTest() throws UnsupportedEncodingException {
        checkBasic("", 0);
        checkBasic("hello", 5);
        checkBasic("大 千 世 界", 7);
    }
}
