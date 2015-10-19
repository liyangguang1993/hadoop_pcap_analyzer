/*
 * Decompiled with CFR 0_102.
 */
package com.offline.Package_IO;

import java.nio.ByteOrder;

public class BinaryUtils {
    public static byte[] flip(byte[] bytes, int length) {
        byte[] tmp = new byte[length];
        for (int i = 0; i < length; ++i) {
            tmp[i] = bytes[length - 1 - i];
        }
        return tmp;
    }

    public static byte[] flipBO(byte[] bytes, int length) {
        if (ByteOrder.nativeOrder().toString().equals("BIG_ENDIAN")) {
            return bytes;
        }
        byte[] tmp = new byte[length];
        for (int i = 0; i < length; ++i) {
            tmp[i] = bytes[length - 1 - i];
        }
        return tmp;
    }

    public static byte[] flipBO(byte[] bytes) {
        return BinaryUtils.flipBO(bytes, bytes.length);
    }

    /*
     * Unable to fully structure code
     * Enabled aggressive block sorting
     * Lifted jumps to return sites
     */
    /*public static int byteToInt(byte[] b) {
        int idx;
        int dataLen = b.length;
        idx = dataLen;
       int  val = 0;
        int sum = 0;
        int bit_pos = 0;
        int out = 0;
//        ** GOTO lbl15
        
        block0{
            out = b[idx] >> bit_pos & 1;
            sum|=out << bit_pos;
            ++bit_pos;
            do {
                if (bit_pos < 8) continue block0;
                val|=sum << (dataLen - 1 - idx) * 8;
                bit_pos = 0;
                sum = 0;
//lbl15: // 2 sources:
            } while (--idx >= 0);
        }
        
        
        return val;
    }*/

    /*
     * Unable to fully structure code
     * Enabled aggressive block sorting
     * Lifted jumps to return sites
     */
    /*public static int byteToInt(byte[] b, int len) {
        idx = dataLen = len;
        val = 0;
        sum = 0;
        bit_pos = 0;
        out = 0;
        ** GOTO lbl15
        {
            out = b[idx] >> bit_pos & 1;
            sum|=out << bit_pos;
            ++bit_pos;
            do {
                if (bit_pos < 8) continue block0;
                val|=sum << (dataLen - 1 - idx) * 8;
                bit_pos = 0;
                sum = 0;
lbl15: // 2 sources:
            } while (--idx >= 0);
        }
        return val;
    }*/

    public static long ubyteToLong(byte[] b) {
        long val = 0;
        for (int i = 0; i < b.length; ++i) {
            val|=((long)b[i] & 255) << (b.length - i - 1) * 8;
        }
        return val;
    }

    public static long ubyteToLong(byte[] b, int len) {
        byte[] newb = new byte[len];
        System.arraycopy(b, 0, newb, 0, newb.length);
        return BinaryUtils.ubyteToLong(newb);
    }

    public static long byteToLong(byte[] b) {
        if (b.length < 8) {
            return BinaryUtils.byteToLong(b, b.length);
        }
        long f = -72057594037927936L;
        long val = 0;
        for (int i = 0; i < b.length; ++i) {
            val|=(long)(b[i] << (b.length - i) * 8) & f >> i * 8;
        }
        return val;
    }

    /*
     * Unable to fully structure code
     * Enabled aggressive block sorting
     * Lifted jumps to return sites
     */
    public static long byteToLong(byte[] b, int len) {
        /*idx = dataLen = len;
        sum = 0;
        bit_pos = 0;
        out = 0;
        val = 0;
        ** GOTO lbl15
        {
            out = b[idx] >> bit_pos & 1;
            sum|=out << bit_pos;
            ++bit_pos;
            do {
                if (bit_pos < 8) continue block0;
                val|=(long)(sum << (dataLen - 1 - idx) * 8);
                bit_pos = 0;
                sum = 0;
lbl15: // 2 sources:
            } while (--idx >= 0);
        }

        return val;*/
        long l = 0L;
		l = java.lang.Long.parseLong(new String(b));
		return l;
    }

    public static byte[] LongToBytes(long lval) {
        byte[] bytes = new byte[8];
        long f = 255;
        for (int i = 0; i < bytes.length; ++i) {
            bytes[i] = (byte)(lval >> (bytes.length - 1 - i) * 8 & f);
        }
        return bytes;
    }

    public static byte[] IntToBytes(int val) {
        byte[] bytes = new byte[4];
        for (int i = 0; i < bytes.length; ++i) {
            bytes[i] = (byte)(val >> (bytes.length - 1 - i) * 8 & 255);
        }
        return bytes;
    }

    public static byte[] uIntToBytes(long val) {
        byte[] bytes = new byte[4];
        for (int i = 0; i < bytes.length; ++i) {
            bytes[i] = (byte)(val >> (bytes.length - 1 - i) * 8 & 255);
        }
        return bytes;
    }
}

