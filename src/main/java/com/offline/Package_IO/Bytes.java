/*
 * Decompiled with CFR 0_102.
 */
package com.offline.Package_IO;

public class Bytes {
    public static int toInt(byte[] src, int srcPos) {
        int dword = 0;
        for (int i = 0; i < src.length - srcPos; ++i) {
            dword = (dword << 8) + (src[i + srcPos] & 127);
            if ((src[i + srcPos] & 128) != 128) continue;
            dword+=128;
        }
        return dword;
    }

    public static int toInt(byte[] src) {
        return Bytes.toInt(src, 0);
    }

    public static int toInt(byte src) {
        byte[] b = new byte[]{src};
        return Bytes.toInt(b, 0);
    }

    public static long toLong(byte[] src, int srcPos) {
        long dword = 0;
        for (int i = 0; i < src.length - srcPos; ++i) {
            dword = (dword << 8) + (long)(src[i + srcPos] & 127);
            if ((src[i + srcPos] & 128) != 128) continue;
            dword+=128;
        }
        return dword;
    }

    public static long toLong(byte[] src) {
        return Bytes.toLong(src, 0);
    }
}

