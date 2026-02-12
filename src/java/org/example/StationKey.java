package org.example;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

final class StationKey {
    private final byte[] bytes;
    private final int offset;
    private final int length;
    private final int hash;

    StationKey(byte[] source, int off, int len) {
        this.bytes = Arrays.copyOfRange(source, off, off + len);
        this.offset = 0;
        this.length = len;
        this.hash = computeHash(bytes, offset, length);
    }

    private static int computeHash(byte[] b, int off, int len) {
        int h = 1;
        for (int i = off; i < off + len; i++) {
            h = 31 * h + (b[i] & 0xFF);
        }
        return h;
    }

    @Override
    public int hashCode() {
        return hash;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof StationKey other)) return false;
        if (this.length != other.length) return false;

        byte[] a = this.bytes;
        byte[] b = other.bytes;
        int i = this.offset;
        int j = other.offset;
        int end = i + this.length;

        while (i < end) {
            if (a[i] != b[j]) return false;
            i++;
            j++;
        }
        return true;
    }

    String toStringUtf8() {
        return new String(bytes, offset, length, StandardCharsets.UTF_8);
    }
}
