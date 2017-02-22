/**
 * SqlJetUtility.java
 * Copyright (C) 2009-2013 TMate Software Ltd
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; version 2 of the License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * For information on how to redistribute this software under
 * the terms of a license other than GNU General Public License
 * contact TMate Software at support@sqljet.com
 */
package org.tmatesoft.sqljet.core.internal;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.EnumSet;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import javax.annotation.Nonnull;

import org.tmatesoft.sqljet.core.SqlJetEncoding;
import org.tmatesoft.sqljet.core.SqlJetErrorCode;
import org.tmatesoft.sqljet.core.SqlJetException;
import org.tmatesoft.sqljet.core.internal.memory.SqlJetByteBuffer;
import org.tmatesoft.sqljet.core.internal.memory.SqlJetMemoryManager;
import org.tmatesoft.sqljet.core.table.SqlJetScope;
import org.tmatesoft.sqljet.core.table.SqlJetScope.SqlJetScopeBound;

/**
 * @author TMate Software Ltd.
 * @author Sergey Scherbina (sergey.scherbina@gmail.com)
 *
 */
public final class SqlJetUtility {
    /**
     * Activates logging of stack trace at each logging invocation.
     */
    private static final String SQLJET_LOG_STACKTRACE_PROP = "SQLJET_LOG_STACKTRACE";

    private static final String SQLJET_PACKAGENAME = "org.tmatesoft.sqljet";
    private static final boolean SQLJET_LOG_STACKTRACE = getBoolSysProp(SQLJET_LOG_STACKTRACE_PROP, false);
    
    public static final ISqlJetMemoryManager memoryManager = new SqlJetMemoryManager();

    /**
     * @param logger
     * @param format
     * @param args
     */
    public static void log(Logger logger, String format, Object... args) {
        StringBuilder s = new StringBuilder();
        s.append(String.format(format, args)).append('\n');
        if (SQLJET_LOG_STACKTRACE) {
            logStackTrace(s);
        }
        logger.info(s.toString());
    }

    /**
     * @param s
     */
    private static void logStackTrace(@Nonnull StringBuilder s) {
        final StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
        for (StackTraceElement stackTraceElement : stackTrace) {
            final String l = stackTraceElement.toString();
            if (l.startsWith(SQLJET_PACKAGENAME)) {
				s.append('\t').append(l).append('\n');
			}
        }
    }

    /**
     *
     * @param buf
     * @return
     */
    public static final @Nonnull ISqlJetMemoryPointer pointer(ISqlJetMemoryPointer p) {
        return p.getBuffer().getPointer(p.getPointer());
    }

    /**
     * @param bs
     * @return
     */
    public static final @Nonnull ISqlJetMemoryPointer wrapPtr(byte[] bs) {
        final ISqlJetMemoryPointer p = memoryManager.allocatePtr(bs.length);
        p.putBytes(bs);
        return p;
    }

    @SuppressWarnings("null")
	public static @Nonnull String getSysProp(@Nonnull String propName, @Nonnull String defValue) {
        return System.getProperty(propName, defValue);
    }

    public static int getIntSysProp(final String propName, final int defValue) {
        return Integer.parseInt(System.getProperty(propName, Integer.toString(defValue)));
    }

    /**
     * @param string
     * @param b
     * @return
     */
    public static boolean getBoolSysProp(String propName, boolean defValue) {
        return Boolean.parseBoolean(System.getProperty(propName, Boolean.toString(defValue)));
    }

    /**
     * @param <T>
     * @param propName
     * @param defValue
     * @return
     */
    @SuppressWarnings("null")
	public static @Nonnull <T extends Enum<T>> T getEnumSysProp(@Nonnull String propName, @Nonnull T defValue) {
        return Enum.valueOf(defValue.getDeclaringClass(), System.getProperty(propName, defValue.toString()));
    }

    /**
     * Write a four-byte big-endian integer value.
     */
    public static final ISqlJetMemoryPointer put4byte(int v) {
        final ISqlJetMemoryPointer b = memoryManager.allocatePtr(4);
        b.putInt(0, v);
        return b;
    }

    /**
     * @param dest
     * @param src
     * @param length
     */
    public static final void memcpy(byte[] dest, byte[] src, int length) {
        System.arraycopy(src, 0, dest, 0, length);
    }

    public static final void memcpy(byte[] dest, int dstPos, byte[] src, int srcPos, int length) {
        System.arraycopy(src, srcPos, dest, dstPos, length);
    }

    @SuppressWarnings("unchecked")
    public static final <T extends SqlJetCloneable> T memcpy(T src) throws SqlJetException {
        try {
            return (T) src.clone();
        } catch (CloneNotSupportedException e) {
            throw new SqlJetException(SqlJetErrorCode.INTERNAL, e);
        }
    }

    public static int strlen(@Nonnull ISqlJetMemoryPointer s, int from) {
        int p = from;
        /* Loop over the data in s. */
        while (p < s.remaining() && s.getByteUnsigned(p) != 0) {
			p++;
		}
        return p - from;
    }

    /**
     * Check to see if the i-th bit is set. Return true or false. If p is NULL
     * (if the bitmap has not been created) or if i is out of range, then return
     * false.
     *
     * @param bitSet
     * @param index
     * @return
     */
    public static boolean bitSetTest(BitSet bitSet, int index) {
        if (bitSet == null) {
			return false;
		}
        if (index < 0) {
			return false;
		}
        return bitSet.get(index);
    }

    public static final int memcmp(byte[] a1, int from1, byte[] a2, int from2, int count) {
        for (int i = 0; i < count; i++) {
            final Byte b1 = Byte.valueOf(a1[from1 + i]);
            final Byte b2 = Byte.valueOf(a2[from2 + i]);
            final int c = b1.compareTo(b2);
            if (0 != c) {
				return c;
			}
        }
        return 0;
    }

    /**
     * @param z
     * @param z2
     * @param count
     * @return
     */
    public static final int memcmp(ISqlJetMemoryPointer a1, ISqlJetMemoryPointer a2, int count) {
        for (int i = 0; i < count; i++) {
            final int b1 = a1.getByteUnsigned(i);
            final int b2 = a2.getByteUnsigned(i);
            final int c = b1 - b2;
            if (0 != c) {
				return c;
			}
        }
        return 0;
    }

    /**
     * @param z
     * @param z2
     * @param count
     * @return
     */
    public static final int memcmp(ISqlJetMemoryPointer a1, int a1offs, ISqlJetMemoryPointer a2, int a2offs, int count) {
        for (int i = 0; i < count; i++) {
            final int b1 = a1.getByteUnsigned(a1offs + i);
            final int b2 = a2.getByteUnsigned(a2offs + i);
            final int c = b1 - b2;
            if (0 != c) {
				return c;
			}
        }
        return 0;
    }

    /**
     * @param b
     * @return
     */
    public static @Nonnull byte[] addZeroByteEnd(byte[] b) {
        byte[] r = new byte[b.length + 1];
        memcpy(r, b, b.length);
        r[b.length] = 0;
        return r;
    }

    /**
     * Convert byte buffer to string.
     *
     * @param buf
     * @return
     */
    public static @Nonnull String toString(@Nonnull ISqlJetMemoryPointer buf) {
        synchronized (buf) {
            return new String(buf.getBytes());
        }
    }

    /**
     * Convert byte buffer to string.
     *
     * @param buf
     * @return
     * @throws SqlJetException
     */
    public static String toString(ISqlJetMemoryPointer buf, SqlJetEncoding enc) {
        if (buf == null || enc == null) {
			return null;
		}
        synchronized (buf) {
            byte[] bytes = buf.getBytes();
            final String s = new String(bytes, enc.getCharset());
            int p = s.indexOf(0);
            return p<0 ? s : s.substring(0, p);
        }
    }

    /**
     * Get {@link ByteBuffer} from {@link String}.
     *
     * @param s
     * @param enc
     * @return
     * @throws SqlJetException
     */
    public static ISqlJetMemoryPointer fromString(String s, SqlJetEncoding enc) {
    	return wrapPtr(s.getBytes(enc.getCharset()));
    }

    /**
     * Return the number of bytes that will be needed to store the given 64-bit
     * integer.
     */
    public static int varintLen(long v) {
        int i = 0;
        do {
            i++;
            v >>= 7;
        } while (v != 0 && i < 9);
        return i;
    }

    public static final short fromUnsigned(int value) {
        return (short) (value & 0xffff);
    }

    public static final int fromUnsigned(long value) {
        return (int) (value & 0xffffffffL);
    }

    /**
     * Write a four-byte big-endian integer value.
     */
    public static final ISqlJetMemoryPointer put4byteUnsigned(long v) {
        final ISqlJetMemoryPointer b = memoryManager.allocatePtr(4);
        b.putIntUnsigned(v);
        return b;
    }

    /**
     * Returns absolute value of argument
     *
     * @param i
     * @return
     */
    public static final long absolute(long i) {
    	return i==Long.MIN_VALUE ? Long.MAX_VALUE : Math.abs(i);
    }

    public static final Object[] addValueToArray(Object[] array1, Object value) {
    	Object[] a = new Object[array1.length + 1];
    	System.arraycopy(array1, 0, a, 0, array1.length);
    	a[a.length-1] = value;
    	return a;
    }

    public static final <E extends Enum<E>> EnumSet<E> of(E e) {
        return EnumSet.of(e);
    }

    public static final <E extends Enum<E>> EnumSet<E> of(E e1, E e2) {
        return EnumSet.of(e1, e2);
    }

    public static final <E extends Enum<E>> EnumSet<E> of(E e1, E e2, E e3) {
        return EnumSet.of(e1, e2, e3);
    }

    /**
     * @param key
     * @return
     */
    public static final Object[] adjustNumberTypes(Object[] key) {
        if (null == key) {
			return null;
		}
        for (int i = 0; i < key.length; i++) {
            key[i] = adjustNumberType(key[i]);
        }
        return key;
    }

    public static final SqlJetScope adjustScopeNumberTypes(SqlJetScope scope) {
        SqlJetScopeBound leftBound = scope.getLeftBound();
        SqlJetScopeBound rightBound = scope.getRightBound();
        if (leftBound != null) {
            leftBound = new SqlJetScopeBound(adjustNumberTypes(leftBound.getValue()), leftBound.isInclusive());
        }
        if (rightBound != null) {
            rightBound = new SqlJetScopeBound(adjustNumberTypes(rightBound.getValue()), rightBound.isInclusive());
        }
        return new SqlJetScope(leftBound, rightBound);
    }

    public static final Object adjustNumberType(Object value) {
        if (null == value) {
            return null;
        }
        if (value instanceof Number) {
            if (value instanceof Byte || value instanceof Short || value instanceof Integer) {
                return Long.valueOf(((Number) value).longValue());
            } else if (value instanceof Float) {
                return Double.valueOf(((Float) value).toString());
            }
        }
        return value;
    }

    /**
     * @param value
     * @return
     * @throws SqlJetException
     */
    public static ISqlJetMemoryPointer streamToBuffer(InputStream stream) throws SqlJetException {
        try {
            byte[] b = new byte[stream.available()];
            stream.read(b);
            stream.reset();
            return wrapPtr(b);
        } catch (IOException e) {
            throw new SqlJetException(SqlJetErrorCode.IOERR, e);
        }

    }

    /**
     * @param firstKey
     * @return
     */
    public static Object[] copyArray(Object[] array) {
        if (null == array) {
			return null;
		}
        final Object[] copy = new Object[array.length];
        System.arraycopy(array, 0, copy, 0, array.length);
        return copy;
    }

    static private final Pattern NUMBER_PATTER = Pattern.compile("[-+]?(([0-9]+)|([0-9]*\\.))[0-9]+([eE][-+]?[0-9]+)?");
    static private final Pattern REAL_PATTERN = Pattern.compile("[-+]?[0-9]*\\.[0-9]+([eE][-+]?[0-9]+)?");
    
    /**
     * Return TRUE if z is a pure numeric string. Return FALSE and leave realnum
     * unchanged if the string contains any character which is not part of a
     * number.
     *
     * If the string is pure numeric, set realnum to TRUE if the string contains
     * the '.' character or an "E+000" style exponentiation suffix. Otherwise
     * set realnum to FALSE. Note that just becaue realnum is false does not
     * mean that the number can be successfully converted into an integer - it
     * might be too big.
     *
     * An empty string is considered non-numeric.
     *
     * @param s
     * @return
     */
    public static boolean isNumber(String s) {
        if (s == null) {
			return false;
		}
        return NUMBER_PATTER.matcher(s).matches();
    }
    
    public static boolean isRealNumber(String s) {
    	return isNumber(s) && REAL_PATTERN.matcher(s).matches();
    }

    /**
     * @param value
     * @return
     */
    public static ISqlJetMemoryPointer fromByteBuffer(ByteBuffer b) {
        return new SqlJetByteBuffer(b).getPointer(0);
    }
    
    public static ISqlJetMemoryPointer getMoved(ISqlJetMemoryPointer preceding, ISqlJetMemoryPointer ptr, int offset) {
        if (ptr.getPointer() + offset >= 0) {
            return ptr.getMoved(offset);
        }
        
        assert preceding != null;
        
        int getFromPreceding = -(ptr.getPointer() + offset);
        int ptrLength = ptr.getLimit();
        int precedingLength = preceding.getLimit() - preceding.getPointer();
        ISqlJetMemoryPointer newPtr = memoryManager.allocatePtr(ptrLength + getFromPreceding);
        newPtr.copyFrom(0, preceding, precedingLength - getFromPreceding, getFromPreceding);
        newPtr.copyFrom(getFromPreceding, ptr, 0, ptrLength);
        return newPtr;
    }
}
