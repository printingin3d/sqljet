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
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.EnumSet;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import org.tmatesoft.sqljet.core.ISqlJetMutex;
import org.tmatesoft.sqljet.core.SqlJetEncoding;
import org.tmatesoft.sqljet.core.SqlJetError;
import org.tmatesoft.sqljet.core.SqlJetErrorCode;
import org.tmatesoft.sqljet.core.SqlJetException;
import org.tmatesoft.sqljet.core.SqlJetLogDefinitions;
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
    private static final String SQLJET_PACKAGENAME = "org.tmatesoft.sqljet";
    private static final boolean SQLJET_LOG_STACKTRACE = getBoolSysProp(SqlJetLogDefinitions.SQLJET_LOG_STACKTRACE,
            false);
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
    private static void logStackTrace(StringBuilder s) {
        final StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
        for (StackTraceElement stackTraceElement : stackTrace) {
            final String l = stackTraceElement.toString();
            if (l.startsWith(SQLJET_PACKAGENAME)) {
				s.append('\t').append(l).append('\n');
			}
        }
    }

    public static final ISqlJetMemoryPointer allocatePtr(int size, SqlJetMemoryBufferType bufferType) {
        return memoryManager.allocatePtr(size, bufferType);
    }

    /**
     *
     * @param buf
     * @return
     */
    public static final ISqlJetMemoryPointer pointer(ISqlJetMemoryPointer p) {
        return p.getBuffer().getPointer(p.getPointer());
    }

    /**
     * @param bs
     * @return
     */
    public static final ISqlJetMemoryPointer wrapPtr(byte[] bs) {
        final ISqlJetMemoryPointer p = memoryManager.allocatePtr(bs.length);
        p.putBytes(bs);
        return p;
    }

    public static String getSysProp(final String propName, final String defValue) throws SqlJetError {
        if (null == propName) {
			throw new SqlJetError("Undefined property name");
		}
        try {
            return System.getProperty(propName, defValue);
        } catch (Throwable t) {
            throw new SqlJetError("Error while get int value for property " + propName, t);
        }
    }

    public static int getIntSysProp(final String propName, final int defValue) throws SqlJetError {
        if (null == propName) {
			throw new SqlJetError("Undefined property name");
		}
        try {
            return Integer.parseInt(System.getProperty(propName, Integer.toString(defValue)));
        } catch (Throwable t) {
            throw new SqlJetError("Error while get int value for property " + propName, t);
        }
    }

    /**
     * @param string
     * @param b
     * @return
     */
    public static boolean getBoolSysProp(String propName, boolean defValue) {
        if (null == propName) {
			throw new SqlJetError("Undefined property name");
		}
        try {
            return Boolean.parseBoolean(System.getProperty(propName, Boolean.toString(defValue)));
        } catch (Throwable t) {
            throw new SqlJetError("Error while get int value for property " + propName, t);
        }
    }

    /**
     * @param <T>
     * @param propName
     * @param defValue
     * @return
     */
    public static <T extends Enum<T>> T getEnumSysProp(String propName, T defValue) {
        if (null == propName) {
			throw new SqlJetError("Undefined property name");
		}
        if (null == defValue) {
			throw new SqlJetError("Undefined default value");
		}
        try {
            return Enum.valueOf(defValue.getDeclaringClass(), System.getProperty(propName, defValue.toString()));
        } catch (Exception t) {
            throw new SqlJetError("Error while get int value for property " + propName, t);
        }
    }

    /**
     * Write a two-byte big-endian integer values.
     */
    public static final void put2byte(ISqlJetMemoryPointer p, int v) {
        put2byte(p, 0, v);
    }

    /**
     * Write a two-byte big-endian integer values.
     */
    public static final void put2byte(ISqlJetMemoryPointer p, int off, int v) {
        p.putShortUnsigned(off, v);
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
     * Write a four-byte big-endian integer value.
     */
    public static final void put4byte(ISqlJetMemoryPointer p, int pos, long v) {
        if (null == p || (p.remaining() - pos) < 4) {
			throw new SqlJetError("Wrong destination");
		}
        p.putIntUnsigned(pos, v);
    }

    /**
     * Write a four-byte big-endian integer value.
     */
    public static final void put4byte(ISqlJetMemoryPointer p, long v) {
        put4byte(p, 0, v);
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

    public static final void memcpy(SqlJetCloneable[] dest, SqlJetCloneable[] src) throws SqlJetException {
        memcpy(src, 0, dest, 0);
    }

    @SuppressWarnings("unchecked")
    public static final <T extends SqlJetCloneable> T memcpy(T src) throws SqlJetException {
        try {
            return (T) src.clone();
        } catch (CloneNotSupportedException e) {
            throw new SqlJetException(SqlJetErrorCode.INTERNAL, e);
        }
    }

    /**
     * @param src
     * @param dstPos
     * @param dest
     * @param srcPos
     *
     * @throws SqlJetException
     */
    private static final void memcpy(SqlJetCloneable[] src, int srcPos, SqlJetCloneable[] dest, int dstPos)
            throws SqlJetException {
        for (int x = srcPos, y = dstPos; x < src.length && y < dest.length; x++, y++) {
            final SqlJetCloneable o = src[x];
            if (null == o) {
				continue;
			}
            try {
                dest[y] = o.clone();
            } catch (CloneNotSupportedException e) {
                throw new SqlJetException(SqlJetErrorCode.INTERNAL, e);
            }
        }
    }

    /**
     * @param data
     * @param from
     * @param value
     * @param count
     */
    public static final void memset(ISqlJetMemoryPointer data, int from, byte value, int count) {
        data.fill(from, count, value);
    }

    /**
     * @param data
     * @param value
     * @param count
     */
    public static final void memset(ISqlJetMemoryPointer data, byte value, int count) {
        memset(data, 0, value, count);
    }

    /**
     * @param data
     * @param value
     */
    public static final void memset(ISqlJetMemoryPointer data, byte value) {
        memset(data, value, data.remaining());
    }

    public static int strlen(ISqlJetMemoryPointer s, int from) {
        int p = from;
        /* Loop over the data in s. */
        while (p < s.remaining() && s.getByteUnsigned(p) != 0) {
			p++;
		}
        return (p - from);
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

    /**
     * @param magic
     * @param journalMagic
     * @param i
     * @return
     */
    public static final int memcmp(byte[] a1, byte[] a2, int count) {
        for (int i = 0; i < count; i++) {
            final Byte b1 = Byte.valueOf(a1[i]);
            final Byte b2 = Byte.valueOf(a2[i]);
            final int c = b1.compareTo(b2);
            if (0 != c) {
				return c;
			}
        }
        return 0;
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
    public static byte[] addZeroByteEnd(byte[] b) {
        if (null == b) {
			throw new SqlJetError("Undefined byte array");
		}
        byte[] r = new byte[b.length + 1];
        memcpy(r, b, b.length);
        r[b.length] = 0;
        return r;
    }

    /**
     * @param sqliteFileHeader
     * @return
     */
    public static byte[] getBytes(String string) {
        if (null == string) {
			throw new SqlJetError("Undefined string");
		}
        try {
            return string.getBytes("UTF8");
        } catch (Throwable t) {
            throw new SqlJetError("Error while get bytes for string \"" + string + "\"", t);
        }
    }

    /**
     * Return the number of bytes that will be needed to store the given 64-bit
     * integer.
     */
    public static int sqlite3VarintLen(long v) {
        int i = 0;
        do {
            i++;
            v >>= 7;
        } while (v != 0 && i < 9);
        return i;
    }

    /**
     * @param mutex
     * @return
     */
    public static final boolean mutex_held(ISqlJetMutex mutex) {
        return mutex == null || mutex.held();
    }

    /**
     * Convert byte buffer to string.
     *
     * @param buf
     * @return
     */
    public static String toString(ISqlJetMemoryPointer buf) {
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
    public static String toString(ISqlJetMemoryPointer buf, SqlJetEncoding enc) throws SqlJetException {
        if (buf == null || enc == null) {
			return null;
		}
        synchronized (buf) {
            byte[] bytes = buf.getBytes();
            try {
                final String s = new String(bytes, enc.getCharsetName());
                for(int i=0;i<s.length();i++){
                	if(s.charAt(i)=='\0'){
                		return s.substring(0, i);
                	}
                }
				return s;
            } catch (UnsupportedEncodingException e) {
                throw new SqlJetException(SqlJetErrorCode.MISUSE, "Unknown charset " + enc.name());
            }
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
    public static ISqlJetMemoryPointer fromString(String s, SqlJetEncoding enc) throws SqlJetException {
        try {
            return wrapPtr(s.getBytes(enc.getCharsetName()));
        } catch (UnsupportedEncodingException e) {
            throw new SqlJetException(SqlJetErrorCode.MISUSE, "Unknown charset " + enc.name());
        }
    }

    /**
     * Translate {@link ByteBuffer} from one charset to other charset.
     *
     * @param buf
     * @param from
     * @param to
     * @return
     * @throws SqlJetException
     */
    public static ISqlJetMemoryPointer translate(ISqlJetMemoryPointer buf, SqlJetEncoding from, SqlJetEncoding to)
            throws SqlJetException {
        return fromString(toString(buf, from), to);
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

    public static final short toUnsigned(byte value) {
        return (short) (value & (short) 0xff);
    }

    public static final byte fromUnsigned(short value) {
        return (byte) (value & 0xff);
    }

    public static final int toUnsigned(short value) {
        return value & 0xffff;
    }

    public static final short fromUnsigned(int value) {
        return (short) (value & 0xffff);
    }

    public static long toUnsigned(int value) {
        return value & 0xffffffffL;
    }

    public static final int fromUnsigned(long value) {
        return (int) (value & 0xffffffffL);
    }

    /**
     * Read a four-byte big-endian integer value.
     */
    public static final long get4byteUnsigned(byte[] p) {
        return wrapPtr(p).getIntUnsigned();
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
     * Write a four-byte big-endian integer value.
     */
    public static final void put4byteUnsigned(byte[] p, int pos, long v) {
        wrapPtr(p).putIntUnsigned(pos, v);
    }

    /**
     * Read a four-byte big-endian integer value.
     */
    public static final long get4byteUnsigned(ISqlJetMemoryPointer p, int pos) {
        return p.getIntUnsigned(pos);
    }

    /**
     * Write a four-byte big-endian integer value.
     */
    public static final void put4byteUnsigned(ISqlJetMemoryPointer p, long v) {
        p.putIntUnsigned(v);
    }

    /**
     * @param z
     * @param slice
     * @param n
     */
    public static final void memmove(ISqlJetMemoryPointer dst, ISqlJetMemoryPointer src, int n) {
        memmove(dst, 0, src, 0, n);
    }

    /**
     * @param z
     * @param slice
     * @param n
     */
    public static final void memmove(ISqlJetMemoryPointer dst, int dstOffs, ISqlJetMemoryPointer src, int srcOffs, int n) {
        byte[] b = new byte[n];
        src.getBytes(srcOffs, b, n);
        dst.putBytes(dstOffs, b, n);
    }

    /**
     * @param z
     * @return
     */
    public static final double atof(ISqlJetMemoryPointer z) {
        final String s = toString(z);
        return Double.valueOf(s).doubleValue();
    }

    /**
     * Returns absolute value of argument
     *
     * @param i
     * @return
     */
    public static final long absolute(long i) {
        long u;
        u = i < 0 ? -i : i;
        if (u == Integer.MIN_VALUE || u == Long.MIN_VALUE) {
			u = u - 1;
		}
        return u;
    }

    /**
     * @param key
     * @param dataRowId
     * @return
     */
    public static final Object[] addArrays(Object[] array1, Object[] array2) {
        Object[] a = new Object[array1.length + array2.length];
        System.arraycopy(array1, 0, a, 0, array1.length);
        System.arraycopy(array2, 0, a, array1.length, array2.length);
        return a;
    }
    
    public static final Object[] addValueToArray(Object[] array1, Object value) {
    	Object[] a = new Object[array1.length + 1];
    	System.arraycopy(array1, 0, a, 0, array1.length);
    	a[a.length-1] = value;
    	return a;
    }

    public static final Object[] insertArray(Object[] intoArray, Object[] insertArray, int pos) {
        Object[] a = new Object[intoArray.length + insertArray.length];
        System.arraycopy(intoArray, 0, a, 0, pos);
        System.arraycopy(insertArray, 0, a, pos, insertArray.length);
        System.arraycopy(intoArray, pos, a, insertArray.length + pos, intoArray.length - pos);
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
        if (null == scope) {
			return null;
		}
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
        if (stream == null) {
			return null;
		}
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
     * @param r
     * @return
     */
    public static Long doubleToInt64(double r) {
        if (r == Double.NaN) {
			return null;
		}
        if (r == Double.POSITIVE_INFINITY) {
			return null;
		}
        if (r == Double.NEGATIVE_INFINITY) {
			return null;
		}
        final double rint = Math.rint(r);
        if (r != rint) {
			return null;
		}
        return Long.valueOf(Double.valueOf(r).longValue());
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
        
        assert(preceding != null);
        
        int getFromPreceding = -(ptr.getPointer() + offset);
        int ptrLength = ptr.getLimit() - ptr.getPointer();
        int precedingLength = preceding.getLimit() - preceding.getPointer();
        ISqlJetMemoryPointer newPtr = memoryManager.allocatePtr(ptrLength + getFromPreceding);
        newPtr.copyFrom(0, preceding, precedingLength - getFromPreceding, getFromPreceding);
        newPtr.copyFrom(getFromPreceding, ptr, 0, ptrLength);
        return newPtr;
    }
}
