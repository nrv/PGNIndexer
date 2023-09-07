/*
 * Copyright 2010, 2011 Institut Pasteur.
 * Copyright 2012 Institut National de l'Audiovisuel.
 *
 * This file is part of NHerveTools.
 *
 * NHerveTools is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * NHerveTools is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with NHerveTools. If not, see <http://www.gnu.org/licenses/>.
 */
package name.herve.chess.pgnindexer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.DoubleBuffer;
import java.nio.FloatBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.util.HashMap;
import java.util.Map;

/**
 * The Class PersistenceToolbox.
 *
 * @author Nicolas HERVE - nherve@ina.fr
 */
public class PersistenceToolbox {

	/** The Constant BAG_TYPE. */
	public final static int BAG_TYPE = 2;

	/** The Constant cs. */
	private final static Charset cs = Charset.forName("UTF-8");

	/** The Constant csd. */
	private final static CharsetDecoder csd = cs.newDecoder();

	/** The Constant cse. */
	private final static CharsetEncoder cse = cs.newEncoder();

	/** The Constant DENSE_TYPE. */
	public final static int DENSE_TYPE = 0;

	/** The Constant DOUBLE_NB_BYTES. */
	public final static int DOUBLE_NB_BYTES = 8;

	/** The Constant FLOAT_NB_BYTES. */
	public final static int FLOAT_NB_BYTES = 4;

	public final static int INDEX_TYPE = 4;

	/** The Constant INT_NB_BYTES. */
	public final static int INT_NB_BYTES = 4;

	public final static int LONG_NB_BYTES = 8;

	/** The Constant BAG_TYPE. */
	public final static int NULL_TYPE = 3;

	/** The Constant SPARSE_TYPE. */
	public final static int SPARSE_TYPE = 1;

	/**
	 * Dump boolean.
	 *
	 * @param fc
	 *            the fc
	 * @param b
	 *            the b
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	public static void dumpBoolean(FileChannel fc, boolean b) throws IOException {
		if (b) {
			dumpInt(fc, 1);
		} else {
			dumpInt(fc, 0);
		}
	}

	/**
	 * Dump double.
	 *
	 * @param fc
	 *            the fc
	 * @param d
	 *            the d
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	public static void dumpDouble(FileChannel fc, double d) throws IOException {
		ByteBuffer bb = ByteBuffer.allocate(DOUBLE_NB_BYTES);
		DoubleBuffer db = bb.asDoubleBuffer();
		db.put(d);
		db.flip();
		fc.write(bb);
	}

	/**
	 * Dump float.
	 *
	 * @param fc
	 *            the fc
	 * @param f
	 *            the f
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	public static void dumpFloat(FileChannel fc, float f) throws IOException {
		ByteBuffer bb = ByteBuffer.allocate(FLOAT_NB_BYTES);
		FloatBuffer db = bb.asFloatBuffer();
		db.put(f);
		db.flip();
		fc.write(bb);
	}

	public static void dumpFullIntArray(FileChannel fc, int[] i) throws IOException {
		dumpInt(fc, i.length);
		dumpIntArray(fc, i);
	}

	public static void dumpFullLongArray(FileChannel fc, long[] i) throws IOException {
		dumpInt(fc, i.length);
		dumpLongArray(fc, i);
	}

	/**
	 * Dump int.
	 *
	 * @param fc
	 *            the fc
	 * @param i
	 *            the i
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	public static void dumpInt(FileChannel fc, int i) throws IOException {
		ByteBuffer bb = ByteBuffer.allocate(INT_NB_BYTES);
		IntBuffer ib = bb.asIntBuffer();
		ib.put(i);
		ib.flip();
		fc.write(bb);
	}

	/**
	 * Dump int array.
	 *
	 * @param fc
	 *            the fc
	 * @param i
	 *            the i
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	public static void dumpIntArray(FileChannel fc, int[] i) throws IOException {
		ByteBuffer bb = ByteBuffer.allocate(i.length * INT_NB_BYTES);
		IntBuffer ib = bb.asIntBuffer();
		ib.put(i);
		ib.flip();
		fc.write(bb);
	}

	public static void dumpLong(FileChannel fc, long l) throws IOException {
		ByteBuffer bb = ByteBuffer.allocate(LONG_NB_BYTES);
		LongBuffer lb = bb.asLongBuffer();
		lb.put(l);
		lb.flip();
		fc.write(bb);
	}

	public static void dumpLongArray(FileChannel fc, long[] i) throws IOException {
		ByteBuffer bb = ByteBuffer.allocate(i.length * LONG_NB_BYTES);
		LongBuffer ib = bb.asLongBuffer();
		ib.put(i);
		ib.flip();
		fc.write(bb);
	}

	/**
	 * Dump string.
	 *
	 * @param fc
	 *            the fc
	 * @param s
	 *            the s
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	public static void dumpString(FileChannel fc, String s) throws IOException {
		CharBuffer cb = CharBuffer.allocate(s.length());
		cb.put(s);
		cb.flip();
		ByteBuffer buff = cse.encode(cb);
		dumpInt(fc, buff.limit());
		fc.write(buff);
	}

	/**
	 * Gets the file.
	 *
	 * @param f
	 *            the f
	 * @param write
	 *            the write
	 * @return the file
	 * @throws FileNotFoundException
	 *             the file not found exception
	 */
	public static RandomAccessFile getFile(File f, boolean write) throws FileNotFoundException {
		if (write) {
			if (f.exists()) {
				f.delete();
			}
			f.getParentFile().mkdirs();
			return new RandomAccessFile(f, "rw");
		} else {
			return new RandomAccessFile(f, "r");
		}
	}


	/**
	 * Load boolean.
	 *
	 * @param fc
	 *            the fc
	 * @return true, if successful
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	public static boolean loadBoolean(FileChannel fc) throws IOException {
		return loadInt(fc) == 1;
	}

	/**
	 * Load double.
	 *
	 * @param fc
	 *            the fc
	 * @return the double
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	public static double loadDouble(FileChannel fc) throws IOException {
		ByteBuffer bb = ByteBuffer.allocate(DOUBLE_NB_BYTES);
		fc.read(bb);
		bb.flip();
		DoubleBuffer db = bb.asDoubleBuffer();
		return db.get();
	}

	/**
	 * Load float.
	 *
	 * @param fc
	 *            the fc
	 * @return the float
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	public static float loadFloat(FileChannel fc) throws IOException {
		ByteBuffer bb = ByteBuffer.allocate(FLOAT_NB_BYTES);
		fc.read(bb);
		bb.flip();
		FloatBuffer db = bb.asFloatBuffer();
		return db.get();
	}

	public static int[] loadFullIntArray(FileChannel fc) throws IOException {
		int sz = loadInt(fc);
		return loadIntArray(fc, sz);
	}

	public static long[] loadFullLongArray(FileChannel fc) throws IOException {
		int sz = loadInt(fc);
		return loadLongArray(fc, sz);
	}

	/**
	 * Load int.
	 *
	 * @param fc
	 *            the fc
	 * @return the int
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	public static int loadInt(FileChannel fc) throws IOException {
		ByteBuffer bb = ByteBuffer.allocate(INT_NB_BYTES);
		fc.read(bb);
		bb.flip();
		IntBuffer ib = bb.asIntBuffer();
		return ib.get();
	}

	public static int[] loadIntArray(FileChannel fc, int sz) throws IOException {
		ByteBuffer bb = ByteBuffer.allocate(PersistenceToolbox.INT_NB_BYTES * sz);
		fc.read(bb);
		bb.flip();
		IntBuffer ib = bb.asIntBuffer();
		int[] res = new int[sz];
		ib.get(res);
		return res;
	}

	public static long loadLong(FileChannel fc) throws IOException {
		ByteBuffer bb = ByteBuffer.allocate(LONG_NB_BYTES);
		fc.read(bb);
		bb.flip();
		LongBuffer ib = bb.asLongBuffer();
		return ib.get();
	}

	public static long[] loadLongArray(FileChannel fc, int sz) throws IOException {
		ByteBuffer bb = ByteBuffer.allocate(PersistenceToolbox.LONG_NB_BYTES * sz);
		fc.read(bb);
		bb.flip();
		LongBuffer ib = bb.asLongBuffer();
		long[] res = new long[sz];
		ib.get(res);
		return res;
	}

	/**
	 * Load string.
	 *
	 * @param fc
	 *            the fc
	 * @return the string
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	public static String loadString(FileChannel fc) throws IOException {
		int sz = loadInt(fc);
		ByteBuffer bb = ByteBuffer.allocate(sz);
		fc.read(bb);
		bb.flip();
		CharBuffer cb = csd.decode(bb);
		return cb.toString();
	}

}
