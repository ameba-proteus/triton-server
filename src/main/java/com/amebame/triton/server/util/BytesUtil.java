package com.amebame.triton.server.util;

import java.nio.ByteBuffer;


/**
 * Binary functions
 */
public class BytesUtil {

	public BytesUtil() {
	}
	
	private static final int ZERO = 0x00;
	private static final int MAX = 0xff;

	/**
	 * Get next byte array
	 * @param buffer
	 * @return
	 */
	public static final byte[] next(byte[] bytes) {
		return _next(copy(bytes));
	}
	
	/**
	 * Get next buffer
	 * @param buffer
	 * @return
	 */
	public static final ByteBuffer next(ByteBuffer buffer) {
		return ByteBuffer.wrap(_next(copy(buffer)));
	}
	
	private static final byte[] _next(byte[] bytes) {
		int pos = bytes.length -1;
		while (pos >= 0) {
			int b = bytes[pos] & 0xff;
			if (b == MAX) {
				bytes[pos] = ZERO;
			} else {
				b++;
				bytes[pos] = (byte) b;
				break;
			}
			pos--;
		}
		return bytes;
	}
	
	/**
	 * Get previous bytes
	 * @param bytes
	 * @return
	 */
	public static final byte[] previous(byte[] bytes) {
		return _previous(copy(bytes));
	}
	
	/**
	 * Get previous buffer
	 * @param buffer
	 * @return
	 */
	public static final ByteBuffer previous(ByteBuffer buffer) {
		return ByteBuffer.wrap(_previous(copy(buffer)));
	}
	
	private static final byte[] _previous(byte[] bytes) {
		int pos = bytes.length -1;
		while (pos >= 0) {
			int b = bytes[pos] & 0xff;
			if (b == ZERO) {
				bytes[pos] = (byte) MAX;
			} else {
				b--;
				bytes[pos] = (byte) b;
				break;
			}
			pos--;
		}
		return bytes;
	}
	
	/**
	 * Copy binary
	 * @param bytes
	 * @return
	 */
	private static final byte[] copy(byte[] bytes) {
		byte[] copy = new byte[bytes.length];
		System.arraycopy(bytes, 0, copy, 0, bytes.length);
		return copy;
	}
	
	/**
	 * Copy buffer to byte array
	 * @param buffer
	 * @return
	 */
	private static final byte[] copy(ByteBuffer buffer) {
		buffer.mark();
		byte[] bytes = new byte[buffer.limit()];
		buffer.get(bytes);
		buffer.reset();
		return bytes;
	}
}
