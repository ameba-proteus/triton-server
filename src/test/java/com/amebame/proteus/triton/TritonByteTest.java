package com.amebame.proteus.triton;

import static org.junit.Assert.assertArrayEquals;

import org.junit.Test;

import com.amebame.triton.server.util.BytesUtil;

public class TritonByteTest {

	public TritonByteTest() {
	}

	@Test
	public void testNext() {
		assertArrayEquals(
				bytes(0x01),
				next(0x00)
		);
		assertArrayEquals(
				bytes(0x02),
				next(0x01)
		);
		assertArrayEquals(
				bytes(0xa1),
				next(0xa0)
		);
		assertArrayEquals(
				bytes(0x00),
				next(0xff)
		);
		assertArrayEquals(
				bytes(0x00, 0x01),
				next(0x00, 0x00)
		);
		assertArrayEquals(
				bytes(0x30, 0xa1),
				next(0x30, 0xa0)
		);
		assertArrayEquals(
				bytes(0xa0, 0xbc, 0x00),
				next(0xa0, 0xbb, 0xff)
		);
		assertArrayEquals(
				bytes(0x00, 0x06, 0x00, 0x00, 0x00),
				next(0x00, 0x05, 0xff, 0xff, 0xff)
		);
	}
	
	@Test
	public void testPrevious() {
		assertArrayEquals(
				bytes(0x00),
				previous(0x01)
		);
		assertArrayEquals(
				bytes(0x01),
				previous(0x02)
		);
		assertArrayEquals(
				bytes(0xa1),
				previous(0xa2)
		);
		assertArrayEquals(
				bytes(0xfe),
				previous(0xff)
		);
		assertArrayEquals(
				bytes(0xff),
				previous(0x00)
		);
		assertArrayEquals(
				bytes(0x00, 0x00),
				previous(0x00, 0x01)
		);
		assertArrayEquals(
				bytes(0x30, 0xa0),
				previous(0x30, 0xa1)
		);
		assertArrayEquals(
				bytes(0xa0, 0xba, 0xff),
				previous(0xa0, 0xbb, 0x00)
		);
		assertArrayEquals(
				bytes(0x00, 0x05, 0xff, 0xff, 0xff),
				previous(0x00, 0x06, 0x00, 0x00, 0x00)
		);
	}
	
	private static final byte[] bytes(int ... values) {
		byte[] bytes = new byte[values.length];
		for (int i = 0; i < values.length; i++) {
			bytes[i] = (byte) values[i];
		}
		return bytes;
	}
	
	private static final byte[] next(int ... values) {
		return BytesUtil.next(bytes(values));
	}
	
	private static final byte[] previous(int ... values) {
		return BytesUtil.previous(bytes(values));
	}
}
