package com.cloudera.flume.util;

import java.util.zip.CRC32;

public class CRCUtil {

	private static CRC32 crc;
	
	public CRCUtil() {
		crc = new CRC32();
	}

	public long genHash(byte[] b) {
		crc.reset();
		crc.update(b);
		return crc.getValue();
	}
}
