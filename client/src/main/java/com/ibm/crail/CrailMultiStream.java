package com.ibm.crail;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface CrailMultiStream {
	public int read(ByteBuffer dataBuf) throws IOException;
	public double readDouble() throws Exception;
	public int readInt() throws Exception;
	public double readLong() throws Exception;
	public double readShort() throws Exception;
	public long position();
	public void close() throws IOException;
}
