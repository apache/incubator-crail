package com.ibm.crail;

import java.nio.ByteBuffer;

public interface CrailBuffer {
	
	//ByteBuffer metadata
	
	public int capacity();
	
	public int position();
	
	public CrailBuffer position(int newPosition);
	
	public int limit();
	
	public CrailBuffer limit(int newLimit);
	
	public CrailBuffer clear();
	
	public CrailBuffer flip();
	
	public int remaining();
	
	public boolean hasRemaining();

	public CrailBuffer slice();
	
	//Crail metadata
	
	public long address();
	
	public CrailBuffer getRegion();
	
	public ByteBuffer getByteBuffer();
	
	//ByteBuffer data
	
	public CrailBuffer get(byte[] buf, int off, int bufferRemaining);
	
	public CrailBuffer put(byte[] dataBuf, int off, int bufferRemaining);
	
	public CrailBuffer get(byte[] bytes);
	
	public CrailBuffer put(byte[] bytes);

	public short getShort();

	public CrailBuffer putShort(short value);

	public int getInt();

	public CrailBuffer putInt(int value);

	public long getLong();

	public CrailBuffer putLong(long value);

	public float getFloat();

	public CrailBuffer putFloat(float value);

	public double getDouble();

	public CrailBuffer putDouble(double value);
	
	public CrailBuffer put(ByteBuffer buf);
	
	public CrailBuffer get(ByteBuffer buf);
}
