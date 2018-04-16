/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.crail;

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
	
	public byte get();
	
	public ByteBuffer put(byte b);
	
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
