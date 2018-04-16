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

package org.apache.crail.memory;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.crail.CrailBuffer;
import org.apache.crail.utils.CrailUtils;

import sun.nio.ch.DirectBuffer;

public class OffHeapBuffer implements CrailBuffer {
	private CrailBuffer region;
	private ByteBuffer buffer;
	private long address;
	
	OffHeapBuffer(ByteBuffer buffer) {
		this.region = this;
		this.buffer = buffer;
		this.address = CrailUtils.getAddress(buffer);
	}	
	
	OffHeapBuffer(CrailBuffer region, ByteBuffer buffer) {
		this.region = region;
		this.buffer = buffer;
		this.address = CrailUtils.getAddress(buffer);
	}
	
	public static OffHeapBuffer wrap(ByteBuffer buffer) throws IOException {
		if (!(buffer instanceof DirectBuffer)) {
			throw new IOException("buffer not offheap");
		}		
		return new OffHeapBuffer(buffer);
	}	

	@Override
	public int capacity() {
		return buffer.capacity();
	}

	@Override
	public int position() {
		return buffer.position();
	}

	@Override
	public CrailBuffer position(int newPosition) {
		buffer.position(newPosition);
		return this;
	}

	@Override
	public int limit() {
		return buffer.limit();
	}

	@Override
	public CrailBuffer limit(int newLimit) {
		buffer.limit(newLimit);
		return this;
	}

	@Override
	public CrailBuffer clear() {
		buffer.clear();
		return this;
	}

	@Override
	public CrailBuffer flip() {
		buffer.flip();
		return this;
	}

	@Override
	public int remaining() {
		return buffer.remaining();
	}

	@Override
	public boolean hasRemaining() {
		return buffer.hasRemaining();
	}

	@Override
	public CrailBuffer slice() {
		try {
			ByteBuffer slice = buffer.slice();
			return new OffHeapBuffer(region, slice);
		} catch(Exception e){
			return null;
		}
	}
	
	//Crail metadata
	
	public long address(){
		return address;
	}
	
	public CrailBuffer getRegion(){
		return region;
	}	
	
	@Override
	public ByteBuffer getByteBuffer() {
		return buffer;
	}	
	
	@Override
	public byte get() {
		return buffer.get();
	}

	//ByteBuffer data
	
	@Override
	public ByteBuffer put(byte b) {
		return buffer.put(b);
	}

	@Override
	public CrailBuffer get(byte[] dst, int offset, int length) {
		buffer.get(dst, offset, length);
		return this;
	}
	
	@Override
	public CrailBuffer put(byte[] src, int offset, int length) {
		buffer.put(src, offset, length);
		return this;
	}	
	
	@Override
	public CrailBuffer get(byte[] bytes) {
		buffer.get(bytes);
		return this;
	}

	@Override
	public CrailBuffer put(byte[] bytes) {
		buffer.put(bytes);
		return this;
	}

	@Override
	public short getShort() {
		return buffer.getShort();
	}

	@Override
	public CrailBuffer putShort(short value) {
		buffer.putShort(value);
		return this;
	}

	@Override
	public int getInt() {
		return buffer.getInt();
	}

	@Override
	public CrailBuffer putInt(int value) {
		buffer.putInt(value);
		return this;
	}

	@Override
	public long getLong() {
		return buffer.getLong();
	}

	@Override
	public CrailBuffer putLong(long value) {
		buffer.putLong(value);
		return this;
	}

	@Override
	public float getFloat() {
		return buffer.getFloat();
	}

	@Override
	public CrailBuffer putFloat(float value) {
		buffer.putFloat(value);
		return this;
	}

	@Override
	public double getDouble() {
		return buffer.getDouble();
	}

	@Override
	public CrailBuffer putDouble(double value) {
		buffer.putDouble(value);
		return this;
	}
	
	@Override
	public CrailBuffer put(ByteBuffer src) {
		buffer.put(src);
		return this;
	}

	@Override
	public CrailBuffer get(ByteBuffer dst) {
		dst.put(buffer);
		return this;
	}

	@Override
	public String toString() {
		return buffer.toString();
	}
}
