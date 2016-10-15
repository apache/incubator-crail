/*
 * Crail: A Multi-tiered Distributed Direct Access File System
 *
 * Author: Patrick Stuedi <stu@zurich.ibm.com>
 *
 * Copyright (C) 2016, IBM Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.ibm.crail;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;

import com.ibm.crail.conf.CrailConstants;
import com.ibm.crail.utils.CrailImmediateOperation;
import com.ibm.crail.utils.CrailUtils;


public class CrailBufferedOutputStream extends OutputStream {
	public static final Logger LOG = CrailUtils.getLogger();
	private CrailOutputStream outputStream;
	private ByteBuffer internalBuf;
	private byte[] tmpByteBuf; 
	private ByteBuffer tmpBoundaryBuffer;
	private CrailFS crailFS;
	private CrailImmediateOperation noOp;
	private long position;
	private boolean pending;
	private Future<CrailResult> future;
	
	public CrailBufferedOutputStream(CrailFS crailFS, CrailOutputStream outputStream) throws IOException {
		this.crailFS = crailFS;
		this.outputStream = outputStream;
		this.internalBuf = crailFS.allocateBuffer();
		this.internalBuf.clear();		
		this.tmpByteBuf = new byte[1];
		this.tmpBoundaryBuffer = ByteBuffer.allocate(8);
		this.noOp = new CrailImmediateOperation(0);
		this.position = 0;
		this.pending = false;
		this.future = null;
	}
	
	public final synchronized void write(int dataBuf) throws IOException {
		tmpByteBuf[0] = (byte) dataBuf;
		this.write(tmpByteBuf);
	}
	
	public final synchronized void write(byte[] dataBuf) throws IOException {
		this.write(dataBuf, 0, dataBuf.length);
	}

	public final synchronized void write(byte[] dataBuf, int off, int len) throws IOException {
		try {
			if (dataBuf == null) {
				throw new NullPointerException();
			} else if ((off < 0) || (off > dataBuf.length) || (len < 0) || ((off + len) > dataBuf.length) || ((off + len) < 0)) {
				throw new IndexOutOfBoundsException();
			} else if (len == 0) {
				return;
			}

			while (len > 0) {
				completePurge();
				if (internalBuf.remaining() > 0){
					int bufferRemaining = Math.min(len, internalBuf.remaining());
					internalBuf.put(dataBuf, off, bufferRemaining);
					off += bufferRemaining;
					len -= bufferRemaining;
					position += bufferRemaining;
				}
				purgeIfFull();				
			}
		} catch (Exception e) {
			throw new IOException(e);
		}
	}	
	
	public final synchronized void write(ByteBuffer dataBuf) throws IOException {
		try {
			if (dataBuf == null) {
				throw new NullPointerException();
			} else if (dataBuf.remaining() == 0) {
				return;
			}			
			
			int len = dataBuf.remaining();
			while (len > 0) {
				completePurge();
				if (internalBuf.remaining() > 0){
					int bufferRemaining = Math.min(len, internalBuf.remaining());
					int oldLimit = dataBuf.limit();
					dataBuf.limit(dataBuf.position() + bufferRemaining);
					internalBuf.put(dataBuf);
					dataBuf.limit(oldLimit);
					len -= bufferRemaining;
					position += bufferRemaining;
				}
				purgeIfFull();
			}
		} catch (Exception e) {
			throw new IOException(e);
		}
	}
	
	private void completePurge() throws IOException {
		try {
			if (pending && future != null){
				future.get();
				internalBuf.clear();
				pending = false;
			}
		} catch(Exception e){
			throw new IOException(e);
		}
	}
	
	private synchronized void purgeIfFull() throws IOException {
		if (internalBuf.remaining() == 0){
			future = purge();
		}
	}
	
	public synchronized Future<CrailResult> purge() throws IOException {
		try {
			if (!pending && internalBuf.position() > 0) {
				internalBuf.flip();
				Future<CrailResult> purgeOp = outputStream.write(internalBuf);
				internalBuf.clear();
				pending = true;
				return purgeOp;
			} else if (pending){
				return future;
			} else {
				return noOp;
			}
		} catch(Exception e){
			throw new IOException(e);
		}
	}	
	
	public synchronized Future<Void> sync() throws IOException {
		Future<Void> future = outputStream.sync();
		return future;
	}

	public synchronized void close() throws IOException {
		try {
			if (!outputStream.isOpen()){
				return;
			}
			completePurge();
			future = purge();
			completePurge();
			outputStream.close();
			crailFS.freeBuffer(internalBuf);
		} catch (Exception e) {
			throw new IOException(e);
		} 
	}
	
	public boolean isOpen() {
		return outputStream.isOpen();
	}

	public long position() {
		return position;
	}

	//---------------------- ByteBuffer interface
	
	public final synchronized void writeDouble(double value) throws Exception {
		if (internalBuf.remaining() >= 4){
			internalBuf.putDouble(value);
			purgeIfFull();
		} else {
			tmpBoundaryBuffer.clear();
			tmpBoundaryBuffer.putDouble(value);
			write(tmpBoundaryBuffer);
		}
	}
	
	public final synchronized void writeInt(int value) throws Exception {
		if (internalBuf.remaining() >= 4){
			internalBuf.putInt(value);
			purgeIfFull();
		} else {
			tmpBoundaryBuffer.clear();
			tmpBoundaryBuffer.putInt(value);
			write(tmpBoundaryBuffer);
		}		
	}
	
	public final synchronized void writeLong(long value) throws Exception {
		if (internalBuf.remaining() >= 8){
			internalBuf.putLong(value);
			purgeIfFull();			
		} else {
			tmpBoundaryBuffer.clear();
			tmpBoundaryBuffer.putLong(value);
			write(tmpBoundaryBuffer);
		}			
	}
	
	public final synchronized void writeShort(short value) throws Exception {
		if (internalBuf.remaining() >= 2){
			internalBuf.putShort(value);
			purgeIfFull();		
		} else {
			tmpBoundaryBuffer.clear();
			tmpBoundaryBuffer.putShort(value);
			write(tmpBoundaryBuffer);
		}			
	}			

}
