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

import org.slf4j.Logger;

import com.ibm.crail.utils.CrailImmediateOperation;
import com.ibm.crail.utils.CrailUtils;


public class CrailBufferedOutputStream extends OutputStream {
	public static final Logger LOG = CrailUtils.getLogger();
	
	private CrailFS crailFS;
	private CrailFile file;
	private long writeHint;
	private CrailOutputStream outputStream;
	private ByteBuffer internalBuf;
	private byte[] tmpByteBuf; 
	private ByteBuffer tmpBoundaryBuffer;
	private CrailImmediateOperation noOp;
	private long position;
	private Future<CrailResult> future;
	private boolean open;
	
	CrailBufferedOutputStream(CrailFile file, long writeHint) throws Exception {
		this.crailFS = file.getFileSystem();
		this.file = file;
		this.writeHint = writeHint;
		this.outputStream = null;
		this.internalBuf = crailFS.allocateBuffer();
		this.internalBuf.clear();		
		this.tmpByteBuf = new byte[1];
		this.tmpBoundaryBuffer = ByteBuffer.allocate(8);
		this.noOp = new CrailImmediateOperation(0);
		this.position = 0;
		this.future = null;
		this.open = true;
	}
	
	public final void write(int dataBuf) throws IOException {
		tmpByteBuf[0] = (byte) dataBuf;
		this.write(tmpByteBuf);
	}
	
	public final void write(byte[] dataBuf) throws IOException {
		this.write(dataBuf, 0, dataBuf.length);
	}

	public final void write(byte[] dataBuf, int off, int len) throws IOException {
		try {
			if (dataBuf == null) {
				throw new NullPointerException();
			} else if ((off < 0) || (off > dataBuf.length) || (len < 0) || ((off + len) > dataBuf.length) || ((off + len) < 0)) {
				throw new IndexOutOfBoundsException();
			} else if (!open) {
				throw new IOException("stream closed");
			} else if (len == 0) {
				return;
			}

			while (len > 0 && completePurge() > 0) {
				int bufferRemaining = Math.min(len, internalBuf.remaining());
				internalBuf.put(dataBuf, off, bufferRemaining);
				off += bufferRemaining;
				len -= bufferRemaining;
				position += bufferRemaining;
				purgeIfFull();				
			}
		} catch (Exception e) {
			throw new IOException(e);
		}
	}	
	
	public final void write(ByteBuffer dataBuf) throws IOException {
		try {
			if (dataBuf == null) {
				throw new NullPointerException();
			} else if (!open) {
				throw new IOException("stream closed");
			} else if (dataBuf.remaining() == 0) {
				return;
			}			
			
			int len = dataBuf.remaining();
			while (len > 0 && completePurge() > 0) {
				int bufferRemaining = Math.min(len, internalBuf.remaining());
				int oldLimit = dataBuf.limit();
				dataBuf.limit(dataBuf.position() + bufferRemaining);
				internalBuf.put(dataBuf);
				dataBuf.limit(oldLimit);
				len -= bufferRemaining;
				position += bufferRemaining;
				purgeIfFull();
			}
		} catch (Exception e) {
			throw new IOException(e);
		}
	}
	
	public Future<CrailResult> purge() throws IOException {
		if (!open) {
			throw new IOException("stream closed");
		} 		
		
		try {
			if (future == null && internalBuf.position() > 0) {
				internalBuf.flip();
				future = outputStream().write(internalBuf);
				internalBuf.clear();
				return future;
			} else if (internalBuf.position() == 0){
				future = noOp;
			} 
			return future;
		} catch(Exception e){
			throw new IOException(e);
		}
	}	
	
	public Future<Void> sync() throws IOException {
		Future<Void> future = outputStream().sync();
		return future;
	}

	public void close() throws IOException {
		try {
			if (!open){
				return;
			}
			
			completePurge();
			future = purge();
			completePurge();
			outputStream().close();
			crailFS.freeBuffer(internalBuf);
			internalBuf = null;
			open = false;
		} catch (Exception e) {
			throw new IOException(e);
		} 
	}
	
	public long position() {
		return position;
	}

	//---------------------- ByteBuffer interface
	
	private int completePurge() throws IOException {
		try {
			if (future != null){
				future.get();
				internalBuf.clear();
				future = null;
			}
			return internalBuf.remaining();
		} catch(Exception e){
			throw new IOException(e);
		}
	}

	private void purgeIfFull() throws IOException {
		if (internalBuf.remaining() == 0){
			purge();
		}
	}

	private final void writeDouble(double value) throws Exception {
		if (internalBuf.remaining() >= 4){
			internalBuf.putDouble(value);
			purgeIfFull();
		} else {
			tmpBoundaryBuffer.clear();
			tmpBoundaryBuffer.putDouble(value);
			write(tmpBoundaryBuffer);
		}
	}
	
	private final void writeInt(int value) throws Exception {
		if (internalBuf.remaining() >= 4){
			internalBuf.putInt(value);
			purgeIfFull();
		} else {
			tmpBoundaryBuffer.clear();
			tmpBoundaryBuffer.putInt(value);
			write(tmpBoundaryBuffer);
		}		
	}
	
	private final void writeLong(long value) throws Exception {
		if (internalBuf.remaining() >= 8){
			internalBuf.putLong(value);
			purgeIfFull();			
		} else {
			tmpBoundaryBuffer.clear();
			tmpBoundaryBuffer.putLong(value);
			write(tmpBoundaryBuffer);
		}			
	}
	
	private final void writeShort(short value) throws Exception {
		if (internalBuf.remaining() >= 2){
			internalBuf.putShort(value);
			purgeIfFull();		
		} else {
			tmpBoundaryBuffer.clear();
			tmpBoundaryBuffer.putShort(value);
			write(tmpBoundaryBuffer);
		}			
	}	
	
	private final CrailOutputStream outputStream() throws IOException {
		if (outputStream == null){
			try {
				outputStream = file.getDirectOutputStream(writeHint);
			} catch(Exception e){
				throw new IOException(e);
			}
		}
		return outputStream;
	}	

}
