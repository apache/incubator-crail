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
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.Future;

import org.slf4j.Logger;

import com.ibm.crail.conf.CrailConstants;
import com.ibm.crail.utils.CrailUtils;

public class CrailBufferedInputStream extends InputStream {
	private static final Logger LOG = CrailUtils.getLogger();
	
	private CrailFS crailFS;
	private CrailInputStream inputStream;
	private byte[] tmpByteBuf;
	private ByteBuffer tmpBoundaryBuffer;
	private ByteBuffer internalBuf;	
	private Future<CrailResult> future;
	private long position;
	private boolean open;
	
	CrailBufferedInputStream(CrailFile file, long readHint) throws Exception {
		this.crailFS = file.getFileSystem();
		this.inputStream = file.getDirectInputStream(readHint);
		this.position = 0;
		this.tmpByteBuf = new byte[1];
		this.tmpBoundaryBuffer = ByteBuffer.allocate(8);
		this.internalBuf = crailFS.allocateBuffer();
		this.future = null;
		this.internalBuf.clear().flip();
		triggerFetch();
		this.open = true;
	}
	
	public final int read() throws IOException {
		int ret = read(tmpByteBuf);
		return (ret <= 0) ? -1 : (tmpByteBuf[0] & 0xff);
	}
	
    public final int read(byte b[]) throws IOException {
        return read(b, 0, b.length);
    }	

	public final int read(long position, byte[] buffer, int offset, int length) throws IOException {
		long oldPos = position();
		int nread = -1;
		try {
			seek(position);
			nread = read(buffer, offset, length);
		} finally {
			seek(oldPos);
		}
		return nread;
	}
	
	@Override
	public final int read(byte[] buf, int off, int len)
			throws IOException {
		try {
			if (buf == null) {
				throw new NullPointerException();
			} else if (off < 0 || len < 0 || len > buf.length - off) {
				throw new IndexOutOfBoundsException("off " + off + ", len " + len + ", length " + buf.length);
			} else if (!open) { 
				throw new IOException("strem closed");
			} else if (len == 0) {
				return 0;
			}

			int sumLen = 0;
			while (len > 0 && completeFetch() > 0) {
				int bufferRemaining = Math.min(len, internalBuf.remaining());
				internalBuf.get(buf, off, bufferRemaining);
				len -= bufferRemaining;
				off += bufferRemaining;
				sumLen += bufferRemaining;		
				position += bufferRemaining;
				triggerFetch();
			}		
			return sumLen > 0 ? sumLen : -1;
		} catch (Exception e) {
			e.printStackTrace();
			throw new IOException(e);
		}
	}
	
	public final int read(ByteBuffer dataBuf) throws IOException {
		try {
			if (dataBuf == null) {
				throw new NullPointerException();
			} else if (!open) { 
				throw new IOException("strem closed");
			} else if (dataBuf.remaining() == 0) {
				return 0;
			}

			int len = dataBuf.remaining();
			int sumLen = 0;
			while (len > 0 && completeFetch() > 0) {
				int bufferRemaining = Math.min(len, internalBuf.remaining());
				int oldLimit = internalBuf.limit();
				internalBuf.limit(internalBuf.position() + bufferRemaining);
				dataBuf.put(internalBuf);
				internalBuf.limit(oldLimit);
				len -= bufferRemaining;
				sumLen += bufferRemaining;	
				position += bufferRemaining;
				triggerFetch();
			}
			return sumLen > 0 ? sumLen : -1;			
		} catch (Exception e) {
			throw new IOException(e);
		}
		
	}
	
	@Override
	public void close() throws IOException {
		try {
			if (!open){
				return;
			}
			
			completeFetch();
			inputStream.close();
			crailFS.freeBuffer(internalBuf);
			internalBuf = null;
			open = false;
		} catch (Exception e) {
			throw new IOException(e);
		}
	}
	
	//--------------------------------------

	@Override
	public long skip(long n) throws IOException {
		if (n <= 0) {
			return 0;
		}

		long oldPos = position();
		this.seek(oldPos + n);
		long newPos = position();

		if (newPos >= oldPos) {
			return newPos - oldPos;
		} else {
			throw new IOException("Error in skip operation");
		}
	}

	public void seek(long pos) throws IOException {
		if (pos == position){
			return;
		}
		
		completeFetch();
		long skip = pos - position;
		if (Math.abs(skip) < Integer.MAX_VALUE){
			int bufferPosition = internalBuf.position() + (int) skip;
			if (bufferPosition > 0 && bufferPosition < internalBuf.limit()){
				internalBuf.position(bufferPosition);
				this.position = pos;
				return;
			}
		}
		
		long startOffset = CrailUtils.blockStartAddress(pos);
		long offset = pos % (long) CrailConstants.BUFFER_SIZE;
		inputStream.seek(startOffset);
		internalBuf.clear().flip();
		triggerFetch();
		completeFetch();
		internalBuf.position((int) offset);
		this.position = pos;
	}


	public int available() {
		try {
			if (future != null){
				return (int) (future.isDone() ? future.get().getLen() : 0);
			} else {
				return internalBuf.remaining();
			}
		} catch(Exception e){
			return -1;
		}
	}

	public long position() {
		return position;
	}

	//---------------------- ByteBuffer interface 
	
	private void triggerFetch() throws IOException {
		try {
			if (future == null && internalBuf.remaining() == 0){
				internalBuf.clear();
				future = inputStream.read(internalBuf);
				if (future == null){
					internalBuf.clear().flip();
				}
			}
		} catch(Exception e){
			throw new IOException(e);
		}
	}

	private int completeFetch() throws IOException {
		try {
			if (future != null){
				future.get();
				internalBuf.flip();
				future = null;
			}
			return internalBuf.remaining();
		} catch(Exception e){
			throw new IOException(e);
		}
	}

	private final double readDouble() throws Exception {
		if (internalBuf.remaining() >= 4){
			return internalBuf.getDouble();
		} else {
			tmpBoundaryBuffer.clear();
			tmpBoundaryBuffer.limit(4);
			read(tmpBoundaryBuffer);
			return tmpBoundaryBuffer.getDouble();
		}
	}
	
	private final int readInt() throws Exception {
		if (internalBuf.remaining() >= 4){
			return internalBuf.getInt();
		} else {
			tmpBoundaryBuffer.clear();
			tmpBoundaryBuffer.limit(4);
			read(tmpBoundaryBuffer);
			return tmpBoundaryBuffer.getInt();
		}
	}
	
	private final double readLong() throws Exception {
		if (internalBuf.remaining() >= 8){
			return internalBuf.getLong();
		} else {
			tmpBoundaryBuffer.clear();
			tmpBoundaryBuffer.limit(8);
			read(tmpBoundaryBuffer);
			return tmpBoundaryBuffer.getLong();
		}
	}
	
	private final double readShort() throws Exception {
		if (internalBuf.remaining() >= 2){
			return internalBuf.getShort();
		} else {
			tmpBoundaryBuffer.clear();
			tmpBoundaryBuffer.limit(2);
			read(tmpBoundaryBuffer);
			return tmpBoundaryBuffer.getShort();
		}
	}	
}
