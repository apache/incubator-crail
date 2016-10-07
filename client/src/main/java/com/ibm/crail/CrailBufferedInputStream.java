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
import java.util.concurrent.TimeUnit;
import com.ibm.crail.conf.CrailConstants;
import sun.nio.ch.DirectBuffer;

public class CrailBufferedInputStream extends InputStream implements CrailInputStream {
	private CrailInputStream inputStream;
	private ByteBuffer internalBuf;	
	private byte[] tmpByteBuf;
	private ByteBuffer tmpBoundaryBuffer;
	private CrailFS crailFS;
	
	public CrailBufferedInputStream(CrailFS crailFS, CrailInputStream inputStream) throws IOException {
		this.crailFS = crailFS;
		this.inputStream = inputStream;
		this.internalBuf = crailFS.allocateBuffer();
		this.internalBuf.clear();
		this.internalBuf.flip();
		this.tmpByteBuf = new byte[1];
		this.tmpBoundaryBuffer = ByteBuffer.allocate(8);
	}
	
	public final synchronized int read() throws IOException {
		int ret = read(tmpByteBuf);
		return (ret <= 0) ? -1 : (tmpByteBuf[0] & 0xff);
	}
	
    public final int read(byte b[]) throws IOException {
        return read(b, 0, b.length);
    }	

	public final synchronized int read(long position, byte[] buffer, int offset, int length) throws IOException {
		long oldPos = getPos();
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
	public final synchronized int read(byte[] buf, int off, int len)
			throws IOException {
		try {
			if (buf == null) {
				throw new NullPointerException();
			} else if (off < 0 || len < 0 || len > buf.length - off) {
				throw new IndexOutOfBoundsException();
			} else if (len == 0) {
				return 0;
			}
			
			int sumLen = 0;
			while (len > 0) {
				if (!fetchIfEmpty()){
					break;
				}
				int bufferRemaining = Math.min(len, internalBuf.remaining());
				internalBuf.get(buf, off, bufferRemaining);
				len -= bufferRemaining;
				off += bufferRemaining;
				sumLen += bufferRemaining;
			}

			int totalBytesRead = sumLen > 0 ? sumLen : -1;
			
			return totalBytesRead;
		} catch (Exception e) {
			e.printStackTrace();
			throw new IOException(e);
		}
	}
	
	public final synchronized int read(ByteBuffer dataBuf) throws IOException {
		try {
			long totalBytesRead = -1;
			if (dataBuf instanceof DirectBuffer) {
				Future<CrailResult> future = readAsync(dataBuf);
				if (future != null){
					totalBytesRead = future.get(CrailConstants.DATA_TIMEOUT, TimeUnit.MILLISECONDS).getLen();
				} 
			} else {
				int len = dataBuf.remaining();
				int sumLen = 0;
				while (len > 0) {
					if (!fetchIfEmpty()){
						break;
					}					
					int bufferRemaining = Math.min(len, internalBuf.remaining());
					int oldLimit = internalBuf.limit();
					internalBuf.limit(internalBuf.position() + bufferRemaining);
					dataBuf.put(internalBuf);
					internalBuf.limit(oldLimit);
					len -= bufferRemaining;
					sumLen += bufferRemaining;
				}

				if (sumLen > 0){
					totalBytesRead = sumLen;
				}
			}
			return (int) totalBytesRead;
		} catch (Exception e) {
			throw new IOException(e);
		}
		
	}
	
	public final synchronized Future<CrailResult> readAsync(ByteBuffer dataBuf) throws IOException {
		try {
			if (dataBuf.remaining() > 0 && internalBuf.remaining() > 0){
				int len = dataBuf.remaining();
				int bufferRemaining = Math.min(len, internalBuf.remaining());
				int oldLimit = internalBuf.limit();
				internalBuf.limit(internalBuf.position() + bufferRemaining);
				dataBuf.put(internalBuf);
				internalBuf.limit(oldLimit);				
			}
			Future<CrailResult> future = inputStream.readAsync(dataBuf);
			return future;
		} catch(Exception e){
			throw new IOException(e);
		}
	}
	
	private boolean fetchIfEmpty() throws IOException {
		try {
			if (internalBuf.remaining() != 0){
				return true;
			}
			
			internalBuf.clear();
			Future<CrailResult> future = inputStream.readAsync(internalBuf);
			if (future == null){
				internalBuf.position(internalBuf.limit());
				return false;
			}
			
			long ret = future.get(CrailConstants.DATA_TIMEOUT, TimeUnit.MILLISECONDS).getLen();
			if (ret > 0){
				internalBuf.flip();
			} else {
				internalBuf.position(internalBuf.limit());
				throw new IOException("not enough bytes read");
			}
			
			return true;
		} catch(Exception e){
			throw new IOException(e);
		}
	}
	
	@Override
	public synchronized void close() throws IOException {
		try {
			if (!inputStream.isOpen()){
				return;
			}			
			
			inputStream.close();
			crailFS.freeBuffer(internalBuf);
		} catch (Exception e) {
			throw new IOException(e);
		}
	}
	
	//--------------------------------------

	@Override
	public synchronized long skip(long n) throws IOException {
		if (n <= 0) {
			return 0;
		}

		long oldPos = inputStream.position();
		this.seek(oldPos + n);
		long newPos = inputStream.position();

		if (newPos >= oldPos) {
			return newPos - oldPos;
		} else {
			throw new IOException("Error in skip operation");
		}
	}

	@Override
	public synchronized void reset() throws IOException {
		this.seek(0);
	}
	
	public synchronized void seek(long pos) throws IOException {
		long oldPos = inputStream.position();
		inputStream.seek(pos);
		long newPos = inputStream.position();
		if (oldPos < newPos) {
			long skipped = newPos - oldPos;
			if (skipped < internalBuf.remaining()) {
				int _skipped = (int) skipped;
				internalBuf.position(internalBuf.position() + _skipped);
			} else {
				internalBuf.clear();
				internalBuf.flip();
			}
		} else if (oldPos > newPos) {
			long removed = oldPos - newPos;
			if (removed < internalBuf.position()) {
				int _removed = (int) removed;
				internalBuf.position(internalBuf.position() - _removed);
			} else {
				internalBuf.clear();
				internalBuf.flip();
			}
		}
	}


	public synchronized int available() {
		return inputStream.available();
	}

	public synchronized boolean seekToNewSource(long targetPos) throws IOException {
		return false;
	}


	public boolean markSupported() {
		return false;
	}

	public synchronized void mark(int readlimit) {
	}

	public synchronized long getPos() {
		return inputStream.position();
	}
	
	public void setReadahead(Long readahead) throws IOException,
			UnsupportedOperationException {
	}	
	
	@Override
	public boolean isOpen() {
		return inputStream.isOpen();
	}

	@Override
	public long position() {
		return inputStream.position();
	}

	@Override
	public long getReadHint() {
		return inputStream.getReadHint();
	}
	
	//---------------------- ByteBuffer interface 
	
	public final synchronized double readDouble() throws Exception {
		if (internalBuf.remaining() >= 4){
			return internalBuf.getDouble();
		} else {
			tmpBoundaryBuffer.clear();
			tmpBoundaryBuffer.limit(4);
			read(tmpBoundaryBuffer);
			return tmpBoundaryBuffer.getDouble();
		}
	}
	
	public final synchronized int readInt() throws Exception {
		if (internalBuf.remaining() >= 4){
			return internalBuf.getInt();
		} else {
			tmpBoundaryBuffer.clear();
			tmpBoundaryBuffer.limit(4);
			read(tmpBoundaryBuffer);
			return tmpBoundaryBuffer.getInt();
		}
	}
	
	public final synchronized double readLong() throws Exception {
		if (internalBuf.remaining() >= 8){
			return internalBuf.getLong();
		} else {
			tmpBoundaryBuffer.clear();
			tmpBoundaryBuffer.limit(8);
			read(tmpBoundaryBuffer);
			return tmpBoundaryBuffer.getLong();
		}
	}
	
	public final synchronized double readShort() throws Exception {
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
