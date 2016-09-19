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
import java.util.Iterator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;

import com.ibm.crail.conf.CrailConstants;
import com.ibm.crail.utils.CrailUtils;

public class CrailMultiStream extends InputStream implements CrailInputStream {
	private static final Logger LOG = CrailUtils.getLogger();
	
	private LinkedBlockingQueue<CrailInputStream> streams;
	private LinkedBlockingQueue<ByteBuffer> buffers;
	private LinkedBlockingQueue<CrailInputStream> streamQueue;
	private LinkedBlockingQueue<ByteBuffer> bufferQueue;
	private LinkedBlockingQueue<Future<CrailResult>> futureQueue;
	
	private CrailFS fs;
	private ByteBuffer currentBuffer;
	private CrailInputStream currentStream;
	private byte[] tmpBuf;
	private long virtualCapacity;
	private long virtualPosition;
	private boolean isClosed;
	
	//stats
	private long closeAttempts;
	private long totalReads;
	private long totalBlocks;
	private long totalNonBlocks;
	
	public CrailMultiStream(CrailFS fs, Iterator<String> paths, int outstanding) throws Exception{
		this.virtualPosition = 0;
		this.virtualCapacity = 0;
		this.currentBuffer = null;
		this.streams = new LinkedBlockingQueue<CrailInputStream>();
		this.streamQueue = new LinkedBlockingQueue<CrailInputStream>();
		this.bufferQueue = new LinkedBlockingQueue<ByteBuffer>();
		this.futureQueue = new LinkedBlockingQueue<Future<CrailResult>>();
		this.buffers = new LinkedBlockingQueue<ByteBuffer>();
		this.isClosed = false;
		this.currentStream = null;
		this.closeAttempts = 0;
		this.totalReads = 0;
		this.totalBlocks = 0;
		this.totalNonBlocks = 0;	
		this.fs = fs;
		
		int i = 0;
		while(paths.hasNext()){
			String path = paths.next();
			CrailFile file = null;
			try {
				file = fs.lookupFile(path, false).get();
			} catch(Exception e){
				throw new Exception("File not found, name " + path + ", exc: " + e.getMessage());
			}
			if (file == null){
				throw new Exception("File not found, name " + path);
			}
			CrailInputStream stream = file.getDirectInputStream(file.getCapacity());
			this.virtualCapacity += file.getCapacity();
			this.streamQueue.add(stream);
			this.streams.add(stream);
			
			if (currentStream == null){
				currentStream = this.streamQueue.poll();
			}
			
			if (i < outstanding){
				ByteBuffer buffer = fs.allocateBuffer();
				if (triggerRead(buffer)){
					buffers.add(buffer);
					i++;
				} else {
					fs.freeBuffer(buffer);
				}				
			}
		}
		
		for (; i < outstanding; i++){
			ByteBuffer buffer = fs.allocateBuffer();
			if (triggerRead(buffer)){
				buffers.add(buffer);
			} else {
				fs.freeBuffer(buffer);
				break;
			}
		}
		
		if (CrailConstants.DEBUG){
			LOG.info("multistream, init, streams " + this.streamQueue.size() + ", usedBuffers " + bufferQueue.size() + ", virtualCapacity " + virtualCapacity);
		}
	}	
	
	public final synchronized int read() throws IOException {
		int ret = read(tmpBuf);
		return (ret <= 0) ? -1 : (tmpBuf[0] & 0xff);
	}
	
    public final synchronized int read(byte b[]) throws IOException {
        return read(b, 0, b.length);
    }	

	public final synchronized int read(byte[] buf, int off, int len)
			throws IOException {
		try {
			if (buf == null) {
				throw new NullPointerException();
			} else if (off < 0 || len < 0 || len > buf.length - off) {
				throw new IndexOutOfBoundsException();
			} else if (len == 0) {
				return 0;
			} else if (isClosed){
				return -1;
			}
		
			if (currentBuffer == null){
				currentBuffer = getNextBuffer();
			}			
			
			int sumLen = 0;
			while (currentBuffer != null && len > 0) {
				if (currentBuffer.remaining() == 0) {
					triggerRead(currentBuffer);
					currentBuffer = getNextBuffer();
				} else {
					int bufferRemaining = Math.min(len, currentBuffer.remaining());
					currentBuffer.get(buf, off, bufferRemaining);
					len -= bufferRemaining;
					off += bufferRemaining;
					sumLen += bufferRemaining;
				}
			}

			int totalBytesRead = sumLen > 0 ? sumLen : -1;
			if (totalBytesRead > 0){
				virtualPosition += totalBytesRead;
			}
			return totalBytesRead;
		} catch (Exception e) {
			e.printStackTrace();
			throw new IOException(e);
		}
	}
	
	public final synchronized int read(ByteBuffer buffer)
			throws IOException {
		try {
			if (buffer == null) {
				throw new NullPointerException();
			} else if (buffer.remaining() == 0) {
				return 0;
			} else if (isClosed){
				return -1;
			}
		
			if (currentBuffer == null){
				currentBuffer = getNextBuffer();
			}			
			
			int sumLen = 0;
			while (currentBuffer != null && buffer.remaining() > 0) {
				if (currentBuffer.remaining() == 0) {
					triggerRead(currentBuffer);
					currentBuffer = getNextBuffer();
				} else {
					int bufferRemaining = Math.min(buffer.remaining(), currentBuffer.remaining());
					int oldLimit = currentBuffer.limit();
					currentBuffer.limit(currentBuffer.position() + bufferRemaining);
					buffer.put(currentBuffer);
					currentBuffer.limit(oldLimit);					
					sumLen += bufferRemaining;
				}
			}

			int totalBytesRead = sumLen > 0 ? sumLen : -1;
			if (totalBytesRead > 0){
				virtualPosition += totalBytesRead;
			}
			return totalBytesRead;
		} catch (Exception e) {
			e.printStackTrace();
			throw new IOException(e);
		}
	}
	
	@Override
	public Future<CrailResult> readAsync(ByteBuffer dataBuf) throws Exception {
		throw new Exception("async operation not implemented!");
	}	

	@Override
	public final synchronized void close() throws IOException {
		this.closeAttempts++;
		
		if (isClosed){
			return;
		}
		
//		LOG.info("closing multistream");
		try {
			ByteBuffer buffer = getNextBuffer();
			while(buffer != null){
				buffer = getNextBuffer();
			}
		} catch(Exception e){
//			LOG.info("ERROR " + e.getMessage());
		}
		
		try {
			ByteBuffer buffer = buffers.poll();
			while (buffer != null){
				fs.freeBuffer(buffer);
				buffer = buffers.poll();
			}
		} catch(Exception e){
//			LOG.info("ERROR " + e.getMessage());
		}
			
		try {
			CrailInputStream stream = streams.poll();
			while(stream != null){
				stream.close();
				stream = streams.poll();
			}
		} catch(Exception e){
//			LOG.info("ERROR " + e.getMessage());
		}
		
		this.isClosed = true;
	}

	public final synchronized int available() {
		long diff = virtualCapacity - virtualPosition;
		return (int) diff;
	}
	
	private ByteBuffer getNextBuffer() throws InterruptedException, ExecutionException{
		if (!futureQueue.isEmpty()){
			Future<CrailResult> future = futureQueue.poll();
			if (future.isDone()){
				this.totalNonBlocks++;
			} else {
				this.totalBlocks++;
			}
			future.get();
			currentBuffer = bufferQueue.poll();
			currentBuffer.flip();
			return currentBuffer;
		} else {
			return null;
		}
	}
	
	private boolean triggerRead(ByteBuffer buffer) throws Exception{
		buffer.clear();
		while(currentStream != null){
			Future<CrailResult> future = currentStream.readAsync(buffer);
			if (future != null){
				this.totalReads++;
//				LOG.info("reading new buffer and pushing future to queue");
				bufferQueue.add(buffer);
				futureQueue.add(future);
				return true;
			} else {
//				LOG.info("skipping stream, fd " + currentStream.getFd() + ", streams " + streamQueue.size());
				currentStream = streamQueue.poll();
			}
		}	
		return false;
	}

	public long getCloseAttempts() {
		return closeAttempts;
	}

	public long getTotalReads() {
		return totalReads;
	}

	public long getTotalBlocks() {
		return totalBlocks;
	}

	public long getTotalNonBlocks() {
		return totalNonBlocks;
	}

	public long getCapacity() {
		return virtualCapacity;
	}

	public long getPos() {
		return virtualPosition;
	}

	@Override
	public void seek(long pos) throws IOException {
		throw new IOException("operation not implemented!");
	}

	@Override
	public boolean isOpen() {
		return true;
	}

	@Override
	public long position() {
		return virtualPosition;
	}

	@Override
	public long getReadHint() {
		return 0;
	}
}
