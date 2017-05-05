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
import java.util.LinkedList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Future;

import org.slf4j.Logger;

import com.ibm.crail.conf.CrailConstants;
import com.ibm.crail.utils.CrailUtils;

public class CrailBufferedInputStream extends InputStream {
//	private static final Logger LOG = CrailUtils.getLogger();
	
	private CrailFS crailFS;
	private CrailInputStream inputStream;
	private byte[] tmpByteBuf;
	private ByteBuffer tmpBoundaryBuffer;
	private LinkedList<CrailBuffer> originalBuffers;
	private ArrayBlockingQueue<CrailBuffer> readySlices;
	private ArrayBlockingQueue<CrailBuffer> pendingSlices;
	private ArrayBlockingQueue<Future<CrailResult>> pendingFutures;	
	private ArrayBlockingQueue<CrailBuffer> freeSlices;
	private LinkedList<CrailBuffer> tmpSlices;
	private long position;
	private boolean open;
	private CrailBufferedStatistics statistics;
	private int actualSliceSize;
	private long capacity;
	private CrailFile file;
	
	CrailBufferedInputStream(CrailFile file, long readHint) throws Exception {
		this.file = file;
		this.capacity = file.getCapacity();
		this.crailFS = file.getFileSystem();
		this.inputStream = file.getDirectInputStream(readHint);
		this.position = 0;
		this.tmpByteBuf = new byte[1];
		this.tmpBoundaryBuffer = ByteBuffer.allocate(8);
		this.statistics = new CrailBufferedStatistics("buffered/in");
		
		int allocationSize = Math.max(CrailConstants.BUFFER_SIZE, CrailConstants.SLICE_SIZE);
		this.actualSliceSize = Math.min(CrailConstants.BUFFER_SIZE, CrailConstants.SLICE_SIZE);
		int sliceCount = allocationSize / actualSliceSize;
		this.originalBuffers = new LinkedList<CrailBuffer>();
		this.readySlices = new ArrayBlockingQueue<CrailBuffer>(sliceCount);
		this.pendingSlices = new ArrayBlockingQueue<CrailBuffer>(sliceCount);
		this.freeSlices = new ArrayBlockingQueue<CrailBuffer>(sliceCount);
		this.pendingFutures = new ArrayBlockingQueue<Future<CrailResult>>(sliceCount);
		this.tmpSlices = new LinkedList<CrailBuffer>();
		
		for (int currentSize = 0; currentSize < allocationSize; currentSize += CrailConstants.BUFFER_SIZE){
			CrailBuffer buffer = crailFS.allocateBuffer();
			originalBuffers.add(buffer);
		}
		for (CrailBuffer buffer : originalBuffers){
			while(buffer.hasRemaining()){
				buffer.limit(buffer.position() + actualSliceSize);
				CrailBuffer slice = buffer.slice();
				slice.clear();
				freeSlices.add(slice);
				
				int newpos = buffer.position() + actualSliceSize;
				buffer.clear();
				buffer.position(newpos);
			}			
		}
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
	public final int read(byte[] buf, int off, int len) throws IOException {
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
			while (len > 0) {
				CrailBuffer slice = getSlice(true);
				if (slice == null){
					break;
				}
				int bufferRemaining = Math.min(len, slice.remaining());
				slice.get(buf, off, bufferRemaining);
				len -= bufferRemaining;
				off += bufferRemaining;
				sumLen += bufferRemaining;		
				position += bufferRemaining;
				syncSlice();
			}	
			if (sumLen > 0){
				return sumLen;
			} else if (position < capacity){
				return 0;
			} else {
				return -1;
			}
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
			while (len > 0) {
				CrailBuffer slice = getSlice(true);
				if (slice == null){
					break;
				}				
				int bufferRemaining = Math.min(len, slice.remaining());
				int oldLimit = slice.limit();
				slice.limit(slice.position() + bufferRemaining);
				dataBuf.put(slice.getByteBuffer());
				slice.limit(oldLimit);
				len -= bufferRemaining;
				sumLen += bufferRemaining;	
				position += bufferRemaining;
				syncSlice();
			}
			if (sumLen > 0){
				return sumLen;
			} else if (position < capacity){
				return 0;
			} else {
				return -1;
			}			
		} catch (Exception e) {
			throw new IOException(e);
		}
		
	}
	
	public final double readDouble() throws Exception {
		CrailBuffer slice = getSlice(true);
		if (slice.remaining() >= Double.BYTES){
			double val = slice.getDouble();
			position += Double.BYTES;
			syncSlice();
			return val;
		} else {
			tmpBoundaryBuffer.clear();
			tmpBoundaryBuffer.limit(Double.BYTES);
			read(tmpBoundaryBuffer);
			tmpBoundaryBuffer.flip();
			return tmpBoundaryBuffer.getDouble();
		}
	}
	
	public final int readInt() throws Exception {
		CrailBuffer slice = getSlice(true);
		if (slice.remaining() >= Integer.BYTES){
			int val = slice.getInt();
			position += Integer.BYTES;
			syncSlice();
			return val;
		} else {
			tmpBoundaryBuffer.clear();
			tmpBoundaryBuffer.limit(Integer.BYTES);
			read(tmpBoundaryBuffer);
			tmpBoundaryBuffer.flip();
			return tmpBoundaryBuffer.getInt();
		}
	}
	
	public final double readLong() throws Exception {
		CrailBuffer slice = getSlice(true);
		if (slice.remaining() >= Long.BYTES){
			long val = slice.getLong();
			position += Long.BYTES;
			syncSlice();
			return val;
		} else {
			tmpBoundaryBuffer.clear();
			tmpBoundaryBuffer.limit(Long.BYTES);
			read(tmpBoundaryBuffer);
			tmpBoundaryBuffer.flip();
			return tmpBoundaryBuffer.getLong();
		}
	}
	
	public final double readShort() throws Exception {
		CrailBuffer slice = getSlice(true);
		if (slice.remaining() >= Short.BYTES){
			short val = slice.getShort();
			position += Short.BYTES;
			syncSlice();
			return val;
		} else {
			tmpBoundaryBuffer.clear();
			tmpBoundaryBuffer.limit(Short.BYTES);
			read(tmpBoundaryBuffer);
			tmpBoundaryBuffer.flip();
			return tmpBoundaryBuffer.getShort();
		}
	}		
	
	@Override
	public void close() throws IOException {
		try {
			if (!open){
				return;
			}
			
			while(!pendingFutures.isEmpty()){
				Future<CrailResult> future = pendingFutures.poll();
				future.get();
			}
			
			while(!originalBuffers.isEmpty()){
				CrailBuffer buffer = originalBuffers.remove();
				crailFS.freeBuffer(buffer);
			}
			
			inputStream.close();
			this.crailFS.getStatistics().addProvider(statistics);
			open = false;
		} catch (Exception e) {
			throw new IOException(e);
		}
	}
	
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
		try {
			if (pos >= capacity){
				return;
			} 
			if (pos == position){
				return;
			}
			
			long startPosition = CrailUtils.bufferStartAddress(position, actualSliceSize);
			long endPosition = startPosition + (readySlices.size() + pendingSlices.size())*actualSliceSize;
			if (pos >= startPosition && pos < endPosition){
				long currentPosition = startPosition;
				tmpSlices.clear();
				while(!freeSlices.isEmpty()){
					tmpSlices.add(freeSlices.poll());
				}
				while(!readySlices.isEmpty() && pos >= currentPosition + actualSliceSize){
					currentPosition += actualSliceSize;
					tmpSlices.add(readySlices.poll());
				}
				while(!pendingFutures.isEmpty() && pos >= currentPosition + actualSliceSize){
					Future<CrailResult> future = pendingFutures.poll();
					future.get();
					currentPosition += actualSliceSize;
					tmpSlices.add(pendingSlices.poll());
				}
				while(!tmpSlices.isEmpty()){
					triggerRead(tmpSlices.poll());
				}
				this.position = pos;				
				CrailBuffer slice = getSlice(true);
				long bufPosition = pos - currentPosition;
				slice.position((int) bufPosition);
			} else {
				long sliceStart = CrailUtils.bufferStartAddress(pos, actualSliceSize);
				inputStream.seek(sliceStart);
				tmpSlices.clear();
				while(!freeSlices.isEmpty()){
					tmpSlices.add(freeSlices.poll());
				}				
				while(!readySlices.isEmpty()){
					tmpSlices.add(readySlices.poll());
				}
				while(!pendingFutures.isEmpty()){
					Future<CrailResult> future = pendingFutures.poll();
					future.get();
					tmpSlices.add(pendingSlices.poll());
				}
				while(!tmpSlices.isEmpty()){
					triggerRead(tmpSlices.poll());
				}				
				this.position = pos;				
				CrailBuffer slice = getSlice(true);
				long bufPosition = pos - sliceStart;
				slice.position((int) bufPosition);
			}
		} catch(Exception e){
			e.printStackTrace();
			throw new IOException("position " + position + ", pos " + pos + ", free " + freeSlices.size() + ", ready " + readySlices.size() + ", pending " + pendingSlices.size() + ", capacity " + capacity + ", exception " + e);
		}
	}	

	public int available() {
		try {
			CrailBuffer buffer = getSlice(false);
//			LOG.info("available on path " + file.getPath() + ", inputStream.pos " + inputStream.position() + ", buffered.position " + this.position() + ", ready " + readySlices.size() + ", pending " + pendingSlices.size() + ", buffer " + buffer);
			if (buffer != null){
				return buffer.remaining();
			} else {
				return 0;
			}
		} catch(Exception e){
			return -1;
		}
	}

	public long position() {
		return position;
	}

	//---------------------- ByteBuffer interface 
	
	private CrailBuffer getSlice(boolean blocking) throws Exception {
		CrailBuffer slice = readySlices.peek();
		if (slice == null){
			Future<CrailResult> future = pendingFutures.peek();
			if (future == null){
				slice = freeSlices.poll();
				triggerRead(slice);
				future = pendingFutures.peek();
			}
			if (future != null){
				statistics.incTotalOps();
				if (blocking){
					future = pendingFutures.poll();
					future.get();
				}
				if (future.isDone()){
					future = pendingFutures.poll();
					statistics.incNonBlockingOps();
					slice = pendingSlices.poll();
					slice.flip();
					readySlices.add(slice);
				} else {
					slice = null;
				}					
			} else {
				slice = null;
			}
		} 
		return slice;		
	}

	private void syncSlice() throws Exception {
		CrailBuffer slice = readySlices.peek();
		if (slice != null && slice.remaining() == 0){
			slice = readySlices.poll();
			triggerRead(slice);
		}		
	}
	
	private void triggerRead(CrailBuffer slice) throws Exception {
		slice.clear();
		Future<CrailResult> future = inputStream.read(slice);
		if (future != null){
			pendingSlices.add(slice);
			pendingFutures.add(future);
		} else {
			freeSlices.add(slice);
		}
	}	
}
