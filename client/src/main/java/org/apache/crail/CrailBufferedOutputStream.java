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

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.concurrent.Future;

import org.apache.crail.conf.CrailConstants;
import org.apache.crail.utils.CrailImmediateOperation;
import org.apache.crail.utils.CrailUtils;
import org.apache.crail.utils.RingBuffer;
import org.slf4j.Logger;


public class CrailBufferedOutputStream extends OutputStream {
	public static final Logger LOG = CrailUtils.getLogger();

	private CrailStore crailFS;
	private CrailFile file;
	private long writeHint;
	private CrailOutputStream outputStream;
	private LinkedList<CrailBuffer> originalBuffers;
	private RingBuffer<CrailBuffer> readySlices;
	private RingBuffer<CrailBuffer> pendingSlices;
	private RingBuffer<Future<CrailResult>> pendingFutures;
	private long position;
	private boolean open;
	private CrailBufferedStatistics statistics;
	private int actualSliceSize;

	private CrailImmediateOperation noOp;
	private ByteBuffer tmpBoundaryBuffer;
	private byte[] tmpByteBuf;

	CrailBufferedOutputStream(CrailFile file, long writeHint) throws Exception {
		this.crailFS = file.getFileSystem();
		this.file = file;
		this.writeHint = writeHint;
		this.outputStream = null;
		this.statistics = new CrailBufferedStatistics("buffered/out");

		int allocationSize = Math.max(CrailConstants.BUFFER_SIZE, CrailConstants.SLICE_SIZE);
		this.actualSliceSize = Math.min(CrailConstants.BUFFER_SIZE, CrailConstants.SLICE_SIZE);
		int sliceCount = allocationSize / actualSliceSize;
		this.originalBuffers = new LinkedList<>();
		this.readySlices = new RingBuffer<>(sliceCount);
		this.pendingSlices = new RingBuffer<>(sliceCount);
		this.pendingFutures = new RingBuffer<>(sliceCount);

		for (int currentSize = 0; currentSize < allocationSize; currentSize += CrailConstants.BUFFER_SIZE){
			CrailBuffer buffer = crailFS.allocateBuffer();
			originalBuffers.add(buffer);
		}
		for (CrailBuffer buffer : originalBuffers){
			while(buffer.hasRemaining()){
				buffer.limit(buffer.position() + actualSliceSize);
				CrailBuffer slice = buffer.slice();
				slice.clear();
				readySlices.add(slice);

				int newpos = buffer.position() + actualSliceSize;
				buffer.clear();
				buffer.position(newpos);
			}
		}
		/* adjust first slice remaining to align position to slice size */
		CrailBuffer firstSlice = readySlices.peek();
		long streamPosition = outputStream().position();
		firstSlice.limit(firstSlice.remaining() - (int)(streamPosition % firstSlice.remaining()));

		this.tmpByteBuf = new byte[1];
		this.tmpBoundaryBuffer = ByteBuffer.allocate(8);
		this.noOp = new CrailImmediateOperation(0);
		this.position = 0;
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

			while (len > 0){
				CrailBuffer slice = getSlice();
				int bufferRemaining = Math.min(len, slice.remaining());
				slice.put(dataBuf, off, bufferRemaining);
				off += bufferRemaining;
				len -= bufferRemaining;
				position += bufferRemaining;
				syncSlice();
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
			while (len > 0) {
				CrailBuffer slice = getSlice();
				int bufferRemaining = Math.min(len, slice.remaining());
				int oldLimit = dataBuf.limit();
				dataBuf.limit(dataBuf.position() + bufferRemaining);
				slice.put(dataBuf);
				dataBuf.limit(oldLimit);
				len -= bufferRemaining;
				position += bufferRemaining;
				syncSlice();
			}
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	public final void writeDouble(double value) throws Exception {
		CrailBuffer slice = getSlice();
		if (slice.remaining() >= Double.BYTES){
			slice.putDouble(value);
			syncSlice();
			position += Double.BYTES;
		} else {
			tmpBoundaryBuffer.clear();
			tmpBoundaryBuffer.putDouble(value);
			tmpBoundaryBuffer.flip();
			write(tmpBoundaryBuffer);
		}
	}

	public final void writeFloat(float value) throws Exception {
		CrailBuffer slice = getSlice();
		if (slice.remaining() >= Float.BYTES){
			slice.putFloat(value);
			syncSlice();
			position += Float.BYTES;
		} else {
			tmpBoundaryBuffer.clear();
			tmpBoundaryBuffer.putFloat(value);
			tmpBoundaryBuffer.flip();
			write(tmpBoundaryBuffer);
		}
	}

	public final void writeInt(int value) throws Exception {
		CrailBuffer slice = getSlice();
		if (slice.remaining() >= Integer.BYTES){
			slice.putInt(value);
			syncSlice();
			position += Integer.BYTES;
		} else {
			tmpBoundaryBuffer.clear();
			tmpBoundaryBuffer.putInt(value);
			tmpBoundaryBuffer.flip();
			write(tmpBoundaryBuffer);
		}
	}

	public final void writeLong(long value) throws Exception {
		CrailBuffer slice = getSlice();
		if (slice.remaining() >= Long.BYTES){
			slice.putLong(value);
			syncSlice();
			position += Long.BYTES;
		} else {
			tmpBoundaryBuffer.clear();
			tmpBoundaryBuffer.putLong(value);
			tmpBoundaryBuffer.flip();
			write(tmpBoundaryBuffer);
		}
	}

	public final void writeShort(short value) throws Exception {
		CrailBuffer slice = getSlice();
		if (slice.remaining() >= Short.BYTES){
			slice.putShort(value);
			syncSlice();
			position += Short.BYTES;
		} else {
			tmpBoundaryBuffer.clear();
			tmpBoundaryBuffer.putShort(value);
			tmpBoundaryBuffer.flip();
			write(tmpBoundaryBuffer);
		}
	}

	public Future<CrailResult> purge() throws IOException {
		if (!open) {
			throw new IOException("stream closed");
		}

		try {
			while(!readySlices.isEmpty()){
				CrailBuffer slice = readySlices.poll();
				if (slice.position() > 0){
					slice.flip();
					Future<CrailResult> future = outputStream().write(slice);
					pendingSlices.add(slice);
					pendingFutures.add(future);
				}
			}

			if (pendingFutures.isEmpty()){
				return noOp;
			} else {
				CrailPurgeOperation purgeOp = new CrailPurgeOperation();
				while(!pendingFutures.isEmpty()){
					Future<CrailResult> future = pendingFutures.poll();
					purgeOp.add(future);
				}
				return purgeOp;
			}
		} catch (Exception e) {
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

			while(!readySlices.isEmpty()){
				CrailBuffer slice = readySlices.poll();
				if (slice.position() > 0){
					slice.flip();
					Future<CrailResult> future = outputStream().write(slice);
					pendingSlices.add(slice);
					pendingFutures.add(future);
				}
			}

			while(!pendingFutures.isEmpty()){
				Future<CrailResult> future = pendingFutures.poll();
				future.get();
			}

			while(!originalBuffers.isEmpty()){
				CrailBuffer buffer = originalBuffers.remove();
				crailFS.freeBuffer(buffer);
			}

			outputStream().close();
			this.crailFS.getStatistics().addProvider(statistics);
			open = false;
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	public long position() {
		return position;
	}

	@Override
	public void flush() throws IOException {
		//flush should not be called, use purge instead!
	}

	public CrailNode getFile() throws IOException {
		return outputStream().getFile();
	}

	private CrailBuffer getSlice() throws Exception {
		CrailBuffer slice = readySlices.peek();
		if (slice == null){
			Future<CrailResult> future = pendingFutures.poll();
			statistics.incTotalOps();
			if (future.isDone()){
				statistics.incNonBlockingOps();
			} else {
				statistics.incBlockingOps();
			}
			future.get();
			slice = pendingSlices.poll();
			slice.clear();
			readySlices.add(slice);
		}
		return slice;
	}

	private void syncSlice() throws Exception {
		CrailBuffer slice = readySlices.peek();
		if (slice != null && slice.remaining() == 0){
			slice = readySlices.poll();
			slice.flip();
			Future<CrailResult> future = outputStream().write(slice);
			pendingSlices.add(slice);
			pendingFutures.add(future);
		}
	}

	final CrailOutputStream outputStream() throws IOException {
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
