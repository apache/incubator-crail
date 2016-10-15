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
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;

import com.ibm.crail.utils.CrailUtils;

public class CrailMultiStream extends InputStream {
	private static final Logger LOG = CrailUtils.getLogger();
	
	private CrailFS fs;
	private Iterator<String> paths;
	private int outstanding;
	
	//state
	private LinkedBlockingQueue<SubStream> streams;
	private long triggeredPosition;
	private long consumedPosition;
	private byte[] tmpByteBuf;
	private boolean isClosed;
	
	public CrailMultiStream(CrailFS fs, Iterator<String> paths, int outstanding, int files) throws Exception{
		this.fs = fs;
		this.paths = paths;
		this.outstanding = Math.min(outstanding, files);
		
		this.streams = new LinkedBlockingQueue<SubStream>();
		this.triggeredPosition = 0;
		this.consumedPosition = 0;
		this.tmpByteBuf = new byte[1];
		this.isClosed = false;
		
		for (int i = 0; i < this.outstanding; i++){
			SubStream substream = nextSubStream();
//			LOG.info("init adding multistream " + substream.toString());
			streams.add(substream);
		}
	}	
	
	public final synchronized int read() throws IOException {
		int ret = read(tmpByteBuf);
		return (ret <= 0) ? -1 : (tmpByteBuf[0] & 0xff);
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
		
			return 0;
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
			
			long streamPosition = consumedPosition;
			int bufferPosition = buffer.position();
			int bufferRemaining = buffer.remaining();
			int sum = 0;
//			LOG.info("starting read.., streamPosition " + streamPosition + ", bufferPosition " + bufferPosition + ", bufferRemaining " + bufferRemaining);
			while(sum < bufferRemaining && !streams.isEmpty()){
				SubStream subStream = streams.poll();
				if (subStream.available() > 0){
					int ret = subStream.read(streamPosition, buffer, bufferPosition, bufferRemaining);
					sum += ret;
				}
				if (subStream.isEnd()){
					subStream.close();
					subStream = nextSubStream();
					if (subStream != null){
//						LOG.info("init adding multistream " + subStream.toString());
						streams.add(subStream);
					}
				} else {
					streams.add(subStream);
				}
//				LOG.info("sum " + sum);
			}
			consumedPosition += bufferRemaining;
			return sum > 0 ? sum : -1;			
		} catch (Exception e) {
			e.printStackTrace();
			throw new IOException(e);
		}
	}
	
	@Override
	public final synchronized void close() throws IOException {
		if (isClosed){
			return;
		}
		
		this.isClosed = true;
	}

	public final synchronized int available() {
		return 0;
	}
	
	public void seek(long pos) throws IOException {
		throw new IOException("operation not implemented!");
	}

	public boolean isOpen() {
		return true;
	}

	private SubStream nextSubStream() throws Exception {
		SubStream substream = null;
		if (paths.hasNext()){
			String path = paths.next();
			CrailFile file = fs.lookupFile(path, false).get();
			if (file == null){
				throw new Exception("File not found, name " + path);
			}
			CrailBufferedInputStream stream = file.getBufferedInputStream(file.getCapacity());
			substream = new SubStream(file, stream, triggeredPosition);
			triggeredPosition += file.getCapacity();
		}
		return substream;
	}

	private static class SubStream {
		private CrailFile file;
		private CrailBufferedInputStream stream;
		private long multiStreamOffset;
		
		public SubStream(CrailFile file, CrailBufferedInputStream stream, long multiStreamOffset) {
			this.file = file;
			this.stream = stream;
			this.multiStreamOffset = multiStreamOffset;
		}

		public void close() throws IOException {
			stream.close();
		}

		public boolean isEnd() {
			return file.getCapacity() == stream.position();
		}

		public int read(long streamPosition, ByteBuffer buffer, int bufferPosition, int bufferRemaining) throws IOException {
			int bufferOffset = (int) (multiStreamOffset - streamPosition);
			if (bufferOffset >= 0 && bufferOffset < bufferRemaining){
				int bufferReadPosition = bufferPosition + bufferOffset;
				buffer.position(bufferReadPosition);
				int ret = stream.read(buffer);
				multiStreamOffset += ret;
				return ret;
			}
			return 0;
		}

		public int available() {
			return stream.available();
		}
	}
}
