/*
 * Copyright (C) 2015-2018, IBM Corporation
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

package org.apache.crail.storage.object;

import io.netty.buffer.ByteBuf;
import org.apache.crail.CrailBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class ObjectStoreUtils {
	private static Logger LOG = getLogger();

	private ObjectStoreUtils() {
	}

	public static synchronized Logger getLogger() {
		if (LOG == null) {
			LOG = LoggerFactory.getLogger("org.apache.crail.storage.object");
		}
		return LOG;
	}

	public static int readStreamIntoHeapByteBuffer(InputStream src, ByteBuffer dst)	throws IOException {
		int readBytes = 0, writtenBytes = 0;
		int startPos = dst.position();
		int endPos = dst.limit();
		byte[] array = dst.array();
		try {
			while ((readBytes = src.read(array, writtenBytes, endPos - writtenBytes)) != -1) {
				writtenBytes += readBytes;
			}
		} catch (IOException e) {
			// this could happen if the block size is reduced without deleting existing objects
			LOG.error("Got exception while trying to write into ByteBuffer " + dst + ": ", e);
			LOG.error("Buffer start = {}, pos = {}, capacity = {}, written bytes = {}, read bytes = {}",
					startPos, dst.position(), dst.capacity(), writtenBytes, readBytes);
			LOG.error("Read truncated to {} bytes", writtenBytes);
			throw e;
		}

		LOG.debug("Written {} bytes to ByteBuffer range ({} - {})", writtenBytes, startPos, dst.position());
		return writtenBytes;
	}

	public static int readStreamIntoDirectByteBuffer(InputStream src, byte stagingBuffer[], CrailBuffer dst)
			throws IOException {
		int readBytes, writtenBytes = 0;
		int startPos = dst.position();
		long t1 = 0, t2;
		if (ObjectStoreConstants.PROFILE) {
			t1 = System.nanoTime();
		}
		while ((readBytes = src.read(stagingBuffer)) != -1) {
			try {
				dst.put(stagingBuffer, 0, readBytes);
			} catch (java.nio.BufferOverflowException e) {
				// this could happen if the block size is reduced without deleting existing objects
				LOG.error("Got exception while trying to write into ByteBuffer " + dst + ": ", e);
				LOG.error("Buffer start = {}, pos = {}, capacity = {}, written bytes = {}, read bytes = {}",
						startPos, dst.position(), dst.capacity(), writtenBytes, readBytes);
				LOG.error("Read truncated to {} bytes", writtenBytes);
				break;
			}
			if (ObjectStoreConstants.PROFILE) {
				t2 = System.nanoTime();
				LOG.debug("Read {} bytes into output ByteBuffer, Latency={} (us)", readBytes, (t2 - t1) / 1000.);
				t1 = t2;
			}
			writtenBytes += readBytes;
		}
		LOG.debug("Written {} bytes to ByteBuffer range ({} - {})", writtenBytes, startPos, dst.position());
		return writtenBytes;
	}

	public static void putZeroes(CrailBuffer buf, int count) {
		while (count > 0) {
			buf.putInt(0);
			count -= 4;
		}
	}

	public static void showByteBufferContent(ByteBuffer buf, int offset, int bytes) {
		// dump the content of first bytes from the payload
		if (buf != null) {
			LOG.debug("DUMP: TID:" + Thread.currentThread().getId() + " NioByteBuffer : " + buf);
			int min = (buf.limit() - offset);
			if (min > bytes)
				min = bytes;
			String str = "DUMP: TID:" + Thread.currentThread().getId() + " DUMP (" + offset + " ,+" + min + ") : ";
			min += offset;
			for (int i = offset; i < min; i++) {
				//str += Character.toHexString();
				str += Byte.toString(buf.get(i)) + " : ";
				if (i % 32 == 0)
					str += "\n";
			}
			LOG.debug(str);
		} else {
			LOG.debug("DUMP : payload content is NULL");
		}
	}

	public static void showByteBufContent(ByteBuf buf, int offset, int bytes) {
		// dump the content of first bytes from the payload
		if (buf != null) {
			int ori_rindex = buf.readerIndex();
			LOG.debug("DUMP: TID:" + Thread.currentThread().getId() + " NettyByteBuf : " + buf);
			int min = (buf.capacity() - offset);
			if (min > bytes)
				min = bytes;
			String str = "DUMP: TID:" + Thread.currentThread().getId() + " DUMP (" + offset + " ,+" + min + ") : ";
			min += offset;
			for (int i = offset; i < min; i++) {
				//str += Character.toHexString();
				str += Byte.toString(buf.getByte(i)) + " : ";
				if (i % 32 == 0)
					str += "\n";
			}
			LOG.debug(str);
			buf.readerIndex(ori_rindex);
		} else {
			LOG.debug("DUMP : payload content is NULL");
		}
	}

	public static class ByteBufferBackedInputStream extends InputStream {
		private final CrailBuffer buf;
		//private int mark;
		//private int readlimit;

		public ByteBufferBackedInputStream(CrailBuffer buf) {
			LOG.debug("New buffer");
			this.buf = buf;
		}

		@Override
		public int read() {
			byte[] tmppbuf = new byte[1];
			if (!buf.hasRemaining()) {
				return -1;
			}
			int ret = read(tmppbuf);
			return (ret <= 0) ? -1 : (tmppbuf[0] & 0xff);
		}

		@Override
		public int read(byte[] bytes) {
			if (!buf.hasRemaining()) {
				return -1;
			}
			int initialPos = buf.position();
			buf.get(bytes);
			int finalPos = buf.position();
			int len = finalPos - initialPos;
			return len;
		}

		@Override
		public int read(byte[] bytes, int off, int len) {
			if (!buf.hasRemaining()) {
				return -1;
			}
			len = Math.min(len, buf.remaining());
			buf.get(bytes, off, len);
			return len;
		}

		@Override
		public long skip(long n) {
			int step = Math.min((int) n, buf.remaining());
			this.buf.position(buf.position() + step);
			return step;
		}

		@Override
		public int available() {
			return buf.remaining();
		}

		@Override
		public void mark(int readlimit) {
			//this.mark = this.buf.position();
			//this.readlimit = readlimit;
			//buf.mark();
		}

		@Override
		public void reset() {
			//buf.position(this.mark);
			//buf.reset();
		}

		@Override
		public boolean markSupported() {
			return true;
		}
	}
}
