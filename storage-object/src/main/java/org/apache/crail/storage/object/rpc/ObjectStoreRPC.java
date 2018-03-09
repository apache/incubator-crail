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

package org.apache.crail.storage.object.rpc;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import org.apache.crail.storage.object.ObjectStoreUtils;
import org.slf4j.Logger;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

public class ObjectStoreRPC {
	public final static short TranslateBlockCmd = 1;
	public final static short WriteBlockRangeCmd = 2;
	public final static short WriteBlockCmd = 3;
	public final static short LockBlockCmd = 4;
	public final static short UnmapBlockCmd = 5;
	private static final Logger LOG = ObjectStoreUtils.getLogger();

	public static RPCCall createObjectStoreRPC(ByteBuf buf) throws IOException {
		int start = buf.readerIndex();
		int requestSize = buf.readShort();
		short cmd = buf.readShort();
		long cookie = buf.readLong();
		buf.readerIndex(start);
		switch (cmd) {
			case TranslateBlockCmd:
				return new TranslateBlock(buf);
			case WriteBlockCmd:
				return new WriteBlock(buf);
			case WriteBlockRangeCmd:
				return new WriteBlockRange(buf);
			case UnmapBlockCmd:
				return new UnmapBlock(buf);
			default:
				LOG.error("Unknown RPC command (RpcID = {})", cmd);
				throw new IOException("Unknown RPC command: " + cmd);
		}
	}


	public static class WriteBlock extends RPCCall {
		static short requestSize;

		private long tx_addr; // 8
		private long tx_length; // 8
		private String tx_key; // variable size

		public WriteBlock(long cookie, long addr, long length, String newKey) {
			super(WriteBlockCmd, cookie);
			assert newKey != null;
			tx_addr = addr; // 8
			tx_length = length; // 8
			tx_key = newKey; // 4 + newkey.length()
			this.setRequestSize((short) (RPC_REQ_HEADER_SIZE + 8 + 8 + 4 + newKey.length())); // 4 bytes for key length
		}

		public WriteBlock(ByteBuf buf) {
			super(buf);
		}

		@Override
		public void deserializeRequest(ByteBuf buffer) {
			super.deserializeRequest(buffer);
			tx_addr = buffer.readLong(); // 8
			tx_length = buffer.readLong(); // 8
			int keySize = buffer.readInt(); // 4
			if (keySize > 0) {
				tx_key = buffer.toString(buffer.readerIndex(), keySize, Charset.defaultCharset()); // key.length()
				buffer.skipBytes(keySize);
			} else
				tx_key = null;
		}

		@Override
		public int serializeRequest(ByteBuf buffer) {
			super.serializeRequest(buffer);
			buffer.writeLong(this.tx_addr); // 8
			buffer.writeLong(this.tx_length); // 8
			if (this.tx_key != null) {
				buffer.writeInt(tx_key.length()); // 4
				ByteBufUtil.writeAscii(buffer, tx_key); // key.length()
			} else {
				buffer.writeInt(0);
			}
			assert requestSize == buffer.readableBytes();
			return requestSize;
		}

		public void setRequest(long addr, int length) {
			this.tx_addr = addr;
			this.tx_length = length;
		}

		public long getAddr() {
			return tx_addr;
		}

		public long getLength() {
			return tx_length;
		}

		public String getObjectKey() {
			return tx_key;
		}


		@Override
		public short getRequestSize() {
			return requestSize;
		}

		@Override
		public void setRequestSize(short size) {
			requestSize = size;
		}
	}


	public static class WriteBlockRange extends RPCCall {
		static short requestSize;

		private long tx_addr;
		private long tx_length;
		private long tx_offset;
		private String tx_key;

		public WriteBlockRange(long cookie, long addr, long blockOffset, long length, String newKey) {
			super(WriteBlockRangeCmd, cookie);
			assert newKey != null;
			tx_addr = addr; // 8
			tx_offset = blockOffset; // 8
			tx_length = length; // 8
			tx_key = newKey; // 4 + newkey.length()
			this.setRequestSize((short) (RPC_REQ_HEADER_SIZE + 8 + 8 + 8 + 4 + newKey.length()));
		}

		public WriteBlockRange(ByteBuf buf) {
			super(buf);
		}

		public void setRequest(long addr, int length) {
			this.tx_addr = addr;
			this.tx_length = length;
		}

		@Override
		public int serializeRequest(ByteBuf buffer) {
			super.serializeRequest(buffer);
			buffer.writeLong(this.tx_addr); // 8
			buffer.writeLong(this.tx_offset); // 8
			buffer.writeLong(this.tx_length); // 8
			if (this.tx_key != null) {
				buffer.writeInt(tx_key.length()); // 4
				ByteBufUtil.writeAscii(buffer, tx_key); // key length
			} else {
				buffer.writeInt(0);
			}
			assert requestSize == buffer.readableBytes();
			return requestSize;
		}

		public long getAddr() {
			return tx_addr;
		}

		@Override
		public void deserializeRequest(ByteBuf buffer) {
			super.deserializeRequest(buffer);
			tx_addr = buffer.readLong(); // 8
			tx_offset = buffer.readLong(); // 8
			tx_length = buffer.readLong(); // 8
			int keySize = buffer.readInt(); // 4
			if (keySize > 0) {
				tx_key = buffer.toString(buffer.readerIndex(), keySize, Charset.defaultCharset()); // key length
				buffer.skipBytes(keySize);
			} else
				tx_key = null;
		}

		public long getOffset() {
			return tx_offset;
		}

		public long getLength() {
			return tx_length;
		}

		public String getObjectKey() {
			return tx_key;
		}


		@Override
		public short getRequestSize() {
			return requestSize;
		}

		@Override
		public void setRequestSize(short size) {
			requestSize = size;
		}
	}


	public static class UnmapBlock extends RPCCall {
		private static final int requestSize = RPC_REQ_HEADER_SIZE + 8 + 8;

		private long tx_addr; // 8
		private long tx_length; // 8

		public UnmapBlock(long cookie, long addr, long length) {
			super(UnmapBlockCmd, cookie);
			tx_addr = addr;
			tx_length = length;
		}

		public UnmapBlock(ByteBuf buf) {
			super(buf);
		}

		public void setRequest(long addr, int length) {
			this.tx_addr = addr;
			this.tx_length = length;
		}

		@Override
		public int serializeRequest(ByteBuf buffer) {
			super.serializeRequest(buffer);
			buffer.writeLong(tx_addr);
			buffer.writeLong(tx_length);
			assert requestSize == buffer.readableBytes();
			return requestSize;
		}

		public long getAddr() {
			return tx_addr;
		}

		@Override
		public void deserializeRequest(ByteBuf buffer) {
			super.deserializeRequest(buffer);
			this.tx_addr = buffer.readLong();
			this.tx_length = buffer.readLong();
		}

		public long getLength() {
			return tx_length;
		}


		@Override
		public short getRequestSize() {
			return requestSize; // request size is static, no need to modify it
		}
	}


	public static class TranslateBlock extends RPCCall {
		private static final short requestSize = RPC_REQ_HEADER_SIZE + 8 + 8;
		private short responseSize;

		private long tx_addr; // 8
		private long tx_length; // 8
		private List<MappingEntry> rx_mapping;

		public TranslateBlock(long cookie, long addr, long length) {
			super(TranslateBlockCmd, cookie);
			this.tx_addr = addr;
			this.tx_length = length;
			this.rx_mapping = null;
			responseSize = RPC_RESP_HEADER_SIZE; // minimum size, final size computed when serializing response
		}

		public TranslateBlock(ByteBuf buf) {
			super(buf);
			responseSize = RPC_RESP_HEADER_SIZE;
		}

		public void setRequest(long addr, int length) {
			this.tx_addr = addr;
			this.tx_length = length;
		}

		@Override
		public int serializeRequest(ByteBuf buffer) {
			super.serializeRequest(buffer);
			buffer.writeLong(tx_addr); // 8
			buffer.writeLong(tx_length); // 8
			assert requestSize == buffer.readableBytes();
			assert getMessageSize(buffer) == buffer.readableBytes();
			return requestSize;
		}

		public long getAddr() {
			return tx_addr;
		}

		@Override
		public void deserializeRequest(ByteBuf buffer) {
			super.deserializeRequest(buffer);
			tx_addr = buffer.readLong();
			tx_length = buffer.readLong();
		}

		public long getLength() {
			return tx_length;
		}

		@Override
		public int serializeResponse(ByteBuf buffer) {
			int start = buffer.writerIndex();
			int msgSize = super.serializeResponse(buffer);
			if (this.rx_mapping != null) {
				msgSize += 4; // number of Object entries for this block
				buffer.writeInt(this.rx_mapping.size());
				for (MappingEntry entry : this.rx_mapping) {
					int keySize = entry.getKey().length();
					buffer.writeInt(keySize);
					ByteBufUtil.writeAscii(buffer, entry.getKey());
					buffer.writeLong(entry.getStartOffset());
					buffer.writeLong(entry.getSize());
					msgSize += 4 + keySize + 8 + 8;
				}
				buffer.setShort(start, msgSize);
				setResponseStatus(SUCCESS);
			} else {
				setResponseStatus(NO_MATCH);
			}
			assert msgSize == buffer.readableBytes();
			return msgSize;
		}

		public List<MappingEntry> getResponse() {
			return rx_mapping;
		}

		public void setResponse(List<MappingEntry> mapping) {
			this.rx_mapping = mapping;
			setResponseStatus(SUCCESS);
		}

		@Override
		public void deserializeResponse(ByteBuf buffer) {
			super.deserializeResponse(buffer);
			if (getStatus() == SUCCESS) {
				int ranges = buffer.readInt();
				this.rx_mapping = new ArrayList<>();
				for (int i = 0; i < ranges; i++) {
					int keySize = buffer.readInt();
					String key = buffer.toString(buffer.readerIndex(), keySize, Charset.defaultCharset());
					buffer.skipBytes(keySize);
					long start = buffer.readLong();
					long length = buffer.readLong();
					this.rx_mapping.add(new MappingEntry(key, start, length));
				}
			}
		}

		@Override
		public short getRequestSize() {
			return requestSize; // request size is static, no need to modify it
		}

		@Override
		public short getResponseSize() {
			return responseSize;
		}

		@Override
		public void setResponseSize(short size) {
			responseSize = size;
		}


	}
}
