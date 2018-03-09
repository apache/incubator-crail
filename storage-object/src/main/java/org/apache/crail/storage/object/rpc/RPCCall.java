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
import org.apache.crail.storage.object.ObjectStoreConstants;
import org.apache.crail.storage.object.ObjectStoreUtils;
import org.slf4j.Logger;

public abstract class RPCCall {
	public static final short SUCCESS = 0;
	public static final short RUNTIME_ERROR = -1;
	public static final short NO_MATCH = -2;
	public static final short INVALID_ARGS = -3;
	protected static final short RPC_REQ_HEADER_SIZE = 2 + 2 + 8;
	protected static final short RPC_RESP_HEADER_SIZE = 2 + 2 + 8 + 2;
	private static final Logger LOG = ObjectStoreUtils.getLogger();
	// message size is first 2 bytes
	private short cmd;   // 2
	private short status;  // 2
	private long cookie; // 8
	private long startTime;
	private long endTime;

	public RPCCall(short cmd, long cookie) {
		this.cmd = cmd;
		this.cookie = cookie;
	}

	public RPCCall(ByteBuf buffer) {
		deserializeRequest(buffer);
	}

	public static boolean isMessageComplete(ByteBuf buf) {
		int readableBytes = buf.readableBytes();
		if (readableBytes < 2) {
			return false;
		}
		return readableBytes >= RPCCall.getMessageSize(buf);
	}

	public static short getMessageSize(ByteBuf buf) {
		/* NOTE: I am not sure if messages always start at readerIndex 0. If this is not the case, all reads must be
		 * made relative to the initial buf.readerIndex(). This applies to the other getXXX(ByteBuf) methods.
		 */
		return buf.getShort(buf.readerIndex());
	}

	public static void drainMessage(ByteBuf buf) {
		int readableBytes = buf.readableBytes();
		int size = getMessageSize(buf);
		if (readableBytes < size)
			return;
		while (size-- > 0) {
			buf.readByte();
		}
	}

	public static short getCmd(ByteBuf buf) {
		return buf.getShort(buf.readerIndex() + 2);
	}

	public static long getCookie(ByteBuf buf) {
		return buf.getLong(buf.readerIndex() + 4);
	}

	public static short getStatus(ByteBuf buf) {
		return buf.getShort(buf.readerIndex() + 12);
	}

	public static void setMessageSize(ByteBuf buffer, int size) {
		buffer.setInt(0, size);
	}

	public void deserializeRequest(ByteBuf buffer) {
		setRequestSize(buffer.readShort());
		cmd = buffer.readShort();
		cookie = buffer.readLong();
	}

	public int serializeRequest(ByteBuf buffer) {
		if (ObjectStoreConstants.PROFILE) {
			startTime = System.nanoTime();
		}
		buffer.writeShort(getRequestSize());  // 2
		buffer.writeShort(cmd);  // 2
		buffer.writeLong(cookie);// 8
		return RPC_REQ_HEADER_SIZE;
	}

	public short getRequestSize() {
		return RPC_REQ_HEADER_SIZE;
	}

	public void setRequestSize(short size) {
	}

	public int serializeResponse(ByteBuf buffer) {
		short msgSize = getResponseSize();
		buffer.writeShort(msgSize);  // 2
		buffer.writeShort(cmd);  // 2
		buffer.writeLong(cookie); // 8
		buffer.writeShort(status);  // 2
		assert msgSize >= buffer.readableBytes();
		return RPC_RESP_HEADER_SIZE;
	}

	public short getResponseSize() {
		return RPC_RESP_HEADER_SIZE;
	}

	public void setResponseSize(short size) {
	}

	public void deserializeResponse(ByteBuf buffer) {
		setResponseSize(buffer.readShort());
		short rpcCmd = buffer.readShort();
		long rpcCookie = buffer.readLong();
		status = buffer.readShort();
		assert rpcCmd == this.cmd;
		assert rpcCookie == this.cookie;
		if (ObjectStoreConstants.PROFILE) {
			endTime = System.nanoTime();
			LOG.debug("Rpc call finished: Command Id={}, Cookie={}, Latency={} (us)", this.getCmd(), this.getCookie(),
					(endTime - startTime) / 1000.);
		}
	}

	public short getCmd() {
		return cmd;
	}

	public long getCookie() {
		return cookie;
	}

	public short getStatus() {
		return status;
	}

	public void setResponseStatus(short status) {
		this.status = status;
	}
}
