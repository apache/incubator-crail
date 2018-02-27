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
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.apache.crail.storage.object.ObjectStoreUtils;
import org.apache.crail.storage.object.client.ObjectStoreMetadataClientGroup;
import org.slf4j.Logger;

import java.util.List;

public class ObjectStoreResponseDecoder extends ByteToMessageDecoder {

	private static Logger LOG = ObjectStoreUtils.getLogger();
	private final ObjectStoreMetadataClientGroup group;

	public ObjectStoreResponseDecoder(ObjectStoreMetadataClientGroup group) {
		this.group = group;
	}

	@Override
	protected void decode(ChannelHandlerContext channelHandlerContext, ByteBuf byteBuf, List<Object> list)
			throws Exception {
		LOG.debug("Decoding response ({} bytes available)", byteBuf.readableBytes());
		while (RPCCall.isMessageComplete(byteBuf)) {
			long cookie = RPCCall.getCookie(byteBuf);
			RPCFuture<RPCCall> future = group.retrieveAndRemove(cookie);
			if (future != null) {
				RPCCall rpc = future.getResult();
				//LOG.debug("Received response for Cookie = {}, RpcID = {}", rpc.getCookie(), rpc.getCmd());
				rpc.deserializeResponse(byteBuf);
				future.markDone();
			} else {
				// This can happen in several scenarios: (a) network issues, (b) client kill and restart
				LOG.warn("Received response to non registered RPC. Buffer reader index = {}, readable bytes = {}, " +
								"Cookie = {}. Draining message...",
						byteBuf.readerIndex(), byteBuf.readableBytes(), cookie);
				if (group.getInFlight() == 0) {
					byteBuf.clear();
				} else {
					group.dumpInFligh();
					RPCCall.drainMessage(byteBuf);
				}
			}
		}
	}
}
