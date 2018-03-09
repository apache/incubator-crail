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

package org.apache.crail.storage.object.server;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.apache.crail.storage.object.ObjectStoreUtils;
import org.apache.crail.storage.object.rpc.ObjectStoreRPC;
import org.apache.crail.storage.object.rpc.RPCCall;
import org.slf4j.Logger;


class ObjectStoreServerRPCProcessor extends SimpleChannelInboundHandler<RPCCall> {
	static private final Logger LOG = ObjectStoreUtils.getLogger();
	static private ObjectStoreMetadataServer service;

	static public void setMetadataService(ObjectStoreMetadataServer service) {
		ObjectStoreServerRPCProcessor.service = service;
	}

	@Override
	protected void channelRead0(ChannelHandlerContext ctx, RPCCall rpc) throws Exception {
		try {
			switch (rpc.getCmd()) {
				case ObjectStoreRPC.TranslateBlockCmd:
					service.translateBlock((ObjectStoreRPC.TranslateBlock) rpc);
					break;
				case ObjectStoreRPC.WriteBlockCmd:
					service.writeBlock((ObjectStoreRPC.WriteBlock) rpc);
					break;
				case ObjectStoreRPC.WriteBlockRangeCmd:
					service.writeBlockRange((ObjectStoreRPC.WriteBlockRange) rpc);
					break;
				case ObjectStoreRPC.UnmapBlockCmd:
					service.unmapBlock((ObjectStoreRPC.UnmapBlock) rpc);
					break;
				default:
					LOG.error("Ignoring invalid RPC command not valid (RpcID = {})", rpc.getCmd());
			}
		} catch (Exception e) {
			e.printStackTrace();
			LOG.error(e.getMessage());
		}

		try {
			/* flush response */
			ctx.channel().writeAndFlush(rpc);
		} catch (Exception e) {
			LOG.error("Failed writing RPC response (RpcID = {})", rpc.getCmd());
			e.printStackTrace();
			LOG.error(e.getMessage());
		}
	}

	protected void finalize() {
		LOG.info("Closing ObjectStore RPC processor");
	}
}
