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

package org.apache.crail.storage.object.client;

import io.netty.channel.Channel;
import org.apache.crail.metadata.BlockInfo;
import org.apache.crail.storage.object.ObjectStoreUtils;
import org.apache.crail.storage.object.rpc.ObjectStoreRPC;
import org.apache.crail.storage.object.rpc.RPCCall;
import org.apache.crail.storage.object.rpc.RPCFuture;
import org.slf4j.Logger;

import java.util.concurrent.Future;

public class ObjectStoreMetadataClient {

	static private final Logger LOG = ObjectStoreUtils.getLogger();
	private final Channel clientChannel;
	private final ObjectStoreMetadataClientGroup group;

	public ObjectStoreMetadataClient(Channel clientChannel, ObjectStoreMetadataClientGroup grp) {
		LOG.debug("Creating new ObjectStore metadata client");
		this.clientChannel = clientChannel;
		this.group = grp;
	}

	public String toString() {
		return this.clientChannel.toString();
	}

	public void close() {
		/* don't care about the future */
		LOG.info("Closing ObjectStore Metadata Client");
		this.clientChannel.close();
	}

	public Future<RPCCall> translateBlock(BlockInfo blockInfo) {
		long cookie = this.group.getNextSlot();
		RPCCall rpc = new ObjectStoreRPC.TranslateBlock(cookie, blockInfo.getAddr(), blockInfo.getLength());
		return sendRPC(rpc);
	}

	private Future<RPCCall> sendRPC(RPCCall rpc) {
		//LOG.debug("Sending new RPC. Cookie = {}, RpcID = {}", rpc.getCookie(), rpc.getCmd());
		RPCFuture<RPCCall> resultFuture = new RPCFuture<RPCCall>("RpcID " + rpc.getCmd(), rpc);
		/* rpc goes into the map */
		this.group.insertNewInflight(rpc.getCookie(), resultFuture);
		/* now we construct and push out the request */
		this.clientChannel.writeAndFlush(rpc);
		return resultFuture;
	}

	public Future<RPCCall> writeBlock(BlockInfo blockInfo, String newKey) {
		long cookie = this.group.getNextSlot();
		RPCCall rpc = new ObjectStoreRPC.WriteBlock(cookie, blockInfo.getAddr(), blockInfo.getLength(), newKey);
		return sendRPC(rpc);
	}

	public Future<RPCCall> unmapBlock(BlockInfo blockInfo) {
		long cookie = this.group.getNextSlot();
		RPCCall rpc = new ObjectStoreRPC.UnmapBlock(cookie, blockInfo.getAddr(), blockInfo.getLength());
		return sendRPC(rpc);
	}

	public Future<RPCCall> writeBlockRange(BlockInfo blockInfo, long blockOffset, int length, String newKey) {
		long cookie = this.group.getNextSlot();
		RPCCall rpc = new ObjectStoreRPC.WriteBlockRange(cookie, blockInfo.getAddr(), blockOffset, length, newKey);
		return sendRPC(rpc);
	}
}
