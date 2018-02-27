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
import io.netty.handler.codec.MessageToByteEncoder;
import org.apache.crail.storage.object.ObjectStoreUtils;
import org.slf4j.Logger;

public class RPCRequestEncoder extends MessageToByteEncoder<RPCCall> {

	private static Logger LOG = ObjectStoreUtils.getLogger();

	@Override
	protected void encode(ChannelHandlerContext channelHandlerContext, RPCCall req,
	                      ByteBuf byteBuf) throws Exception {
		int bytes = req.serializeRequest(byteBuf);
		int bytes2 = byteBuf.readableBytes();
		assert bytes == bytes2;
		//LOG.debug("Request encoded (RpcID = {}, size = {})  ", req.getCmd(), byteBuf.readableBytes());
	}
}
