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
import org.slf4j.Logger;

import java.util.List;

public class ObjectStoreRequestDecoder extends ByteToMessageDecoder {
	private static Logger LOG = ObjectStoreUtils.getLogger();

	@Override
	protected void decode(ChannelHandlerContext channelHandlerContext, ByteBuf byteBuf, List<Object> list)
			throws Exception {
		/* Wait for a complete message. The first 2 bytes represent the message length */
		//LOG.debug("Decoding request ({} bytes available)" , byteBuf.readableBytes());
		if (!RPCCall.isMessageComplete(byteBuf)) {
			return;
		}
		/* Full message, we can now decode */
		RPCCall newReq = ObjectStoreRPC.createObjectStoreRPC(byteBuf);
		//LOG.debug("After decoding, {} bytes still remaining", byteBuf.readableBytes());
		list.add(newReq);
	}
}
