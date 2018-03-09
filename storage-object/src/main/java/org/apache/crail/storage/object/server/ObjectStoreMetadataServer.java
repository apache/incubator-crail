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

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.crail.conf.CrailConstants;
import org.apache.crail.storage.object.ObjectStoreConstants;
import org.apache.crail.storage.object.ObjectStoreUtils;
import org.apache.crail.storage.object.client.S3ObjectStoreClient;
import org.apache.crail.storage.object.rpc.*;
import org.slf4j.Logger;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Collections.binarySearch;

public class ObjectStoreMetadataServer extends Thread {
	static private final Logger LOG = ObjectStoreUtils.getLogger();

	private final ConcurrentHashMap<Long, ArrayList<MappingEntry>> blockToObject;

	private final EventLoopGroup acceptGroup;
	private final EventLoopGroup workerGroup;
	private final InetSocketAddress address;

	public ObjectStoreMetadataServer() {
		blockToObject = new ConcurrentHashMap<>();
		address = new InetSocketAddress(ObjectStoreConstants.DATANODE, ObjectStoreConstants.DATANODE_PORT);
		acceptGroup = new NioEventLoopGroup(4);
		workerGroup = new NioEventLoopGroup(4);
	}

	private static void validateMapping(ArrayList<MappingEntry> mapping) {
		if (mapping.size() <= 1) {
			return;
		}
		MappingEntry prev = mapping.get(0);
		MappingEntry cur = mapping.get(0);
		assert prev.getStartOffset() < prev.getEndOffset();
		for (int i = 1; i < mapping.size(); i++) {
			cur = mapping.get(i);
			// check for overlap. This also guarantees a sorted order
			assert prev.getEndOffset() <= cur.getStartOffset();
			assert cur.getStartOffset() < cur.getEndOffset();
		}
		assert cur.getEndOffset() <= CrailConstants.BLOCK_SIZE;
	}

	public void run() {
		/* here we run the incoming RPC service */
		LOG.info("Starting the ObjectStore metadata service at {} ", address);
		/* start the netty server */
		try {
			ServerBootstrap boot = new ServerBootstrap();
			boot.group(acceptGroup, workerGroup);
			/* we use sockets */
			boot.channel(NioServerSocketChannel.class);
			/* for new incoming connection */
			ObjectStoreServerRPCProcessor.setMetadataService(this);
			boot.childHandler(new ChannelInitializer<SocketChannel>() {
				@Override
				public void initChannel(SocketChannel ch) throws Exception {
					LOG.debug("A new connection has arrived from {} ", ch.remoteAddress());
					/* incoming pipeline */
					ch.pipeline().addLast("ObjectStoreRequestDecoder", new ObjectStoreRequestDecoder());
					ch.pipeline().addLast("RPCProcessor", new ObjectStoreServerRPCProcessor());
					/* outgoing pipeline */
					ch.pipeline().addLast("RPCResponseEncoder", new RPCResponseEncoder());
				}
			});
			/* optimizing general settings */
			boot.option(ChannelOption.SO_BACKLOG, 1024);
			boot.childOption(ChannelOption.SO_KEEPALIVE, true);
			boot.option(ChannelOption.SO_KEEPALIVE, true);
			boot.option(ChannelOption.TCP_NODELAY, true);
			boot.option(ChannelOption.SO_REUSEADDR, true);
			boot.option(ChannelOption.SO_RCVBUF, 1048576);
			boot.option(ChannelOption.SO_SNDBUF, 1048576);
			boot.option(ChannelOption.SO_LINGER, 1);

			/* bind the server and start */
			ChannelFuture f = boot.bind(address).sync();
			f.channel().closeFuture().sync();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} finally {
			close();
		}
	}

	public void close() {
		LOG.debug("Object store metadata service @{} shut down", address);
		acceptGroup.shutdownGracefully();
		workerGroup.shutdownGracefully();
		cleanup();
	}

	private void cleanup() {
		if (ObjectStoreConstants.CLEANUP_ON_EXIT) {
			// create new connection to the ObjectStore
			S3ObjectStoreClient objectStoreClient;
			objectStoreClient = new S3ObjectStoreClient();
			if (objectStoreClient.deleteBucket(ObjectStoreConstants.S3_BUCKET_NAME))
				return;
			// Could not delete bucket. Try to delete all mapped objects
			for (long entry : blockToObject.keySet()) {
				List<MappingEntry> mapping = blockToObject.get(entry);
				for (MappingEntry object : mapping) {
					try {
						objectStoreClient.deleteObject(object.getKey());
					} catch (Exception e) {
						LOG.warn("While deleting object {}, got exception ", object.getKey(), e);
					}
				}
			}
		}
	}

	public void translateBlock(ObjectStoreRPC.TranslateBlock rpc) {
		List<MappingEntry> mapping = blockToObject.get(rpc.getAddr());
		if (mapping != null) {
			rpc.setResponse(mapping);
		} else {
			rpc.setResponseStatus(RPCCall.NO_MATCH);
		}
	}

	public void writeBlock(ObjectStoreRPC.WriteBlock rpc) {
		MappingEntry range = new MappingEntry(rpc.getObjectKey(), 0, rpc.getLength());
		LOG.debug("Block {} mapped to Object '{}'", rpc.getAddr(), rpc.getObjectKey());
		ArrayList<MappingEntry> mapping = new ArrayList<>();
		if (mapping.add(range)) {
			blockToObject.put(rpc.getAddr(), mapping);
			rpc.setResponseStatus(RPCCall.SUCCESS);
		} else {
			LOG.error("Could not add block to object mapping");
			rpc.setResponseSize(RPCCall.RUNTIME_ERROR);
		}
	}

	public void unmapBlock(ObjectStoreRPC.UnmapBlock rpc) {
		LOG.debug("Unmapping block {}", rpc.getAddr());
		if (blockToObject.remove(rpc.getAddr()) != null) {
			rpc.setResponseStatus(RPCCall.SUCCESS);
		} else {
			rpc.setResponseStatus(RPCCall.NO_MATCH);
		}
	}

	public void writeBlockRange(ObjectStoreRPC.WriteBlockRange rpc) {
		assert ObjectStoreConstants.ALLOCATION_SIZE >= (rpc.getOffset() + rpc.getLength());
		// create new entry
		long start = rpc.getOffset();
		long length = rpc.getLength();
		long end = start + length;
		MappingEntry newEntry = new MappingEntry(rpc.getObjectKey(), start, length);
		LOG.debug("Block {} range ({} - {}) mapped to Object '{}'",
				rpc.getAddr(), rpc.getOffset(), rpc.getOffset() + rpc.getLength(), rpc.getObjectKey());
		// retrieve existing mapping if it exists
		ArrayList<MappingEntry> mapping = blockToObject.get(rpc.getAddr());
		if (mapping == null) {
			mapping = new ArrayList<>();
			blockToObject.put(rpc.getAddr(), mapping);
			mapping.add(newEntry);
			rpc.setResponseStatus(RPCCall.SUCCESS);
			return;
		}
		// insert new entry at appropriate location (entries are ordered by start address and do not overlap)
		int insertPos = binarySearch(mapping, newEntry);
		if (insertPos < 0)
			insertPos = -(insertPos + 1);
		// check if an existing entry must be split
		if (insertPos > 0) {
			MappingEntry prev = mapping.get(insertPos - 1);
			if (prev.getStartOffset() < start && prev.getEndOffset() > end) {
				mapping.add(insertPos, new MappingEntry(prev.getKey(), end, prev.getEndOffset() - end));
			}
		}
		mapping.add(insertPos, newEntry);
		// Update/remove overlapping entries to the left. Only a single overlapping entry can exist
		if (insertPos > 0) {
			MappingEntry cur = mapping.get(insertPos - 1);
			if (cur.getEndOffset() > start) {
				cur.setEndAddr(start);
				if (!cur.isValid())
					mapping.remove(insertPos - 1);
			}
		}
		// Update/remove overlapping entries to the right. Only a single overlapping entry can exist
		for (int i = insertPos + 1; i < mapping.size(); i++) {
			MappingEntry cur = mapping.get(i);
			if (cur.getStartOffset() < end) {
				cur.setStartAddr(end);
				if (!cur.isValid()) {
					mapping.remove(i);
					i--;
				}
			} else {
				break;
			}
		}
		rpc.setResponseStatus(RPCCall.SUCCESS);
		//validateMapping(mapping);
	}

	public EventLoopGroup getAcceptGroup() {
		return acceptGroup;
	}

	public EventLoopGroup getWorkerGroup() {
		return workerGroup;
	}

	protected void finalize() {
		LOG.info("Closing ObjectStore Metadata Server");
		try {
			close();
		} catch (Exception e) {
			LOG.error("Could not close ObjectStore Metadata Server. Reason: {}", e);
		}
	}
}