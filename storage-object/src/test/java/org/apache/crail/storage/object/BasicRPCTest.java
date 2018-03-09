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

import org.apache.crail.metadata.BlockInfo;
import org.apache.crail.metadata.DataNodeInfo;
import org.apache.crail.storage.object.client.ObjectStoreMetadataClient;
import org.apache.crail.storage.object.client.ObjectStoreMetadataClientGroup;
import org.apache.crail.storage.object.rpc.MappingEntry;
import org.apache.crail.storage.object.rpc.ObjectStoreRPC;
import org.apache.crail.storage.object.rpc.RPCCall;
import org.apache.crail.storage.object.server.ObjectStoreMetadataServer;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.*;

import java.util.List;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;

public class BasicRPCTest {
	private static final org.slf4j.Logger LOG = ObjectStoreUtils.getLogger();
	private final long addr = 1234;
	private final int length = 1000000;
	private ObjectStoreMetadataClientGroup clientGroup;
	private ObjectStoreMetadataClient client;
	private ObjectStoreMetadataServer server;

	@BeforeClass
	public static void setUp() {
		BasicConfigurator.configure();
		Logger.getRootLogger().setLevel(Level.INFO);
		LOG.info(" ---------------------------------------------");
		LOG.info(" --- Starting Basic RPC Tests ---");
		ObjectStoreConstants.PROFILE = true;
		ObjectStoreConstants.DATANODE = "127.0.0.1";
	}

	@AfterClass
	public static void tearDown() {
		BasicConfigurator.resetConfiguration();
		Logger.getRootLogger().setLevel(Level.INFO);
		ObjectStoreConstants.PROFILE = false;
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		LOG.info(" --- End Basic RPC Tests ---");
		LOG.info(" ---------------------------------------------\n\n");
	}

	@Before
	public void setUpTest() {
		LOG.info(" *** Set up test ***");
		server = new ObjectStoreMetadataServer();
		server.start();
		clientGroup = new ObjectStoreMetadataClientGroup();
		client = clientGroup.getClient();
	}

	@After
	public void tearDownTest() {
		LOG.info(" *** Tear down test ***");
		client.close();
		clientGroup.closeClientGroup();
		server.close();
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testBasicRpc() throws Exception {
		LOG.info(" *** testBasicRpc() ***");
		BlockInfo bi = new BlockInfo(new DataNodeInfo(), addr, addr, length, 123456);
		Future<RPCCall> rpcFuture;
		ObjectStoreRPC.TranslateBlock translateBlock;

		// read non-existing block
		rpcFuture = client.translateBlock(bi);
		translateBlock = (ObjectStoreRPC.TranslateBlock) rpcFuture.get();
		assertEquals(translateBlock.getStatus(), RPCCall.NO_MATCH);
		rpcFuture = client.translateBlock(bi);
		translateBlock = (ObjectStoreRPC.TranslateBlock) rpcFuture.get();
		assertEquals(translateBlock.getStatus(), RPCCall.NO_MATCH);

		// write block and retrive block to object mappping
		rpcFuture = client.writeBlock(bi, "key");
		ObjectStoreRPC.WriteBlock writeBlock = (ObjectStoreRPC.WriteBlock) rpcFuture.get();
		assertEquals(writeBlock.getStatus(), RPCCall.SUCCESS);
		rpcFuture = client.translateBlock(bi);
		translateBlock = (ObjectStoreRPC.TranslateBlock) rpcFuture.get();
		assertEquals(translateBlock.getStatus(), RPCCall.SUCCESS);
		List<MappingEntry> blockMapping = translateBlock.getResponse();
		assertEquals(blockMapping.size(), 1);
		assertEquals(blockMapping.get(0).getKey(), "key");

		// delete block & verify
		rpcFuture = client.unmapBlock(bi);
		ObjectStoreRPC.UnmapBlock unmapBlock = (ObjectStoreRPC.UnmapBlock) rpcFuture.get();
		assertEquals(unmapBlock.getStatus(), RPCCall.SUCCESS);
		rpcFuture = client.translateBlock(bi);
		translateBlock = (ObjectStoreRPC.TranslateBlock) rpcFuture.get();
		assertEquals(translateBlock.getStatus(), RPCCall.NO_MATCH);
	}
}
