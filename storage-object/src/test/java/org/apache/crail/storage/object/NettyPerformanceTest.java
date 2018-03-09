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

import org.apache.crail.conf.CrailConstants;
import org.apache.crail.metadata.BlockInfo;
import org.apache.crail.metadata.DataNodeInfo;
import org.apache.crail.storage.object.client.ObjectStoreMetadataClient;
import org.apache.crail.storage.object.client.ObjectStoreMetadataClientGroup;
import org.apache.crail.storage.object.rpc.ObjectStoreRPC;
import org.apache.crail.storage.object.rpc.RPCCall;
import org.apache.crail.storage.object.server.ObjectStoreMetadataServer;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.*;

import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;

public class NettyPerformanceTest {
	private static final org.slf4j.Logger LOG = ObjectStoreUtils.getLogger();
	private final long addr = 0;
	private final int threadNr = 8;
	private ObjectStoreMetadataClientGroup clientGroup;
	private ObjectStoreMetadataClient client;
	private ObjectStoreMetadataServer server;

	@BeforeClass
	public static void setUp() {
		BasicConfigurator.configure();
		Logger.getRootLogger().setLevel(Level.INFO);
		LOG.info(" ---------------------------------------------");
		LOG.info(" --- Starting NettyPerformanceTests ---");
		ObjectStoreConstants.PROFILE = false;
		ObjectStoreConstants.DATANODE = "127.0.0.1";
	}

	@AfterClass
	public static void tearDown() {
		LOG.info(" --- End NettyPerformanceTests       ---");
		LOG.info(" ---------------------------------------------\n\n");
		BasicConfigurator.resetConfiguration();
		Logger.getRootLogger().setLevel(Level.INFO);
		ObjectStoreConstants.PROFILE = false;
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	@Before
	public void setUpTest() {
		LOG.info(" *** Setting up test ***");
		server = new ObjectStoreMetadataServer(); // create server first
		server.start();
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		clientGroup = new ObjectStoreMetadataClientGroup();
		client = clientGroup.getClient();
	}

	@After
	public void tearDownTest() {
		LOG.info(" *** Tearing down test ***\n");
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
	public void testBasicPerformance() throws Exception {
		LOG.info(" *** Netty single thread latency test ***");
		OneRPCRun run = new OneRPCRun(this.client, 0);
		Thread thread = new Thread(run);
		thread.start();
		thread.join();
		assertEquals(run.getStatus(), 0);
	}

	@Test
	public void testRPCConcurrency() throws Exception {
		LOG.info(" *** Netty concurrency test ***");
		Thread[] threads = new Thread[threadNr];
		OneRPCRun[] runs = new OneRPCRun[threadNr];
		for (int i = 0; i < threadNr; i++) {
			runs[i] = new OneRPCRun(this.client, i);
			threads[i] = new Thread(runs[i]);
			threads[i].start();
		}
		for (int i = 0; i < threadNr; i++) {
			threads[i].join();
			assertEquals(runs[i].getStatus(), 0);
		}
	}

	private class OneRPCRun implements Runnable {
		private ObjectStoreMetadataClient client;
		private int id;
		private volatile int exitStatus = -1;

		public OneRPCRun(ObjectStoreMetadataClient client, int id) {
			this.client = client;
			this.id = id;
		}

		public void run() {
			Future<RPCCall> rpcFuture;
			ObjectStoreRPC.TranslateBlock translateBlock;
			long startTime, endTime, runtime;
			final int iterations = 100000;

			startTime = System.nanoTime();
			try {
				// write block and retrive block to object mappping
				for (int i = 0; i < iterations; i++) {
					BlockInfo bi = new BlockInfo(new DataNodeInfo(),
							addr + id * ObjectStoreConstants.ALLOCATION_SIZE,
							addr + id * ObjectStoreConstants.ALLOCATION_SIZE,
							(int) CrailConstants.BLOCK_SIZE,
							123456);
					rpcFuture = client.writeBlock(bi, "objkey" + id);
					ObjectStoreRPC.WriteBlock writeBlock = (ObjectStoreRPC.WriteBlock) rpcFuture.get();
					assertEquals(writeBlock.getStatus(), RPCCall.SUCCESS);
				}
				endTime = System.nanoTime();
				runtime = endTime - startTime;
				LOG.info("{} WriteBlock RPCs in {} seconds. Average latency {}  (us)", iterations,
						runtime / 1000000000.,
						runtime / iterations / 1000.);


				startTime = System.nanoTime();
				// write block and retrive block to object mappping
				for (int i = 0; i < iterations; i++) {
					BlockInfo bi = new BlockInfo(new DataNodeInfo(),
							addr + id * ObjectStoreConstants.ALLOCATION_SIZE,
							addr + id * ObjectStoreConstants.ALLOCATION_SIZE,
							(int) CrailConstants.BLOCK_SIZE,
							123456);
					rpcFuture = client.translateBlock(bi);
					translateBlock = (ObjectStoreRPC.TranslateBlock) rpcFuture.get();
					assertEquals(translateBlock.getStatus(), RPCCall.SUCCESS);
				}
				endTime = System.nanoTime();
				runtime = endTime - startTime;
				LOG.info("{} TranslateBlock RPCs in {} seconds. Average latency {} (us)", iterations,
						runtime / 1000000000.,
						runtime / iterations / 1000.);
				exitStatus = 0;
			} catch (Exception e) {
				LOG.error("Got exception " + e);
				return;
			}
		}

		public int getStatus() {
			return exitStatus;
		}
	}
}
