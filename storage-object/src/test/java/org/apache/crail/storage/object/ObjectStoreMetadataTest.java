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

import org.apache.crail.storage.object.rpc.MappingEntry;
import org.apache.crail.storage.object.rpc.ObjectStoreRPC;
import org.apache.crail.storage.object.rpc.RPCCall;
import org.apache.crail.storage.object.server.ObjectStoreMetadataServer;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ObjectStoreMetadataTest {
	private static final org.slf4j.Logger LOG = ObjectStoreUtils.getLogger();

	private final static String key = "ABCDEFGHIJKLMN";
	private final long cookie = 1;
	private final long addr = 1234;
	private final long length = 1000000;
	private ObjectStoreMetadataServer mdServer;

	private static void validateMapping(List<MappingEntry> expected, List<MappingEntry> mapping) {
		int i = 0;
		for (MappingEntry oi : mapping) {
			MappingEntry exp = expected.get(i++);
			assertTrue(oi.equals(exp));
		}
	}

	private static void validateMapping(MappingEntry expected, List<MappingEntry> mapping) {
		assertEquals(mapping.size(), 1);
		MappingEntry oi = mapping.get(0);
		assertTrue(oi.equals(expected));
	}

	@BeforeClass
	public static void setUp() {
		BasicConfigurator.configure();
		Logger.getRootLogger().setLevel(Level.INFO);
		LOG.info(" ---------------------------------------------");
		LOG.info(" --- Starting ObjectStore Metadata Tests ---");
		ObjectStoreConstants.DATANODE = "127.0.0.1";
	}

	@AfterClass
	public static void tearDown() {
		LOG.info(" --- End ObjectStore Metadata Tests      ---");
		LOG.info(" ---------------------------------------------\n\n");
		BasicConfigurator.resetConfiguration();
		Logger.getRootLogger().setLevel(Level.INFO);
	}

	@Before
	public void setUpTest() {
		mdServer = new ObjectStoreMetadataServer();
	}

	@After
	public void tearDownTest() {
		mdServer.close();
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}


	@Test
	public void testBasicBlockOperations() {
		// Create metadata server and exercise all metadata calls

		short ret;
		MappingEntry oi;
		List<MappingEntry> mapping;
		ObjectStoreRPC.TranslateBlock translateBlock = new ObjectStoreRPC.TranslateBlock(cookie, addr, length);
		ObjectStoreRPC.WriteBlock newBlock = new ObjectStoreRPC.WriteBlock(cookie, addr, length, key);
		ObjectStoreRPC.UnmapBlock delBlock = new ObjectStoreRPC.UnmapBlock(cookie, addr, length);

		// Basic operations on a full block
		mdServer.writeBlock(newBlock);
		assertEquals(newBlock.getStatus(), RPCCall.SUCCESS);
		mdServer.translateBlock(translateBlock);
		assertEquals(translateBlock.getStatus(), RPCCall.SUCCESS);
		validateMapping(new MappingEntry(key, 0, length), translateBlock.getResponse());
		mdServer.unmapBlock(delBlock);
		assertEquals(delBlock.getStatus(), RPCCall.SUCCESS);
		mdServer.unmapBlock(delBlock);
		assertEquals(delBlock.getStatus(), RPCCall.NO_MATCH);
		mdServer.translateBlock(translateBlock);
		assertEquals(translateBlock.getStatus(), RPCCall.NO_MATCH);
	}

	@Test
	public void testMetadataOperations() {
		// Create metadata server and exercise all metadata calls
		short ret;
		MappingEntry oi;
		List<MappingEntry> mapping;

		ObjectStoreRPC.TranslateBlock translateBlock = new ObjectStoreRPC.TranslateBlock(cookie, addr, length);
		int sector = 1000;
		long entries = 5;// length / sector;

		// Write initial ranges
		for (int i = 0; i < entries; i++) {
			ObjectStoreRPC.WriteBlockRange range =
					new ObjectStoreRPC.WriteBlockRange(cookie, addr, sector * i, sector, key + i);
			mdServer.writeBlockRange(range);
			assertEquals(range.getStatus(), RPCCall.SUCCESS);
		}
		ArrayList<MappingEntry> expectedMapping = new ArrayList();
		for (int i = 0; i < entries; i++) {
			expectedMapping.add(i, new MappingEntry(key + i, sector * i, sector));
		}
		mdServer.translateBlock(translateBlock);
		List<MappingEntry> currentMapping = translateBlock.getResponse();
		validateMapping(expectedMapping, currentMapping);

		// Overwrite ranges
		for (int i = 0; i < entries; i++) {
			ObjectStoreRPC.WriteBlockRange range =
					new ObjectStoreRPC.WriteBlockRange(cookie, addr, sector * i, sector, key + i + "new");
			mdServer.writeBlockRange(range);
			assertEquals(range.getStatus(), RPCCall.SUCCESS);
		}
		expectedMapping = new ArrayList();
		for (int i = 0; i < entries; i++) {
			expectedMapping.add(i, new MappingEntry(key + i + "new", sector * i, sector));
		}
		mdServer.translateBlock(translateBlock);
		currentMapping = translateBlock.getResponse();
		validateMapping(expectedMapping, currentMapping);
	}

	@Test
	public void testBlockRangeOperations() {
		// Create metadata server and exercise all metadata calls
		short ret;
		MappingEntry oi;
		String k1 = key + "-1";
		String k2 = key + "-2";
		String k3 = key + "-3";

		List<MappingEntry> mapping;
		ObjectStoreRPC.TranslateBlock translateBlock = new ObjectStoreRPC.TranslateBlock(cookie, addr, length);
		ObjectStoreRPC.WriteBlock newBlock = new ObjectStoreRPC.WriteBlock(cookie, addr, length, key);
		ObjectStoreRPC.UnmapBlock delBlock = new ObjectStoreRPC.UnmapBlock(cookie, addr, length);
		ObjectStoreRPC.WriteBlockRange write1 = new ObjectStoreRPC.WriteBlockRange(cookie, addr, 100, 100, k1);
		ObjectStoreRPC.WriteBlockRange write2 = new ObjectStoreRPC.WriteBlockRange(cookie, addr, 300, 100, k2);
		ObjectStoreRPC.WriteBlockRange write3 = new ObjectStoreRPC.WriteBlockRange(cookie, addr, 500, 100, k3);
		mdServer.writeBlock(newBlock);
		assertEquals(newBlock.getStatus(), RPCCall.SUCCESS);
		mdServer.translateBlock(translateBlock);
		validateMapping(Arrays.asList(new MappingEntry(key, 0, length)),
				translateBlock.getResponse());
		mdServer.writeBlockRange(write1);
		assertEquals(write1.getStatus(), RPCCall.SUCCESS);
		validateMapping(Arrays.asList(
				new MappingEntry(key, 0, 100),
				new MappingEntry(k1, 100, 100),
				new MappingEntry(key, 200, length - 200)),
				translateBlock.getResponse());
		mdServer.writeBlockRange(write2);
		assertEquals(write2.getStatus(), RPCCall.SUCCESS);
		validateMapping(Arrays.asList(
				new MappingEntry(key, 0, 100),
				new MappingEntry(k1, 100, 100),
				new MappingEntry(key, 200, 100),
				new MappingEntry(k2, 300, 100),
				new MappingEntry(key, 400, length - 400)),
				translateBlock.getResponse());
		mdServer.writeBlockRange(write3);
		assertEquals(write3.getStatus(), RPCCall.SUCCESS);
		mdServer.translateBlock(translateBlock);
		assertEquals(translateBlock.getStatus(), RPCCall.SUCCESS);
		validateMapping(Arrays.asList(
				new MappingEntry(key, 0, 100),
				new MappingEntry(k1, 100, 100),
				new MappingEntry(key, 200, 100),
				new MappingEntry(k2, 300, 100),
				new MappingEntry(key, 400, 100),
				new MappingEntry(k3, 500, 100),
				new MappingEntry(key, 600, length - 600)),
				translateBlock.getResponse());
		mdServer.unmapBlock(delBlock);
		assertEquals(delBlock.getStatus(), RPCCall.SUCCESS);
		mdServer.unmapBlock(delBlock);
		assertEquals(delBlock.getStatus(), RPCCall.NO_MATCH);
		mdServer.translateBlock(translateBlock);
		assertEquals(translateBlock.getStatus(), RPCCall.NO_MATCH);
	}

	@Test
	public void testOverlappingRanges() {
		// Create metadata server and exercise all metadata calls
		short ret;
		MappingEntry oi;
		List<MappingEntry> mapping;

		ObjectStoreRPC.TranslateBlock translateBlock = new ObjectStoreRPC.TranslateBlock(cookie, addr, length);
		ObjectStoreRPC.WriteBlock newBlock = new ObjectStoreRPC.WriteBlock(cookie, addr, length, key);
		ObjectStoreRPC.UnmapBlock delBlock = new ObjectStoreRPC.UnmapBlock(cookie, addr, length);
		int sector = 1000;
		ObjectStoreRPC.WriteBlockRange rangeLarge =
				new ObjectStoreRPC.WriteBlockRange(cookie, addr, sector * 0, sector * 10, key + "1");
		ObjectStoreRPC.WriteBlockRange rangeMedium =
				new ObjectStoreRPC.WriteBlockRange(cookie, addr, sector * 2, sector * 6, key + "2");
		ObjectStoreRPC.WriteBlockRange rangeSmall =
				new ObjectStoreRPC.WriteBlockRange(cookie, addr, sector * 4, sector * 2, key + "3");

		mdServer.writeBlockRange(rangeLarge);
		assertEquals(rangeLarge.getStatus(), RPCCall.SUCCESS);
		mdServer.writeBlockRange(rangeMedium);
		assertEquals(rangeMedium.getStatus(), RPCCall.SUCCESS);
		mdServer.writeBlockRange(rangeSmall);
		assertEquals(rangeSmall.getStatus(), RPCCall.SUCCESS);
		mdServer.translateBlock(translateBlock);
		assertEquals(translateBlock.getStatus(), RPCCall.SUCCESS);
		validateMapping(Arrays.asList(
				new MappingEntry(key + "1", sector * 0, sector * 2),
				new MappingEntry(key + "2", sector * 2, sector * 2),
				new MappingEntry(key + "3", sector * 4, sector * 2),
				new MappingEntry(key + "2", sector * 6, sector * 2),
				new MappingEntry(key + "1", sector * 8, sector * 2)),
				translateBlock.getResponse());

		mdServer.writeBlock(newBlock);
		assertEquals(newBlock.getStatus(), RPCCall.SUCCESS);
		mdServer.translateBlock(translateBlock);
		validateMapping(Arrays.asList(new MappingEntry(key, 0, length)),
				translateBlock.getResponse());
	}
}
