/*
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

package org.apache.crail.storage.nvmf.client;

import org.apache.crail.CrailBuffer;
import org.apache.crail.CrailBufferCache;
import org.apache.crail.conf.CrailConfiguration;
import org.apache.crail.conf.CrailConstants;
import org.apache.crail.memory.MappedBufferCache;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.*;

public class NvmfStagingBufferCacheTest {

	@BeforeClass
	public static void init() throws IOException {
		CrailConstants.updateConstants(CrailConfiguration.createConfigurationFromFile());
	}

	private static CrailBufferCache bufferCache;
	static CrailBufferCache getBufferCache() throws IOException {
		if (bufferCache == null) {
			bufferCache = new MappedBufferCache();
		}
		return bufferCache;
	}


	@Test(expected = IllegalArgumentException.class)
	public void createBufferCache() throws IOException {
		new NvmfStagingBufferCache(getBufferCache(), -1, 512);
		new NvmfStagingBufferCache(getBufferCache(),0, 512);
		new NvmfStagingBufferCache(getBufferCache(),1024, -1);
		new NvmfStagingBufferCache(getBufferCache(),1024, 0);
	}

	@Test(expected = OutOfMemoryError.class)
	public void outOfMemory() throws Exception {
		NvmfStagingBufferCache bufferCache = new NvmfStagingBufferCache(getBufferCache(),1, 512);
		NvmfStagingBufferCache.BufferCacheEntry bufferCacheEntry = bufferCache.get(0);
		NvmfStagingBufferCache.BufferCacheEntry bufferCacheEntry2 = bufferCache.get(1);
	}

	@Test
	public void bufferExists() throws Exception {
		NvmfStagingBufferCache bufferCache = new NvmfStagingBufferCache(getBufferCache(),1, 512);
		NvmfStagingBufferCache.BufferCacheEntry bufferCacheEntry = bufferCache.get(0);
		NvmfStagingBufferCache.BufferCacheEntry existingBufferCacheEntry = bufferCache.getExisting(0);
		assertEquals(bufferCacheEntry, existingBufferCacheEntry);
	}

	@Test
	public void recycleBuffers() throws Exception {
		NvmfStagingBufferCache.BufferCacheEntry bufferCacheEntry[] = new NvmfStagingBufferCache.BufferCacheEntry[5];
		Set<CrailBuffer> buffers = new HashSet<>();
		NvmfStagingBufferCache bufferCache = new NvmfStagingBufferCache(getBufferCache(), bufferCacheEntry.length, 512);
		for (int i = 0; i < bufferCacheEntry.length; i++) {
			bufferCacheEntry[i] = bufferCache.get(i);
			buffers.add(bufferCacheEntry[i].getBuffer());
			bufferCacheEntry[i].put();
		}
		for (int i = 0; i < bufferCacheEntry.length; i++) {
			bufferCacheEntry[i] = bufferCache.get(i + bufferCacheEntry.length);
			assertTrue(buffers.remove(bufferCacheEntry[i].getBuffer()));
		}
	}
}
