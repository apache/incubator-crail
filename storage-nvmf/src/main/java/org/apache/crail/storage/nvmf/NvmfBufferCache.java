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

package org.apache.crail.storage.nvmf;

import com.ibm.disni.nvmef.NvmeEndpointGroup;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.crail.CrailBuffer;
import org.apache.crail.conf.CrailConstants;
import org.apache.crail.memory.BufferCache;
import org.apache.crail.memory.OffHeapBuffer;


public class NvmfBufferCache extends BufferCache {
	private static final int ALIGNMENT = 4096;
	private NvmeEndpointGroup endpointGroup;

	private List<ByteBuffer> bufferPool = new ArrayList<>();

	public NvmfBufferCache() throws IOException {
		super();
		endpointGroup = NvmfStorageTier.getEndpointGroup();
		if (endpointGroup == null) {
			throw new IOException("NvmfStorageTier not initialized");
		}
	}

	@Override
	public String providerName() {
		return "NvmfBufferCache";
	}

	@Override
	public CrailBuffer allocateRegion() throws IOException {
		ByteBuffer buffer = endpointGroup.allocateBuffer(CrailConstants.BUFFER_SIZE, ALIGNMENT);
		bufferPool.add(buffer);
		return OffHeapBuffer.wrap(buffer);
	}

	@Override
	public void close() {
		super.close();
		for (ByteBuffer buffer : bufferPool) {
			endpointGroup.freeBuffer(buffer);
		}
		bufferPool.clear();
	}
}
