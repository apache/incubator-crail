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

import java.io.IOException;

import org.apache.crail.CrailBufferCache;
import org.apache.crail.CrailStatistics;
import org.apache.crail.conf.CrailConfiguration;
import org.apache.crail.metadata.DataNodeInfo;
import org.apache.crail.storage.StorageClient;
import org.apache.crail.storage.StorageEndpoint;
import org.apache.crail.storage.nvmf.client.NvmfStorageEndpoint;
import org.apache.crail.utils.CrailUtils;
import org.slf4j.Logger;

import com.ibm.disni.nvmef.NvmeEndpointGroup;
import com.ibm.disni.nvmef.spdk.NvmeTransportType;

public class NvmfStorageClient implements StorageClient {
	private static final Logger LOG = CrailUtils.getLogger();
	private static NvmeEndpointGroup clientGroup;
	private boolean initialized = false;

	public void init(CrailStatistics statistics, CrailBufferCache bufferCache, CrailConfiguration crailConfiguration,
					 String[] args) throws IOException {
		if (initialized) {
			throw new IOException("NvmfStorageTier already initialized");
		}
		initialized = true;

		NvmfStorageConstants.parseCmdLine(crailConfiguration, args);
	}

	public void printConf(Logger logger) {
		NvmfStorageConstants.printConf(logger);
	}

	public static NvmeEndpointGroup getEndpointGroup() {
		if (clientGroup == null) {
			clientGroup = new NvmeEndpointGroup(new NvmeTransportType[]{NvmeTransportType.RDMA},
					NvmfStorageConstants.CLIENT_MEMPOOL);
		}
		return clientGroup;
	}

	public synchronized StorageEndpoint createEndpoint(DataNodeInfo info) throws IOException {
		return new NvmfStorageEndpoint(getEndpointGroup(), CrailUtils.datanodeInfo2SocketAddr(info));
	}

	public void close() throws Exception {
	}

}
