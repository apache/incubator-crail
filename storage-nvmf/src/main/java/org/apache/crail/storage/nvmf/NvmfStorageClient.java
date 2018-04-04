/*
 * Copyright (C) 2018, IBM Corporation
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

import com.ibm.jnvmf.Nvme;
import org.apache.crail.CrailBufferCache;
import org.apache.crail.CrailStatistics;
import org.apache.crail.conf.CrailConfiguration;
import org.apache.crail.metadata.DataNodeInfo;
import org.apache.crail.storage.StorageClient;
import org.apache.crail.storage.StorageEndpoint;
import org.apache.crail.storage.nvmf.client.NvmfStorageEndpoint;
import org.apache.crail.utils.CrailUtils;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class NvmfStorageClient implements StorageClient {
	private static final Logger LOG = CrailUtils.getLogger();
	private static Nvme nvme;
	private boolean initialized;
	private List<StorageEndpoint> endpoints;
	private CrailStatistics statistics;
	private CrailBufferCache bufferCache;

	public NvmfStorageClient() {
		this.initialized = false;
		this.endpoints = new CopyOnWriteArrayList<>();
	}

	public void init(CrailStatistics statistics, CrailBufferCache bufferCache, CrailConfiguration crailConfiguration,
					 String[] args) throws IOException {
		if (initialized) {
			throw new IOException("NvmfStorageTier already initialized");
		}
		initialized = true;
		this.statistics = statistics;
		this.bufferCache = bufferCache;
		LOG.info("Initialize Nvmf storage client");
		NvmfStorageConstants.parseCmdLine(crailConfiguration, args);
	}

	public void printConf(Logger logger) {
		NvmfStorageConstants.printConf(logger);
	}

	public static Nvme getEndpointGroup() throws UnknownHostException {
		if (nvme == null) {
			nvme = new Nvme();
		}
		return nvme;
	}

	public synchronized StorageEndpoint createEndpoint(DataNodeInfo info) throws IOException {
		StorageEndpoint endpoint = new NvmfStorageEndpoint(getEndpointGroup(), info, statistics, bufferCache);
		endpoints.add(endpoint);
		return endpoint;
	}

	public void close() throws Exception {
		for (StorageEndpoint endpoint : endpoints) {
			endpoint.close();
		}
	}

}
