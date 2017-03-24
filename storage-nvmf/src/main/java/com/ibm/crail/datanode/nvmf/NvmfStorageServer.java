/*
 * Crail: A Multi-tiered Distributed Direct Access File System
 *
 * Author:
 * Jonas Pfefferle <jpf@zurich.ibm.com>
 *
 * Copyright (C) 2016, IBM Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.ibm.crail.datanode.nvmf;

import com.ibm.crail.utils.CrailUtils;
import com.ibm.disni.nvmef.NvmeEndpoint;
import com.ibm.disni.nvmef.NvmeServerEndpoint;
import org.slf4j.Logger;

import java.net.InetSocketAddress;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class NvmfStorageServer implements Runnable {
	private static final Logger LOG = CrailUtils.getLogger();

	private final NvmeServerEndpoint serverEndpoint;
	private final InetSocketAddress datanodeAddr;
	private final Set<NvmeEndpoint> allEndpoints;

	public NvmfStorageServer(NvmeServerEndpoint serverEndpoint, InetSocketAddress datanodeAddr) {
		this.serverEndpoint = serverEndpoint;
		this.datanodeAddr = datanodeAddr;
		this.allEndpoints = ConcurrentHashMap.newKeySet();
	}

	public void close(NvmeEndpoint ep) {
		try {
			allEndpoints.remove(ep);
			LOG.info("removing endpoint, connCount " + allEndpoints.size());
		} catch (Exception e){
			LOG.info("error closing " + e.getMessage());
		}
	}

	public void run() {
		try {
			LOG.info("RdmaDataNodeServer started at " + datanodeAddr);
			while(true){
				NvmeEndpoint clientEndpoint = serverEndpoint.accept();
				allEndpoints.add(clientEndpoint);
				LOG.info("accepting client connection, conncount " + allEndpoints.size());
			}
		} catch(Exception e){
			e.printStackTrace();
		}
	}
}
