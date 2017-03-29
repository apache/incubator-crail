/*
 * Crail: A Multi-tiered Distributed Direct Access File System
 *
 * Author: Patrick Stuedi <stu@zurich.ibm.com>
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

package com.ibm.crail.storage.rdma;

import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;

import com.ibm.crail.utils.CrailUtils;
import com.ibm.disni.rdma.*;

public class RdmaStorageServer implements Runnable {
	private static final Logger LOG = CrailUtils.getLogger();
	
	private InetSocketAddress datanodeAddr;
	private RdmaServerEndpoint<RdmaStorageServerEndpoint> datanodeServerEndpoint;
	private ConcurrentHashMap<Integer, RdmaEndpoint> allEndpoints; 
	
	public RdmaStorageServer(RdmaServerEndpoint<RdmaStorageServerEndpoint> serverEndpoint, InetSocketAddress datanodeAddr) {
		this.datanodeAddr = datanodeAddr;
		this.datanodeServerEndpoint = serverEndpoint;
		this.allEndpoints = new ConcurrentHashMap<Integer, RdmaEndpoint>();
	}

	public void close(RdmaEndpoint ep) {
		try {
			allEndpoints.remove(ep.getEndpointId());
			LOG.info("removing endpoint, connCount " + allEndpoints.size());
		} catch (Exception e){
			LOG.info("error closing " + e.getMessage());
		}
	}		
	
	@Override
	public void run() {
		try {
			LOG.info("RdmaDataNodeServer started at " + datanodeAddr);
			while(true){
				RdmaEndpoint clientEndpoint = datanodeServerEndpoint.accept();
				allEndpoints.put(clientEndpoint.getEndpointId(), clientEndpoint);
				LOG.info("accepting client connection, conncount " + allEndpoints.size());
			}			
		} catch(Exception e){
			e.printStackTrace();
		}
	}

	public RdmaServerEndpoint<RdmaStorageServerEndpoint> getDatanodeServerEndpoint() {
		return datanodeServerEndpoint;
	}

}
