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

import java.io.IOException;
import java.net.InetSocketAddress;
import org.slf4j.Logger;
import com.ibm.crail.conf.CrailConfiguration;
import com.ibm.crail.storage.StorageTier;
import com.ibm.crail.storage.StorageEndpoint;
import com.ibm.crail.storage.rdma.client.RdmaStorageActiveEndpointFactory;
import com.ibm.crail.storage.rdma.client.RdmaStorageActiveGroup;
import com.ibm.crail.storage.rdma.client.RdmaStoragePassiveEndpointFactory;
import com.ibm.crail.storage.rdma.client.RdmaStoragePassiveGroup;
import com.ibm.crail.utils.CrailUtils;
import com.ibm.disni.util.*;

public class RdmaStorageTier extends StorageTier {
	private static final Logger LOG = CrailUtils.getLogger();
	
	private MrCache clientMrCache = null;
	private RdmaStorageGroup clientGroup = null;
	
	public RdmaStorageTier(){
		this.clientGroup = null;
		this.clientMrCache = null;
	}
	
	public void init(CrailConfiguration conf, String[] args) throws IOException{
		if (args != null){
			GetOpt go = new GetOpt(args, "i:p");
			int ch = -1;
			while ((ch = go.getopt()) != GetOpt.optEOF) {
				if ((char) ch == 'i') {
					String ifname = go.optArgGet();
					LOG.info("using custom interface " + ifname);
					conf.set(RdmaConstants.STORAGE_RDMA_INTERFACE_KEY, ifname);
				} else if ((char) ch == 'p') {
					String port = go.optArgGet();
					LOG.info("using custom port " + port);
					conf.set(RdmaConstants.STORAGE_RDMA_PORT_KEY, port);
				} 
			}		
		}
		
		RdmaConstants.updateConstants(conf);
		RdmaConstants.verify();		
	}
	
	public void printConf(Logger logger){
		RdmaConstants.printConf(logger);
	}	
	
	@Override
	public StorageEndpoint createEndpoint(InetSocketAddress inetAddress) throws IOException {
		if (clientMrCache == null){
			synchronized(this){
				if (clientMrCache == null){
					this.clientMrCache = new MrCache();
				}
			}
		}
		if (clientGroup == null){
			synchronized(this){
				if (clientGroup == null){
					if (RdmaConstants.STORAGE_RDMA_TYPE.equalsIgnoreCase("passive")){
						LOG.info("passive data client ");
						RdmaStoragePassiveGroup _endpointGroup = new RdmaStoragePassiveGroup(100, RdmaConstants.STORAGE_RDMA_QUEUESIZE, 4, RdmaConstants.STORAGE_RDMA_QUEUESIZE*2, clientMrCache, RdmaStorageServer.getDataNodeAddress());
						_endpointGroup.init(new RdmaStoragePassiveEndpointFactory(_endpointGroup));
						this.clientGroup = _endpointGroup;
					} else {
						LOG.info("active data client ");
						RdmaStorageActiveGroup _endpointGroup = new RdmaStorageActiveGroup(100, false, RdmaConstants.STORAGE_RDMA_QUEUESIZE, 4, RdmaConstants.STORAGE_RDMA_QUEUESIZE*2, clientMrCache, RdmaStorageServer.getDataNodeAddress());
						_endpointGroup.init(new RdmaStorageActiveEndpointFactory(_endpointGroup));
						this.clientGroup = _endpointGroup;
					}		
				}
			}
		}
		
		return clientGroup.createEndpoint(inetAddress);
	}


	public void close() throws Exception {
		if (clientGroup != null){
			this.clientGroup.close();
		}
	}	
	
	
	public RdmaStorageServer launchServer () throws Exception {
		RdmaStorageServer datanodeServer = new RdmaStorageServer();
		Thread dataNode = new Thread(datanodeServer);
		dataNode.start();
		
		return datanodeServer;
	}
}
