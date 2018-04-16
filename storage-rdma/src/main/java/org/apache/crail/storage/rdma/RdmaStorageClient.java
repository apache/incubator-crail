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

package org.apache.crail.storage.rdma;

import java.io.IOException;

import org.apache.crail.CrailBufferCache;
import org.apache.crail.CrailStatistics;
import org.apache.crail.conf.CrailConfiguration;
import org.apache.crail.metadata.DataNodeInfo;
import org.apache.crail.storage.StorageClient;
import org.apache.crail.storage.StorageEndpoint;
import org.apache.crail.storage.rdma.client.RdmaStorageActiveEndpointFactory;
import org.apache.crail.storage.rdma.client.RdmaStorageActiveGroup;
import org.apache.crail.storage.rdma.client.RdmaStoragePassiveEndpointFactory;
import org.apache.crail.storage.rdma.client.RdmaStoragePassiveGroup;
import org.apache.crail.utils.CrailUtils;
import org.slf4j.Logger;

public class RdmaStorageClient implements StorageClient {
	private static final Logger LOG = CrailUtils.getLogger();

	private MrCache clientMrCache = null;
	private RdmaStorageGroup clientGroup = null;

	public RdmaStorageClient(){
		this.clientGroup = null;
		this.clientMrCache = null;
	}

	public void init(CrailStatistics statistics, CrailBufferCache bufferCache, CrailConfiguration conf, String[] args)
			throws IOException {
		RdmaConstants.init(conf, args);
	}

	public void printConf(Logger logger){
		RdmaConstants.printConf(logger);
	}

	@Override
	public StorageEndpoint createEndpoint(DataNodeInfo info) throws IOException {
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
						RdmaStoragePassiveGroup _endpointGroup = new RdmaStoragePassiveGroup(100, RdmaConstants.STORAGE_RDMA_QUEUESIZE, 4, RdmaConstants.STORAGE_RDMA_QUEUESIZE*2, clientMrCache);
						_endpointGroup.init(new RdmaStoragePassiveEndpointFactory(_endpointGroup));
						this.clientGroup = _endpointGroup;
					} else {
						LOG.info("active data client ");
						RdmaStorageActiveGroup _endpointGroup = new RdmaStorageActiveGroup(100, false, RdmaConstants.STORAGE_RDMA_QUEUESIZE, 4, RdmaConstants.STORAGE_RDMA_QUEUESIZE*2, clientMrCache);
						_endpointGroup.init(new RdmaStorageActiveEndpointFactory(_endpointGroup));
						this.clientGroup = _endpointGroup;
					}
				}
			}
		}

		return clientGroup.createEndpoint(info);
	}


	public void close() throws Exception {
		if (clientGroup != null){
			this.clientGroup.close();
		}
	}
}
