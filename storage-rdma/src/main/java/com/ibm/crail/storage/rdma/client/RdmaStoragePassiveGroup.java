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

package com.ibm.crail.storage.rdma.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;

import com.ibm.crail.storage.StorageEndpoint;
import com.ibm.crail.storage.rdma.MrCache;
import com.ibm.crail.storage.rdma.RdmaConstants;
import com.ibm.crail.storage.rdma.RdmaStorageTier;
import com.ibm.crail.storage.rdma.RdmaStorageGroup;
import com.ibm.crail.utils.CrailUtils;
import com.ibm.disni.rdma.*;

public class RdmaStoragePassiveGroup extends RdmaPassiveEndpointGroup<RdmaStoragePassiveEndpoint> implements RdmaStorageGroup {
	private RdmaStorageLocalEndpoint localEndpoint;
	private MrCache mrCache;

	public RdmaStoragePassiveGroup(int timeout, int maxWR, int maxSge, int cqSize, MrCache mrCache)
			throws IOException {
		super(timeout, maxWR, maxSge, cqSize);
		try {
			this.mrCache = mrCache;
			InetSocketAddress datanodeAddr = RdmaStorageTier.getDataNodeAddress();
			if (datanodeAddr != null){
				this.localEndpoint = new RdmaStorageLocalEndpoint(datanodeAddr);
			} else {
				this.localEndpoint = null;
			}
		} catch(Exception e){
			throw new IOException(e);
		}
	}

	public StorageEndpoint createEndpoint(InetSocketAddress inetAddress) throws IOException {
		if (RdmaConstants.DATANODE_RDMA_LOCAL_MAP && CrailUtils.isLocalAddress(inetAddress.getAddress())){
			return this.localEndpoint;
		} 
		RdmaStoragePassiveEndpoint endpoint = super.createEndpoint();
		try {
			URI uri = URI.create("rdma://" + inetAddress.getAddress().getHostAddress() + ":" + inetAddress.getPort());
			endpoint.connect(uri);
		} catch(Exception e){
			throw new IOException(e);
		}
		return endpoint;
	}
	
	public int getType() {
		return 0;
	}	
	
	@Override
	public String toString() {
		return "maxWR " + getMaxWR() + ", maxSge " + getMaxSge() + ", cqSize " + getCqSize();
	}

	public MrCache getMrCache() {
		return mrCache;
	}	
}
