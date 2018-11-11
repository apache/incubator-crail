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

package org.apache.crail.storage.rdma.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;

import org.apache.crail.metadata.DataNodeInfo;
import org.apache.crail.storage.StorageEndpoint;
import org.apache.crail.storage.rdma.MrCache;
import org.apache.crail.storage.rdma.RdmaConstants;
import org.apache.crail.storage.rdma.RdmaStorageGroup;
import org.apache.crail.utils.CrailUtils;

import com.ibm.disni.*;

public class RdmaStoragePassiveGroup extends RdmaPassiveEndpointGroup<RdmaStoragePassiveEndpoint> implements RdmaStorageGroup {
	private HashMap<InetSocketAddress, RdmaStorageLocalEndpoint> localCache;
	private MrCache mrCache;

	public RdmaStoragePassiveGroup(int timeout, int maxWR, int maxSge, int cqSize, MrCache mrCache)
			throws IOException {
		super(timeout, maxWR, maxSge, cqSize);
		try {
			this.mrCache = mrCache;
			this.localCache = new HashMap<InetSocketAddress, RdmaStorageLocalEndpoint>();
		} catch(Exception e){
			throw new IOException(e);
		}
	}
	
	public StorageEndpoint createEndpoint(DataNodeInfo info) throws IOException {
		try {
			return createEndpoint(CrailUtils.datanodeInfo2SocketAddr(info));
		} catch(Exception e){
			throw new IOException(e);
		}
	}	

	public StorageEndpoint createEndpoint(InetSocketAddress inetAddress) throws Exception {
		if (RdmaConstants.STORAGE_RDMA_LOCAL_MAP && CrailUtils.isLocalAddress(inetAddress.getAddress())){
			RdmaStorageLocalEndpoint localEndpoint = localCache.get(inetAddress.getAddress());
			if (localEndpoint == null){
				localEndpoint = new RdmaStorageLocalEndpoint(inetAddress);
				localCache.put(inetAddress, localEndpoint);
			}
			return localEndpoint;
		} 
		
		RdmaStoragePassiveEndpoint endpoint = super.createEndpoint();
		endpoint.connect(inetAddress, RdmaConstants.STORAGE_RDMA_CONNECTTIMEOUT);
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
