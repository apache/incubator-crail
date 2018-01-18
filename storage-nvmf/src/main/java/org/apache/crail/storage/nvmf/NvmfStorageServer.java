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

package org.apache.crail.storage.nvmf;

import com.ibm.disni.nvmef.NvmeEndpoint;
import com.ibm.disni.nvmef.NvmeEndpointGroup;
import com.ibm.disni.nvmef.spdk.NvmeTransportType;

import org.apache.crail.conf.CrailConfiguration;
import org.apache.crail.storage.StorageResource;
import org.apache.crail.storage.StorageServer;
import org.apache.crail.utils.CrailUtils;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;

public class NvmfStorageServer implements StorageServer {
	private static final Logger LOG = CrailUtils.getLogger();

	private boolean isAlive;
	private long alignedSize;
	private long offset;
	private boolean initialized = false;
	private NvmeEndpoint endpoint;

	public NvmfStorageServer() {}
	
	public void init(CrailConfiguration crailConfiguration, String[] args) throws Exception {
		if (initialized) {
			throw new IOException("NvmfStorageTier already initialized");
		}
		initialized = true;
		NvmfStorageConstants.parseCmdLine(crailConfiguration, args);

		NvmeEndpointGroup group = new NvmeEndpointGroup(new NvmeTransportType[]{NvmeTransportType.RDMA}, NvmfStorageConstants.SERVER_MEMPOOL);
		endpoint = group.createEndpoint();

		URI uri = new URI("nvmef://" + NvmfStorageConstants.IP_ADDR.getHostAddress() + ":" + NvmfStorageConstants.PORT +
					"/0/" + NvmfStorageConstants.NAMESPACE + "?subsystem=" + NvmfStorageConstants.NQN);
		endpoint.connect(uri);

		long namespaceSize = endpoint.getNamespaceSize();
		alignedSize = namespaceSize - (namespaceSize % NvmfStorageConstants.ALLOCATION_SIZE);
		offset = 0;

		isAlive = true;
	}	

	@Override
	public void printConf(Logger log) {
		NvmfStorageConstants.printConf(log);		
	}

	public void run() {
		LOG.info("NnvmfStorageServer started with NVMf target " + getAddress());
		while (isAlive) {
			try {
				Thread.sleep(1000 /* ms */);
				endpoint.keepAlive();
			} catch (Exception e) {
				e.printStackTrace();
				isAlive = false;
			}
		}
	}

	@Override
	public StorageResource allocateResource() throws Exception {
		StorageResource resource = null;
		
		if (alignedSize > 0){
			LOG.info("new block, length " + NvmfStorageConstants.ALLOCATION_SIZE);
			LOG.debug("block stag 0, offset " + offset + ", length " + NvmfStorageConstants.ALLOCATION_SIZE);
			alignedSize -= NvmfStorageConstants.ALLOCATION_SIZE;
			resource = StorageResource.createResource(offset, (int)NvmfStorageConstants.ALLOCATION_SIZE, 0);
			offset += NvmfStorageConstants.ALLOCATION_SIZE;
		}
		
		return resource;
	}

	@Override
	public InetSocketAddress getAddress() {
		return new InetSocketAddress(NvmfStorageConstants.IP_ADDR, NvmfStorageConstants.PORT);
	}

	@Override
	public boolean isAlive() {
		return isAlive;
	}
}
