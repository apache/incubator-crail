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

import com.ibm.crail.metadata.DataNodeStatistics;
import com.ibm.crail.storage.StorageRpcClient;
import com.ibm.crail.storage.StorageServer;
import com.ibm.crail.utils.CrailUtils;
import com.ibm.disni.nvmef.NvmeEndpoint;
import com.ibm.disni.nvmef.NvmeEndpointGroup;
import com.ibm.disni.nvmef.NvmeServerEndpoint;
import com.ibm.disni.nvmef.spdk.NvmeController;
import com.ibm.disni.nvmef.spdk.NvmeNamespace;
import com.ibm.disni.nvmef.spdk.NvmeTransportType;

import org.slf4j.Logger;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class NvmfStorageServer implements Runnable, StorageServer {
	private static final Logger LOG = CrailUtils.getLogger();

	private final NvmeEndpointGroup group;
	private final NvmeServerEndpoint serverEndpoint;
	private final Set<NvmeEndpoint> allEndpoints;
	private boolean isAlive;

	public NvmfStorageServer() throws Exception {
		this.allEndpoints = ConcurrentHashMap.newKeySet();
		this.group = new NvmeEndpointGroup(new NvmeTransportType[]{NvmeTransportType.PCIE, NvmeTransportType.RDMA}, NvmfStorageConstants.HUGEDIR, NvmfStorageConstants.SOCKETMEM);
		this.serverEndpoint = group.createServerEndpoint();
		URI url = new URI("nvmef://" + NvmfStorageConstants.IP_ADDR.getHostAddress() + ":" + NvmfStorageConstants.PORT + "/0/1?subsystem=nqn.2016-06.io.spdk:cnode1&pci=" + NvmfStorageConstants.PCIE_ADDR);
		serverEndpoint.bind(url);
		this.isAlive = false;
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
			this.isAlive = true;
			LOG.info("RdmaDataNodeServer started at " + this.getAddress());
			while(true){
				NvmeEndpoint clientEndpoint = serverEndpoint.accept();
				allEndpoints.add(clientEndpoint);
				LOG.info("accepting client connection, conncount " + allEndpoints.size());
			}
		} catch(Exception e){
			e.printStackTrace();
		}
		this.isAlive = false;
	}

	@Override
	public void registerResources(StorageRpcClient client) throws Exception {
		NvmeController controller = serverEndpoint.getNvmecontroller();
		NvmeNamespace namespace = controller.getNamespace(NvmfStorageConstants.NAMESPACE);
		long namespaceSize = namespace.getSize();
		long alignedSize = namespaceSize - (namespaceSize % NvmfStorageConstants.ALLOCATION_SIZE);


		long addr = 0;
		while (alignedSize > 0) {
			DataNodeStatistics statistics = client.getDataNode();
			LOG.info("datanode statistics, freeBlocks " + statistics.getFreeBlockCount());

			LOG.info("new block, length " + NvmfStorageConstants.ALLOCATION_SIZE);
			LOG.debug("block stag 0, addr " + addr + ", length " + NvmfStorageConstants.ALLOCATION_SIZE);
			alignedSize -= NvmfStorageConstants.ALLOCATION_SIZE;
			client.setBlock(addr, (int)NvmfStorageConstants.ALLOCATION_SIZE, 0);
			addr += NvmfStorageConstants.ALLOCATION_SIZE;
		}
	}

	@Override
	public void join() throws Exception {
		this.join();
	}

	@Override
	public InetSocketAddress getAddress() {
		return new InetSocketAddress(NvmfStorageConstants.IP_ADDR, NvmfStorageConstants.PORT);
	}

	@Override
	public boolean isAlive() {
		return this.isAlive;
	}
}
