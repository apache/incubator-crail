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

package org.apache.crail.storage.object.server;

import org.apache.crail.conf.CrailConfiguration;
import org.apache.crail.conf.CrailConstants;
import org.apache.crail.storage.StorageResource;
import org.apache.crail.storage.StorageServer;
import org.apache.crail.storage.object.ObjectStoreConstants;
import org.apache.crail.storage.object.ObjectStoreUtils;
import org.apache.crail.storage.object.object.S3ObjectStoreClient;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;

public class ObjectStoreServer implements StorageServer {
	private static final Logger LOG = ObjectStoreUtils.getLogger();

	private InetSocketAddress datanodeAddr;
	private long blockID = 0;
	private S3ObjectStoreClient objectStoreClient = null;
	private boolean isAlive = false;
	private long allocated = 0;
	private long alignedSize;
	private int currentStag = 1;
	private boolean initialized;
	private long offset;

	public ObjectStoreServer() {
	}

	public void init(CrailConfiguration crailConfiguration, String[] args) throws Exception {
		if (initialized) {
			throw new IOException("NvmfStorageTier already initialized");
		}
		initialized = true;
		ObjectStoreConstants.parseCmdLine(crailConfiguration, args);
		this.alignedSize = ObjectStoreConstants.STORAGE_LIMIT -
				(ObjectStoreConstants.STORAGE_LIMIT % ObjectStoreConstants.ALLOCATION_SIZE);
		/*
		NvmeEndpointGroup group = new NvmeEndpointGroup(new NvmeTransportType[]{NvmeTransportType.RDMA}, NvmfStorageConstants.SERVER_MEMPOOL);
		endpoint = group.createEndpoint();

		URI uri = new URI("nvmef://" + NvmfStorageConstants.IP_ADDR.getHostAddress() + ":" + NvmfStorageConstants.PORT +
				"/0/" + NvmfStorageConstants.NAMESPACE + "?subsystem=" + NvmfStorageConstants.NQN);
		endpoint.connect(uri);

		long namespaceSize = endpoint.getNamespaceSize();
		alignedSize = namespaceSize - (namespaceSize % NvmfStorageConstants.ALLOCATION_SIZE);
		offset = 0;
		*/

		isAlive = true;
	}

	@Override
	public void printConf(Logger log) {
		log.info("TODO: dump all configuration");
	}

	@Override
	public StorageResource allocateResource() throws Exception {
		StorageResource res = null;
		LOG.info("Allocating object store blocks");
		if (allocated < alignedSize) {
			long addr = allocated;
			allocated += ObjectStoreConstants.ALLOCATION_SIZE;
			res = StorageResource.createResource(addr, (int) ObjectStoreConstants.ALLOCATION_SIZE, currentStag);
			blockID += ObjectStoreConstants.ALLOCATION_SIZE / CrailConstants.BLOCK_SIZE;
			double sizeGBs = ObjectStoreConstants.ALLOCATION_SIZE / (1024. * 1024. * 1024.);
			double perc = (allocated * 100.) / alignedSize;
			currentStag++;
			LOG.info("Allocation done : " + perc + "% , allocated " + allocated + " / " + alignedSize);
		}
		return res;
	}

	@Override
	public boolean isAlive() {
		return this.isAlive;
	}

	@Override
	public InetSocketAddress getAddress() {
		return new InetSocketAddress(ObjectStoreConstants.DATANODE, ObjectStoreConstants.DATANODE_PORT);
	}

	public void run() {
		LOG.info("ObjectStorageServer started at " + getAddress());
		while (isAlive) {
			try {
				Thread.sleep(1000 /* ms */);
				//endpoint.keepAlive();
			} catch (Exception e) {
				e.printStackTrace();
				isAlive = false;
			}
		}
	}

	protected void finalize() {
		LOG.info("Datanode finalize");
		try {
			close();
		} catch (Exception e) {
			LOG.error("Could not close ObjectStoreDataNode. Reason: {}", e);
		}
	}

	/*
	@Override
	public void run() {
		this.isAlive = true;
		try {
			int prevBlockCount = 0;
			//noinspection InfiniteLoopStatement
			while (this.isAlive) {
				if (rpcClient != null) {
					DataNodeStatistics statistics = rpcClient.getDataNode();
					int curBlockCount = statistics.getFreeBlockCount();
					if (prevBlockCount != curBlockCount) {
						LOG.info("Datanode statistics: freeBlocks = " + curBlockCount);
					}
					prevBlockCount = curBlockCount;
				}
				Thread.sleep(2000);
			}
		} catch(Exception e){
			e.printStackTrace();
		}
		this.isAlive = false;
	}
*/
	public void close() throws Exception {
		this.isAlive = false;
	}
}
