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

package org.apache.crail.storage.nvmf;

import com.ibm.jnvmf.*;
import org.apache.crail.conf.CrailConfiguration;
import org.apache.crail.storage.StorageResource;
import org.apache.crail.storage.StorageServer;
import org.apache.crail.utils.CrailUtils;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;

public class NvmfStorageServer implements StorageServer {
	private static final Logger LOG = CrailUtils.getLogger();

	private boolean isAlive;
	private long alignedSize;
	private long address;
	private boolean initialized = false;
	private Controller controller;

	public NvmfStorageServer() {}

	public void init(CrailConfiguration crailConfiguration, String[] args) throws Exception {
		if (initialized) {
			throw new IOException("NvmfStorageTier already initialized");
		}
		initialized = true;
		NvmfStorageConstants.parseCmdLine(crailConfiguration, args);

		Nvme nvme;
		if (NvmfStorageConstants.HOST_NQN == null) {
			nvme = new Nvme();
		} else {
			nvme = new Nvme(NvmfStorageConstants.HOST_NQN);
		}

		NvmfTransportId transportId = new NvmfTransportId(
				new InetSocketAddress(NvmfStorageConstants.IP_ADDR, NvmfStorageConstants.PORT),
				NvmfStorageConstants.NQN);
		controller = nvme.connect(transportId);
		controller.getControllerConfiguration().setEnable(true);
		controller.syncConfiguration();
		controller.waitUntilReady();

		List<Namespace> namespaces = controller.getActiveNamespaces();
		Namespace namespace = null;
		for (Namespace n : namespaces) {
			if (n.getIdentifier().equals(NvmfStorageConstants.NAMESPACE)) {
				namespace = n;
				break;
			}
		}
		if (namespace == null) {
			throw new IllegalArgumentException("No namespace with id " + NvmfStorageConstants.NAMESPACE +
					" at controller " + transportId.toString());
		}
		IdentifyNamespaceData namespaceData = namespace.getIdentifyNamespaceData();
		LbaFormat lbaFormat = namespaceData.getFormattedLbaSize();
		int dataSize = lbaFormat.getLbaDataSize().toInt();
		long namespaceSize = dataSize * namespaceData.getNamespaceCapacity();
		alignedSize = namespaceSize - (namespaceSize % NvmfStorageConstants.ALLOCATION_SIZE);
		address = 0;

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
				Thread.sleep(NvmfStorageConstants.KEEP_ALIVE_INTERVAL_MS);
				controller.keepAlive();
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
			LOG.debug("block stag 0, address " + address + ", length " + NvmfStorageConstants.ALLOCATION_SIZE);
			alignedSize -= NvmfStorageConstants.ALLOCATION_SIZE;
			resource = StorageResource.createResource(address, NvmfStorageConstants.ALLOCATION_SIZE, 0);
			address += NvmfStorageConstants.ALLOCATION_SIZE;
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
