package com.ibm.crail.storage.nvmf;

import java.io.IOException;

import org.slf4j.Logger;

import com.ibm.crail.conf.CrailConfiguration;
import com.ibm.crail.metadata.DataNodeInfo;
import com.ibm.crail.storage.StorageClient;
import com.ibm.crail.storage.StorageEndpoint;
import com.ibm.crail.storage.nvmf.client.NvmfStorageEndpoint;
import com.ibm.crail.utils.CrailUtils;
import com.ibm.disni.nvmef.NvmeEndpointGroup;
import com.ibm.disni.nvmef.spdk.NvmeTransportType;

public class NvmfStorageClient implements StorageClient {
	private static final Logger LOG = CrailUtils.getLogger();
	private static NvmeEndpointGroup clientGroup;
	private boolean initialized = false;

	public void init(CrailConfiguration crailConfiguration, String[] args) throws IOException {
		if (initialized) {
			throw new IOException("NvmfStorageTier already initialized");
		}
		initialized = true;
		
		NvmfStorageConstants.init(crailConfiguration, args);
	}

	public void printConf(Logger logger) {
		NvmfStorageConstants.printConf(logger);
	}

	public static NvmeEndpointGroup getEndpointGroup() {
		if (clientGroup == null) {
			clientGroup = new NvmeEndpointGroup(new NvmeTransportType[]{NvmeTransportType.RDMA},
					NvmfStorageConstants.HUGEDIR,
					NvmfStorageConstants.CLIENT_MEMPOOL);
		}
		return clientGroup;
	}

	public synchronized StorageEndpoint createEndpoint(DataNodeInfo info) throws IOException {
		return new NvmfStorageEndpoint(getEndpointGroup(), CrailUtils.datanodeInfo2SocketAddr(info));
	}
	
	public void close() throws Exception {
	}	

}
