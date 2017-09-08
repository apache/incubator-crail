package com.ibm.crail.storage.rdma;

import java.io.IOException;

import org.slf4j.Logger;

import com.ibm.crail.conf.CrailConfiguration;
import com.ibm.crail.metadata.DataNodeInfo;
import com.ibm.crail.storage.StorageClient;
import com.ibm.crail.storage.StorageEndpoint;
import com.ibm.crail.storage.rdma.client.RdmaStorageActiveEndpointFactory;
import com.ibm.crail.storage.rdma.client.RdmaStorageActiveGroup;
import com.ibm.crail.storage.rdma.client.RdmaStoragePassiveEndpointFactory;
import com.ibm.crail.storage.rdma.client.RdmaStoragePassiveGroup;
import com.ibm.crail.utils.CrailUtils;

public class RdmaStorageClient implements StorageClient {
	private static final Logger LOG = CrailUtils.getLogger();
	
	private MrCache clientMrCache = null;
	private RdmaStorageGroup clientGroup = null;
	
	public RdmaStorageClient(){
		this.clientGroup = null;
		this.clientMrCache = null;
	}
	
	public void init(CrailConfiguration conf, String[] args) throws Exception {
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
