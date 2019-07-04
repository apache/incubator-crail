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

import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.crail.conf.CrailConfiguration;
import org.apache.crail.storage.StorageResource;
import org.apache.crail.storage.StorageServer;
import org.apache.crail.storage.StorageUtils;
import org.apache.crail.utils.CrailUtils;
import org.slf4j.Logger;

import com.ibm.disni.*;
import com.ibm.disni.verbs.IbvMr;

public class RdmaStorageServer implements Runnable, StorageServer {
	private static final Logger LOG = CrailUtils.getLogger();
	
	private InetSocketAddress serverAddr;
	private RdmaActiveEndpointGroup<RdmaStorageServerEndpoint> datanodeGroup;
	private RdmaServerEndpoint<RdmaStorageServerEndpoint> datanodeServerEndpoint;
	private ConcurrentHashMap<Integer, RdmaEndpoint> allEndpoints; 
	private boolean isAlive;
	
	private String dataDirPath;
	private long allocatedSize;
	private int fileCount;
	
	public RdmaStorageServer() throws Exception {
		this.isAlive = false;
		this.serverAddr = null;
		this.datanodeGroup = null;
		this.datanodeServerEndpoint = null;
		this.allEndpoints = new ConcurrentHashMap<Integer, RdmaEndpoint>();
	}
	
	public void init(CrailConfiguration conf, String[] args) throws Exception {
		RdmaConstants.init(conf, args);

		this.serverAddr = StorageUtils.getDataNodeAddress(RdmaConstants.STORAGE_RDMA_INTERFACE, RdmaConstants.STORAGE_RDMA_PORT);
		if (serverAddr == null){
			LOG.info("Configured network interface " + RdmaConstants.STORAGE_RDMA_INTERFACE + " cannot be found..exiting!!!");
			return;
		}
		this.datanodeGroup = new RdmaActiveEndpointGroup<RdmaStorageServerEndpoint>(-1, false, 1, 1, 1);
		this.datanodeServerEndpoint = datanodeGroup.createServerEndpoint();
		datanodeGroup.init(new RdmaStorageEndpointFactory(datanodeGroup, this));
		datanodeServerEndpoint.bind(serverAddr, RdmaConstants.STORAGE_RDMA_BACKLOG);
		
		this.allocatedSize = 0;
		this.fileCount = 0;
		this.dataDirPath = StorageUtils.getDatanodeDirectory(RdmaConstants.STORAGE_RDMA_DATA_PATH, serverAddr);
		if (!RdmaConstants.STORAGE_RDMA_PERSISTENT){
			StorageUtils.clean(RdmaConstants.STORAGE_RDMA_DATA_PATH, dataDirPath);		
		} 
	}
	
	public void printConf(Logger logger){
		RdmaConstants.printConf(logger);
	}

	public void close(RdmaEndpoint ep) {
		try {
			allEndpoints.remove(ep.getEndpointId());
			ep.close();
			LOG.info("removing endpoint, connCount " + allEndpoints.size());
		} catch (Exception e){
			LOG.info("error closing " + e.getMessage());
		}
	}	
	
	@Override
	public StorageResource allocateResource() throws Exception {
		StorageResource resource = null;
		
		if (allocatedSize < RdmaConstants.STORAGE_RDMA_STORAGE_LIMIT){
			//mmap buffer
			int fileId = fileCount++;
			String dataFilePath = dataDirPath + "/" + fileId;
			RandomAccessFile dataFile = new RandomAccessFile(dataFilePath, "rw");
			if (!RdmaConstants.STORAGE_RDMA_PERSISTENT){
				dataFile.setLength(RdmaConstants.STORAGE_RDMA_ALLOCATION_SIZE);
			}
			FileChannel dataChannel = dataFile.getChannel();
			ByteBuffer dataBuffer = dataChannel.map(MapMode.READ_WRITE, 0, RdmaConstants.STORAGE_RDMA_ALLOCATION_SIZE);
			dataFile.close();
			dataChannel.close();

			//register buffer
			allocatedSize += dataBuffer.capacity();
			IbvMr mr = datanodeServerEndpoint.registerMemory(dataBuffer).execute().free().getMr();

			//create resource
			resource = StorageResource.createResource(mr.getAddr(), mr.getLength(), mr.getLkey());
		}
		
		return resource;
	}

	@Override
	public void run() {
		try {
			this.isAlive = true;
			LOG.info("rdma storage server started, address " + serverAddr + ", persistent " + RdmaConstants.STORAGE_RDMA_PERSISTENT + ", maxWR " + datanodeGroup.getMaxWR() + ", maxSge " + datanodeGroup.getMaxSge() + ", cqSize " + datanodeGroup.getCqSize());
			while(true){
				RdmaEndpoint clientEndpoint = datanodeServerEndpoint.accept();
				allEndpoints.put(clientEndpoint.getEndpointId(), clientEndpoint);
				LOG.info("accepting client connection, conncount " + allEndpoints.size());
			}			
		} catch(Exception e){
			e.printStackTrace();
		}
		this.isAlive = false;
	}

	@Override
	public InetSocketAddress getAddress() {
		return serverAddr;
	}	
	
	@Override
	public boolean isAlive() {
		return isAlive;
	}
}
