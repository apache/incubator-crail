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

package com.ibm.crail.storage.rdma;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;

import com.ibm.crail.conf.CrailConstants;
import com.ibm.crail.storage.StorageResource;
import com.ibm.crail.storage.StorageServer;
import com.ibm.crail.storage.rdma.client.RdmaBlockIndex;
import com.ibm.crail.utils.CrailUtils;
import com.ibm.disni.rdma.*;
import com.ibm.disni.rdma.verbs.IbvMr;

public class RdmaStorageServer implements Runnable, StorageServer {
	private static final Logger LOG = CrailUtils.getLogger();
	
	private InetSocketAddress serverAddr;
	private RdmaPassiveEndpointGroup<RdmaStorageServerEndpoint> datanodeGroup;
	private RdmaServerEndpoint<RdmaStorageServerEndpoint> datanodeServerEndpoint;
	private ConcurrentHashMap<Integer, RdmaEndpoint> allEndpoints; 
	private boolean isAlive;
	
	private String dataDirPath;
	private String indexDirPath;
	private long allocatedSize;
	private int fileCount;
	private ByteBuffer fileBuffer;
	
	public RdmaStorageServer() throws Exception {
		this.isAlive = false;
		this.serverAddr = getDataNodeAddress();
		if (serverAddr == null){
			LOG.info("Configured network interface " + RdmaConstants.STORAGE_RDMA_INTERFACE + " cannot be found..exiting!!!");
			return;
		}
		this.allEndpoints = new ConcurrentHashMap<Integer, RdmaEndpoint>();
		URI uri = URI.create("rdma://" + serverAddr.getAddress().getHostAddress() + ":" + serverAddr.getPort());
		this.datanodeGroup = new RdmaPassiveEndpointGroup<RdmaStorageServerEndpoint>(-1, RdmaConstants.STORAGE_RDMA_QUEUESIZE, 4, RdmaConstants.STORAGE_RDMA_QUEUESIZE*100);
		this.datanodeServerEndpoint = datanodeGroup.createServerEndpoint();		
		datanodeGroup.init(new RdmaStorageEndpointFactory(datanodeGroup, this));
		datanodeServerEndpoint.bind(uri);
		LOG.info("RdmaDataNode started, maxWR " + datanodeGroup.getMaxWR() + ", maxSge " + datanodeGroup.getMaxSge() + ", cqSize " + datanodeGroup.getCqSize());
		this.dataDirPath = getDatanodeDirectory(serverAddr);
		this.indexDirPath = getIndexDirectory(serverAddr);
		LOG.info("dataPath " + dataDirPath + ", indexPath " + indexDirPath);
		this.allocatedSize = 0;
		this.fileCount = 0;
		this.fileBuffer = ByteBuffer.allocateDirect(CrailConstants.BUFFER_SIZE);
		clean();
	}
	
	private void clean(){
		File dataDir = new File(dataDirPath);
		if (!dataDir.exists()){
			dataDir.mkdirs();
		}
		for (File child : dataDir.listFiles()) {
			child.delete();
		}

		File indexDir = new File(indexDirPath);
		if (!indexDir.exists()){
			indexDir.mkdirs();
		}
		for (File child : indexDir.listFiles()) {
			child.delete();
		}			
		LOG.info("crail data/index directories cleaned");			
	}
	
	public void close(RdmaEndpoint ep) {
		try {
			allEndpoints.remove(ep.getEndpointId());
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
			dataFile.setLength(RdmaConstants.STORAGE_RDMA_ALLOCATION_SIZE);
			FileChannel dataChannel = dataFile.getChannel();
			ByteBuffer dataBuffer = dataChannel.map(MapMode.READ_WRITE, 0, RdmaConstants.STORAGE_RDMA_ALLOCATION_SIZE);
			dataFile.close();
			dataChannel.close();

			//register buffer
			allocatedSize += dataBuffer.capacity();
			IbvMr mr = datanodeServerEndpoint.registerMemory(dataBuffer).execute().free().getMr();
			
			//write index file
			String indexFilePath = indexDirPath + "/" + mr.getLkey();
			File indexFile = new File(indexFilePath);
			FileOutputStream indexStream = new FileOutputStream(indexFile);
			FileChannel indexChannel = indexStream.getChannel();
			RdmaBlockIndex blockIndex = new RdmaBlockIndex(mr.getLkey(), mr.getAddr(), dataFilePath);
			fileBuffer.clear();
			blockIndex.write(fileBuffer);
			fileBuffer.flip();
			indexChannel.write(fileBuffer);
			indexChannel.close();
			indexStream.close();	
			
			resource = StorageResource.createResource(mr.getAddr(), mr.getLength(), mr.getLkey());
		}
		
		return resource;
	}

	@Override
	public void run() {
		try {
			this.isAlive = true;
			LOG.info("RdmaDataNodeServer started at " + serverAddr);
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
	
	//--------------------
	
	public static String getDatanodeDirectory(InetSocketAddress inetAddress){
		String address = inetAddress.getAddress().toString();
		if (address.startsWith("/")){
			return RdmaConstants.STORAGE_RDMA_DATA_PATH + address + "-"  + inetAddress.getPort();
		} else {
			return RdmaConstants.STORAGE_RDMA_DATA_PATH + address + "-"  + inetAddress.getPort();
		}
	}
	
	public static String getIndexDirectory(InetSocketAddress inetAddress){
		String address = inetAddress.getAddress().toString();
		if (address.startsWith("/")){
			return RdmaConstants.STORAGE_RDMA_INDEX_PATH + address + "-"  + inetAddress.getPort();
		} else {
			return RdmaConstants.STORAGE_RDMA_INDEX_PATH + address + "-"  + inetAddress.getPort();
		}
	}
	
	public static InetSocketAddress getDataNodeAddress() throws IOException {
		String ifname = RdmaConstants.STORAGE_RDMA_INTERFACE;
		int port = RdmaConstants.STORAGE_RDMA_PORT;
		
		NetworkInterface netif = NetworkInterface.getByName(ifname);
		if (netif == null){
			return null;
		}
		List<InterfaceAddress> addresses = netif.getInterfaceAddresses();
		InetAddress addr = null;
		for (InterfaceAddress address: addresses){
//			LOG.info("address* " + address.toString() + ", _addr " + _addr.toString() + ", isSiteLocal " + _addr.isSiteLocalAddress() + ", tmp " + tmp + ", size " + tmp.length + ", broadcast " + address.getBroadcast());
			if (address.getBroadcast() != null){
				InetAddress _addr = address.getAddress();
				addr = _addr;
			}
		}		
		InetSocketAddress inetAddr = new InetSocketAddress(addr, port);
		return inetAddr;
	}
	
	@Override
	public boolean isAlive() {
		return isAlive;
	}
}
