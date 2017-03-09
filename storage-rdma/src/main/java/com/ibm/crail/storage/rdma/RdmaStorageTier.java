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

import org.slf4j.Logger;

import com.ibm.crail.conf.CrailConfiguration;
import com.ibm.crail.conf.CrailConstants;
import com.ibm.crail.namenode.protocol.DataNodeStatistics;
import com.ibm.crail.storage.StorageTier;
import com.ibm.crail.storage.StorageEndpoint;
import com.ibm.crail.storage.rdma.client.RdmaBlockIndex;
import com.ibm.crail.storage.rdma.client.RdmaStorageActiveEndpointFactory;
import com.ibm.crail.storage.rdma.client.RdmaStorageActiveGroup;
import com.ibm.crail.storage.rdma.client.RdmaStoragePassiveEndpointFactory;
import com.ibm.crail.storage.rdma.client.RdmaStoragePassiveGroup;
import com.ibm.crail.utils.CrailUtils;
import com.ibm.disni.rdma.*;
import com.ibm.disni.util.*;
import com.ibm.disni.rdma.verbs.IbvMr;

public class RdmaStorageTier extends StorageTier {
	private static final Logger LOG = CrailUtils.getLogger();
	
	//server-side
	private InetSocketAddress serverAddr;
	
	//client-side
	private MrCache clientMrCache = null;
	private RdmaStorageGroup clientGroup = null;
	
	public RdmaStorageTier(){
		this.serverAddr = null;
		this.clientGroup = null;
		this.clientMrCache = null;
	}
	
	public void init(CrailConfiguration conf, String[] args) throws IOException{
		if (args != null){
			GetOpt go = new GetOpt(args, "i:p");
			int ch = -1;
			while ((ch = go.getopt()) != GetOpt.optEOF) {
				if ((char) ch == 'i') {
					String ifname = go.optArgGet();
					LOG.info("using custom interface " + ifname);
					conf.set(RdmaConstants.DATANODE_RDMA_INTERFACE_KEY, ifname);
				} else if ((char) ch == 'p') {
					String port = go.optArgGet();
					LOG.info("using custom port " + port);
					conf.set(RdmaConstants.DATANODE_RDMA_PORT_KEY, port);
				} 
			}		
		}
		
		RdmaConstants.updateConstants(conf);
		RdmaConstants.verify();		
	}
	
	public void printConf(Logger logger){
		RdmaConstants.printConf(logger);
	}
	
	@Override
	public StorageEndpoint createEndpoint(InetSocketAddress inetAddress)
			throws IOException {
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
					if (RdmaConstants.DATANODE_RDMA_TYPE.equalsIgnoreCase("passive")){
						LOG.info("passive data client ");
						RdmaStoragePassiveGroup _endpointGroup = new RdmaStoragePassiveGroup(100, RdmaConstants.DATANODE_RDMA_QUEUESIZE, 4, RdmaConstants.DATANODE_RDMA_QUEUESIZE*2, clientMrCache);
						_endpointGroup.init(new RdmaStoragePassiveEndpointFactory(_endpointGroup));
						this.clientGroup = _endpointGroup;
					} else {
						LOG.info("active data client ");
						RdmaStorageActiveGroup _endpointGroup = new RdmaStorageActiveGroup(100, false, RdmaConstants.DATANODE_RDMA_QUEUESIZE, 4, RdmaConstants.DATANODE_RDMA_QUEUESIZE*2, clientMrCache);
						_endpointGroup.init(new RdmaStorageActiveEndpointFactory(_endpointGroup));
						this.clientGroup = _endpointGroup;
					}		
				}
			}
		}
		
		return clientGroup.createEndpoint(inetAddress);
	}


	public void close() throws Exception {
		if (clientGroup != null){
			this.clientGroup.close();
		}
	}	
	
	
	@Override
	public InetSocketAddress getAddress() {
		return serverAddr;
	}	
	
	public void run () throws Exception {
		this.serverAddr = getDataNodeAddress();
		if (serverAddr == null){
			LOG.info("Configured network interface " + RdmaConstants.DATANODE_RDMA_INTERFACE + " cannot be found..exiting!!!");
			return;
		}
		URI uri = URI.create("rdma://" + serverAddr.getAddress().getHostAddress() + ":" + serverAddr.getPort());
		RdmaPassiveEndpointGroup<RdmaStorageServerEndpoint> datanodeGroup = new RdmaPassiveEndpointGroup<RdmaStorageServerEndpoint>(-1, RdmaConstants.DATANODE_RDMA_QUEUESIZE, 4, RdmaConstants.DATANODE_RDMA_QUEUESIZE*100);
		RdmaServerEndpoint<RdmaStorageServerEndpoint> datanodeServerEndpoint = datanodeGroup.createServerEndpoint();
		RdmaStorageServer datanodeServer = new RdmaStorageServer(datanodeServerEndpoint, serverAddr);
		
		try {
			datanodeGroup.init(new RdmaStorageEndpointFactory(datanodeGroup, datanodeServer));
			datanodeServerEndpoint.bind(uri);
		} catch(Exception e){
			LOG.info("######## port already occupied");
		}
		
		Thread dataNode = new Thread(datanodeServer);
		dataNode.start();
		
		try{		
			LOG.info("RdmaDataNode started, maxWR " + datanodeGroup.getMaxWR() + ", maxSge " + datanodeGroup.getMaxSge() + ", cqSize " + datanodeGroup.getCqSize());
			String dataDirPath = getDatanodeDirectory(serverAddr);
			String indexDirPath = getIndexDirectory(serverAddr);
			LOG.info("dataPath " + dataDirPath + ", indexPath " + indexDirPath);
			
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
			
			long allocatedSize = 0;
			int fileCount = 0;
			ByteBuffer fileBuffer = ByteBuffer.allocateDirect(CrailConstants.BUFFER_SIZE);
			while (true) {
				try {
					DataNodeStatistics statistics = this.getDataNode();
					LOG.info("datanode statistics, freeBlocks " + statistics.getFreeBlockCount());
					
					if (allocatedSize < RdmaConstants.DATANODE_RDMA_STORAGE_LIMIT){
						//mmap buffer
						int fileId = fileCount++;
						String dataFilePath = dataDirPath + "/" + fileId;
						RandomAccessFile dataFile = new RandomAccessFile(dataFilePath, "rw");
						dataFile.setLength(RdmaConstants.DATANODE_RDMA_ALLOCATION_SIZE);
						FileChannel dataChannel = dataFile.getChannel();
						ByteBuffer dataBuffer = dataChannel.map(MapMode.READ_WRITE, 0, RdmaConstants.DATANODE_RDMA_ALLOCATION_SIZE);
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
						
						//inform namenode
						this.setBlock(mr.getAddr(), mr.getLength(), mr.getLkey());
					} else {
						Thread.sleep(2000);
					}
				} catch(Exception e){
					LOG.info("Fatal error...exiting..");
					e.printStackTrace();
					System.exit(-1);
				}
 
			}
		} catch(Exception e){
			LOG.info("Fatal error...exiting..");
			e.printStackTrace();
			System.exit(-1);			
		}

		
		dataNode.join();
	}
	
	public static String getDatanodeDirectory(InetSocketAddress inetAddress){
		String address = inetAddress.getAddress().toString();
		if (address.startsWith("/")){
			return RdmaConstants.DATANODE_RDMA_DATA_PATH + address + "-"  + inetAddress.getPort();
		} else {
			return RdmaConstants.DATANODE_RDMA_DATA_PATH + address + "-"  + inetAddress.getPort();
		}
	}
	
	public static String getIndexDirectory(InetSocketAddress inetAddress){
		String address = inetAddress.getAddress().toString();
		if (address.startsWith("/")){
			return RdmaConstants.DATANODE_RDMA_INDEX_PATH + address + "-"  + inetAddress.getPort();
		} else {
			return RdmaConstants.DATANODE_RDMA_INDEX_PATH + address + "-"  + inetAddress.getPort();
		}
	}
	
	public static InetSocketAddress getDataNodeAddress() throws Exception {
		String ifname = RdmaConstants.DATANODE_RDMA_INTERFACE;
		int port = RdmaConstants.DATANODE_RDMA_PORT;
		
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
}
