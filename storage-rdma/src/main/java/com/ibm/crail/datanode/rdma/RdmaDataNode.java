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

package com.ibm.crail.datanode.rdma;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.List;

import org.slf4j.Logger;

import com.ibm.crail.conf.CrailConfiguration;
import com.ibm.crail.conf.CrailConstants;
import com.ibm.crail.datanode.DataNode;
import com.ibm.crail.datanode.DataNodeEndpoint;
import com.ibm.crail.datanode.rdma.client.RdmaBlockIndex;
import com.ibm.crail.datanode.rdma.client.RdmaDataNodeActiveEndpointFactory;
import com.ibm.crail.datanode.rdma.client.RdmaDataNodeActiveGroup;
import com.ibm.crail.datanode.rdma.client.RdmaDataNodePassiveEndpointFactory;
import com.ibm.crail.datanode.rdma.client.RdmaDataNodePassiveGroup;
import com.ibm.crail.namenode.protocol.DataNodeStatistics;
import com.ibm.crail.utils.CrailUtils;
import com.ibm.disni.endpoints.*;
import com.ibm.disni.util.*;
import com.ibm.disni.verbs.IbvMr;

public class RdmaDataNode extends DataNode {
	private static final Logger LOG = CrailUtils.getLogger();
	
	//server-side
	private InetSocketAddress serverAddr;
	
	//client-side
	private MrCache clientMrCache = null;
	private RdmaDataNodeGroup clientGroup = null;
	
	public RdmaDataNode(){
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
	public DataNodeEndpoint createEndpoint(InetSocketAddress inetAddress)
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
						RdmaDataNodePassiveGroup _endpointGroup = new RdmaDataNodePassiveGroup(100, RdmaConstants.DATANODE_RDMA_QUEUESIZE, 4, RdmaConstants.DATANODE_RDMA_QUEUESIZE*2, clientMrCache);
						_endpointGroup.init(new RdmaDataNodePassiveEndpointFactory(_endpointGroup));
						this.clientGroup = _endpointGroup;
					} else {
						LOG.info("active data client ");
						RdmaDataNodeActiveGroup _endpointGroup = new RdmaDataNodeActiveGroup(100, false, RdmaConstants.DATANODE_RDMA_QUEUESIZE, 4, RdmaConstants.DATANODE_RDMA_QUEUESIZE*2, clientMrCache);
						_endpointGroup.init(new RdmaDataNodeActiveEndpointFactory(_endpointGroup));
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
		
		RdmaPassiveEndpointGroup<RdmaDataNodeServerEndpoint> datanodeGroup = new RdmaPassiveEndpointGroup<RdmaDataNodeServerEndpoint>(-1, RdmaConstants.DATANODE_RDMA_QUEUESIZE, 4, RdmaConstants.DATANODE_RDMA_QUEUESIZE*100);
		RdmaServerEndpoint<RdmaDataNodeServerEndpoint> datanodeServerEndpoint = datanodeGroup.createServerEndpoint();
		RdmaDataNodeServer datanodeServer = new RdmaDataNodeServer(datanodeServerEndpoint, serverAddr);
		
		try {
			datanodeGroup.init(new RdmaDataNodeServerEndpointFactory(datanodeGroup, datanodeServer));
			datanodeServerEndpoint.bind(serverAddr, 100);
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
					e.printStackTrace();
				}
 
			}
		} catch(Exception e){
			e.printStackTrace();
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
