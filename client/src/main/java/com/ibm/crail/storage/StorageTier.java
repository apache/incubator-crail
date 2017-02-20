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

package com.ibm.crail.storage;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;

import com.ibm.crail.conf.CrailConfiguration;
import com.ibm.crail.conf.CrailConstants;
import com.ibm.crail.namenode.protocol.BlockInfo;
import com.ibm.crail.namenode.protocol.DataNodeInfo;
import com.ibm.crail.namenode.protocol.DataNodeStatistics;
import com.ibm.crail.namenode.rpc.NameNodeProtocol;
import com.ibm.crail.namenode.rpc.RpcNameNode;
import com.ibm.crail.namenode.rpc.RpcNameNodeClient;
import com.ibm.crail.namenode.rpc.RpcResponseMessage;
import com.ibm.crail.utils.CrailUtils;
import com.ibm.disni.util.*;

public abstract class StorageTier {
	public static final Logger LOG = CrailUtils.getLogger();

	public abstract void init(CrailConfiguration conf, String[] args) throws IOException;
	public abstract StorageEndpoint createEndpoint(InetSocketAddress inetAddress) throws IOException;	
	public abstract void run() throws Exception;	
	public abstract void close() throws Exception;
	public abstract InetSocketAddress getAddress();
	public abstract void printConf(Logger log);
	
	private RpcNameNode rpcNameNode;
	private RpcNameNodeClient namenodeClientRpc;	
	private boolean isConnected;
	private ConcurrentHashMap<String, Integer> dataNodeTypes;

	public StorageTier(){
		this.isConnected = false;
	}	
	
	
	public final synchronized void connect() throws Exception{
		if (isConnected){
			return;
		}
		
		dataNodeTypes = new ConcurrentHashMap<String, Integer>();
		StringTokenizer tokenizer = new StringTokenizer(CrailConstants.DATANODE_TYPES, ",");
		int storageTier = 0;
		while (tokenizer.hasMoreTokens()){
			String name = tokenizer.nextToken();
			dataNodeTypes.put(name, storageTier++);
		}		
		
		InetAddress localhost = InetAddress.getLocalHost();
		String hostname = localhost.getHostName();
		int hosthash = hostname.hashCode();
		LOG.info("hostname " + hostname + ", hash " + hosthash);		
		
		InetSocketAddress nnAddr = CrailUtils.getNameNodeAddress();
		this.rpcNameNode = RpcNameNode.createInstance(CrailConstants.NAMENODE_RPC_TYPE);
		this.namenodeClientRpc = rpcNameNode.getRpcClient(nnAddr);
		this.isConnected = true;
		LOG.info("connected to namenode at " + nnAddr);		
	}
	
	public void setBlock(long addr, int length, int key) throws Exception {
		int localAffinity = InetAddress.getLocalHost().getHostName().hashCode();
		int storageTier = dataNodeTypes.get(getType());
		InetSocketAddress inetAddress = getAddress();
		DataNodeInfo dnInfo = new DataNodeInfo(storageTier, localAffinity, inetAddress.getAddress().getAddress(), inetAddress.getPort());
		BlockInfo blockInfo = new BlockInfo(dnInfo, addr, length, key);
		RpcResponseMessage.VoidRes res = namenodeClientRpc.setBlock(blockInfo).get(CrailConstants.RPC_TIMEOUT, TimeUnit.MILLISECONDS);
		if (res.getError() != NameNodeProtocol.ERR_OK){
			LOG.info("setBlock: " + NameNodeProtocol.messages[res.getError()]);
			throw new IOException("setBlock: " + NameNodeProtocol.messages[res.getError()]);
		}
	}
	
	public DataNodeStatistics getDataNode() throws Exception{
		int localAffinity = InetAddress.getLocalHost().getHostName().hashCode();
		int storageTier = dataNodeTypes.get(getType());
		InetSocketAddress inetAddress = getAddress();
		DataNodeInfo dnInfo = new DataNodeInfo(storageTier, localAffinity, inetAddress.getAddress().getAddress(), inetAddress.getPort());
		return this.namenodeClientRpc.getDataNode(dnInfo).get(CrailConstants.RPC_TIMEOUT, TimeUnit.MILLISECONDS).getStatistics();
	}	
	
	public String getType() {
		return this.getClass().getName();
	};	
	
	public static void main(String[] args){
		try{
			GetOpt go = new GetOpt(args, "t:");
			go.optErr = true;
			int ch = -1;
			String name = "com.ibm.crail.storage.rdma.RdmaStorageTier";
			CrailConfiguration conf = new CrailConfiguration();
			
			while ((ch = go.getopt()) != GetOpt.optEOF) {
				if ((char) ch == 't') {
					name = go.optArgGet();
				}
			}

			CrailConstants.updateConstants(conf);
			CrailConstants.printConf();
			CrailConstants.verify();				
	
			StorageTier dataNode = createInstance(name);
			if (dataNode == null){
				throw new Exception("Cannot instantiate datanode of type " + name);
			}
			dataNode.init(conf, args);
			dataNode.printConf(LOG);
			dataNode.connect();
			dataNode.run();
			System.exit(0);
		} catch(Exception e){
			e.printStackTrace();
		}		
	}
	
	@SuppressWarnings("unchecked")
	public static StorageTier createInstance(String name) throws Exception {
		Class<?> nodeClass = Class.forName(name);
		if (StorageTier.class.isAssignableFrom(nodeClass)){
			Class<? extends StorageTier> dataNodeClass = (Class<? extends StorageTier>) nodeClass;
			StorageTier dataNode = dataNodeClass.newInstance();
			return dataNode;
		} else {
			throw new Exception("Cannot instantiate datanode of type " + name);
		}
		
	}
	
	@Override
	public String toString() {
		return "DataNode, type " + this.getType();
	}
}
