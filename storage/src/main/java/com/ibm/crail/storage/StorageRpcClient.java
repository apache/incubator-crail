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
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;

import com.ibm.crail.conf.CrailConstants;
import com.ibm.crail.metadata.BlockInfo;
import com.ibm.crail.metadata.DataNodeInfo;
import com.ibm.crail.metadata.DataNodeStatistics;
import com.ibm.crail.rpc.RpcErrors;
import com.ibm.crail.rpc.RpcClient;
import com.ibm.crail.rpc.RpcConnection;
import com.ibm.crail.rpc.RpcVoid;
import com.ibm.crail.utils.CrailUtils;

public class StorageRpcClient {
	public static final Logger LOG = CrailUtils.getLogger();
	
	private InetSocketAddress serverAddress;
	private int storageTierIndex;
	private int hostHash;
	private RpcClient rpcNameNode;
	private RpcConnection namenodeClientRpc;	

	public StorageRpcClient(int storageTierIndex, InetSocketAddress serverAddress) throws Exception {
		this.storageTierIndex = storageTierIndex;
		this.serverAddress = serverAddress;
		this.hostHash = CrailUtils.getHostHash();
		LOG.info("hosthash" + hostHash);		
		
		InetSocketAddress nnAddr = CrailUtils.getNameNodeAddress();
		this.rpcNameNode = RpcClient.createInstance(CrailConstants.NAMENODE_RPC_TYPE);
		this.namenodeClientRpc = rpcNameNode.connect(nnAddr);
		LOG.info("connected to namenode at " + nnAddr);				
	}
	
	public void setBlock(long addr, int length, int key) throws Exception {
		InetSocketAddress inetAddress = serverAddress;
		DataNodeInfo dnInfo = new DataNodeInfo(storageTierIndex, hostHash, inetAddress.getAddress().getAddress(), inetAddress.getPort());
		BlockInfo blockInfo = new BlockInfo(dnInfo, addr, length, key);
		RpcVoid res = namenodeClientRpc.setBlock(blockInfo).get(CrailConstants.RPC_TIMEOUT, TimeUnit.MILLISECONDS);
		if (res.getError() != RpcErrors.ERR_OK){
			LOG.info("setBlock: " + RpcErrors.messages[res.getError()]);
			throw new IOException("setBlock: " + RpcErrors.messages[res.getError()]);
		}
	}
	
	public DataNodeStatistics getDataNode() throws Exception{
		InetSocketAddress inetAddress = serverAddress;
		DataNodeInfo dnInfo = new DataNodeInfo(storageTierIndex, hostHash, inetAddress.getAddress().getAddress(), inetAddress.getPort());
		return this.namenodeClientRpc.getDataNode(dnInfo).get(CrailConstants.RPC_TIMEOUT, TimeUnit.MILLISECONDS).getStatistics();
	}	
}
