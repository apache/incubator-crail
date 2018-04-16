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

package org.apache.crail.storage;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import org.apache.crail.CrailLocationClass;
import org.apache.crail.CrailStorageClass;
import org.apache.crail.conf.CrailConstants;
import org.apache.crail.metadata.BlockInfo;
import org.apache.crail.metadata.DataNodeInfo;
import org.apache.crail.metadata.DataNodeStatistics;
import org.apache.crail.rpc.RpcConnection;
import org.apache.crail.rpc.RpcErrors;
import org.apache.crail.rpc.RpcVoid;
import org.apache.crail.utils.CrailUtils;
import org.slf4j.Logger;

public class StorageRpcClient {
	public static final Logger LOG = CrailUtils.getLogger();
	
	private InetSocketAddress serverAddress;
	private int storageType;
	private CrailStorageClass storageClass;
	private CrailLocationClass locationClass;
	private RpcConnection rpcConnection;	

	public StorageRpcClient(int storageType, CrailStorageClass storageClass, InetSocketAddress serverAddress, RpcConnection rpcConnection) throws Exception {
		this.storageType = storageType;
		this.storageClass = storageClass;
		this.serverAddress = serverAddress;
		this.locationClass = CrailUtils.getLocationClass();
		this.rpcConnection = rpcConnection;
	}
	
	public void setBlock(long lba, long addr, int length, int key) throws Exception {
		InetSocketAddress inetAddress = serverAddress;
		DataNodeInfo dnInfo = new DataNodeInfo(storageType, storageClass.value(), locationClass.value(), inetAddress.getAddress().getAddress(), inetAddress.getPort());
		BlockInfo blockInfo = new BlockInfo(dnInfo, lba, addr, length, key);
		RpcVoid res = rpcConnection.setBlock(blockInfo).get(CrailConstants.RPC_TIMEOUT, TimeUnit.MILLISECONDS);
		if (res.getError() != RpcErrors.ERR_OK){
			LOG.info("setBlock: " + RpcErrors.messages[res.getError()]);
			throw new IOException("setBlock: " + RpcErrors.messages[res.getError()]);
		}
	}
	
	public DataNodeStatistics getDataNode() throws Exception{
		InetSocketAddress inetAddress = serverAddress;
		DataNodeInfo dnInfo = new DataNodeInfo(storageType, storageClass.value(), locationClass.value(), inetAddress.getAddress().getAddress(), inetAddress.getPort());
		return this.rpcConnection.getDataNode(dnInfo).get(CrailConstants.RPC_TIMEOUT, TimeUnit.MILLISECONDS).getStatistics();
	}	
}
