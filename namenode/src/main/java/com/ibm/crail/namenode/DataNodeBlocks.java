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

package com.ibm.crail.namenode;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.concurrent.LinkedBlockingQueue;

import com.ibm.crail.namenode.protocol.BlockInfo;
import com.ibm.crail.namenode.protocol.DataNodeInfo;

public class DataNodeBlocks extends DataNodeInfo {
	private LinkedBlockingQueue<BlockInfo> freeBlocks;
	
	public static DataNodeBlocks fromDataNodeInfo(DataNodeInfo dnInfo) throws UnknownHostException{
		InetSocketAddress address = new InetSocketAddress(InetAddress.getByAddress(dnInfo.getIpAddress()), dnInfo.getPort());
		DataNodeBlocks dnInfoNn = new DataNodeBlocks(dnInfo.getStorageTier(), dnInfo.getLocationAffinity(), address);
		return dnInfoNn;
	}	

	public DataNodeBlocks(int tier, int hosthash, InetSocketAddress inetAddress) throws UnknownHostException {
		super(tier, hosthash, inetAddress);
		this.freeBlocks = new LinkedBlockingQueue<BlockInfo>();
	}
	
	public void addFreeBlock(BlockInfo nnBlock) {
		freeBlocks.add(nnBlock);
	}

	public BlockInfo getFreeBlock() throws InterruptedException {
		BlockInfo block = this.freeBlocks.poll();
		return block;
	}
	
	public int getBlockCount() {
		return freeBlocks.size();
	}
}
