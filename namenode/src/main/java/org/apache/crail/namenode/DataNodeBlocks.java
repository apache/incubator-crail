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

package org.apache.crail.namenode;

import java.net.UnknownHostException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.crail.conf.CrailConstants;
import org.apache.crail.metadata.BlockInfo;
import org.apache.crail.metadata.DataNodeInfo;
import org.apache.crail.rpc.RpcErrors;
import org.apache.crail.utils.CrailUtils;
import org.slf4j.Logger;

public class DataNodeBlocks extends DataNodeInfo {
	private static final Logger LOG = CrailUtils.getLogger();
	
	private ConcurrentHashMap<Long, BlockInfo> regions;
	private LinkedBlockingQueue<NameNodeBlockInfo> freeBlocks;
	private long token;
	
	public static DataNodeBlocks fromDataNodeInfo(DataNodeInfo dnInfo) throws UnknownHostException{
		DataNodeBlocks dnInfoNn = new DataNodeBlocks(dnInfo.getStorageType(), dnInfo.getStorageClass(), dnInfo.getLocationClass(), dnInfo.getIpAddress(), dnInfo.getPort());
		return dnInfoNn;
	}	

	private DataNodeBlocks(int storageType, int getStorageClass, int locationClass, byte[] ipAddress, int port) throws UnknownHostException {
		super(storageType, getStorageClass, locationClass, ipAddress, port);
		this.regions = new ConcurrentHashMap<Long, BlockInfo>();
		this.freeBlocks = new LinkedBlockingQueue<NameNodeBlockInfo>();
	}
	
	public void addFreeBlock(NameNodeBlockInfo nnBlock) {
		regions.put(nnBlock.getRegion().getLba(), nnBlock.getRegion());
		freeBlocks.add(nnBlock);
	}

	public NameNodeBlockInfo getFreeBlock() throws InterruptedException {
		NameNodeBlockInfo block = this.freeBlocks.poll();
		return block;
	}
	
	public int getBlockCount() {
		return freeBlocks.size();
	}

	public boolean regionExists(BlockInfo region) {
		if (regions.containsKey(region.getLba())){
			return true;
		} 
		return false;
	}

	public short updateRegion(BlockInfo region) {
		BlockInfo oldRegion = regions.get(region.getLba());
		if (oldRegion == null){
			return RpcErrors.ERR_ADD_BLOCK_FAILED;
		} else {
			oldRegion.setBlockInfo(region);
			return 0;
		}
	}

	public void touch() {
		this.token = System.nanoTime() + TimeUnit.SECONDS.toNanos(CrailConstants.STORAGE_KEEPALIVE*8);		
	}
	
	public boolean isOnline(){
		return System.nanoTime() <= token;
	}	
}
