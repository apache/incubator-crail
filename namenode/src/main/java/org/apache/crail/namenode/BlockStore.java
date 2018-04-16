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
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.crail.conf.CrailConstants;
import org.apache.crail.metadata.BlockInfo;
import org.apache.crail.metadata.DataNodeInfo;
import org.apache.crail.rpc.RpcErrors;
import org.apache.crail.utils.AtomicIntegerModulo;
import org.apache.crail.utils.CrailUtils;
import org.slf4j.Logger;

public class BlockStore {
	private static final Logger LOG = CrailUtils.getLogger();
	
	private StorageClass[] storageClasses;
	
	public BlockStore(){
		storageClasses = new StorageClass[CrailConstants.STORAGE_CLASSES]; 
		for (int i = 0; i < CrailConstants.STORAGE_CLASSES; i++){
			this.storageClasses[i] = new StorageClass(i);
		}		
	}

	public short addBlock(NameNodeBlockInfo blockInfo) throws UnknownHostException {
		int storageClass = blockInfo.getDnInfo().getStorageClass();
		return storageClasses[storageClass].addBlock(blockInfo);
	}

	public boolean regionExists(BlockInfo region) {
		int storageClass = region.getDnInfo().getStorageClass();
		return storageClasses[storageClass].regionExists(region);
	}

	public short updateRegion(BlockInfo region) {
		int storageClass = region.getDnInfo().getStorageClass();
		return storageClasses[storageClass].updateRegion(region);
	}

	public NameNodeBlockInfo getBlock(int storageClass, int locationAffinity) throws InterruptedException {
		NameNodeBlockInfo block = null;
		if (storageClass > 0){
			if (storageClass < storageClasses.length){
				block = storageClasses[storageClass].getBlock(locationAffinity);
			} else {
				//TODO: warn if requested storage class is invalid
			}
		}
		if (block == null){
			for (int i = 0; i < storageClasses.length; i++){
				block = storageClasses[i].getBlock(locationAffinity);
				if (block != null){
					break;
				}
			}
		}
		
		return block;
	}

	public DataNodeBlocks getDataNode(DataNodeInfo dnInfo) {
		int storageClass = dnInfo.getStorageClass();
		return storageClasses[storageClass].getDataNode(dnInfo);
	}
	
}

class StorageClass {
	private static final Logger LOG = CrailUtils.getLogger();
	
	private int storageClass;
	private ConcurrentHashMap<Long, DataNodeBlocks> membership;
	private ConcurrentHashMap<Integer, DataNodeArray> affinitySets;
	private DataNodeArray anySet;
	private BlockSelection blockSelection;
	
	public StorageClass(int storageClass){
		this.storageClass = storageClass;
		this.membership = new ConcurrentHashMap<Long, DataNodeBlocks>();
		this.affinitySets = new ConcurrentHashMap<Integer, DataNodeArray>();
		if (CrailConstants.NAMENODE_BLOCKSELECTION.equalsIgnoreCase("roundrobin")){
			this.blockSelection = new RoundRobinBlockSelection();
		} else {
			this.blockSelection = new RandomBlockSelection();
		}
		this.anySet = new DataNodeArray(blockSelection);
	}
	
	public short updateRegion(BlockInfo region) {
		long dnAddress = region.getDnInfo().key();
		DataNodeBlocks current = membership.get(dnAddress);
		if (current == null) {
			return RpcErrors.ERR_ADD_BLOCK_FAILED;
		} else {
			return current.updateRegion(region);
		}
	}

	public boolean regionExists(BlockInfo region) {
		long dnAddress = region.getDnInfo().key();
		DataNodeBlocks current = membership.get(dnAddress);
		if (current == null) {
			return false;
		} else {
			return current.regionExists(region);
		}
	}

	short addBlock(NameNodeBlockInfo block) throws UnknownHostException {
		long dnAddress = block.getDnInfo().key();
		DataNodeBlocks current = membership.get(dnAddress);
		if (current == null) {
			current = DataNodeBlocks.fromDataNodeInfo(block.getDnInfo());
			addDataNode(current);
		}

		current.touch();
		current.addFreeBlock(block);
		return RpcErrors.ERR_OK;
	}

	NameNodeBlockInfo getBlock(int affinity) throws InterruptedException {
		NameNodeBlockInfo block = null;
		if (affinity == 0) {
			block = anySet.get();
		} else {
			block = _getAffinityBlock(affinity);
			if (block == null) {
				block = anySet.get();
			} else {
			}
		}
		return block;
	}

	DataNodeBlocks getDataNode(DataNodeInfo dataNode) {
		return membership.get(dataNode.key());
	}

	short addDataNode(DataNodeBlocks dataNode) {
		DataNodeBlocks current = membership.putIfAbsent(dataNode.key(), dataNode);
		if (current != null) {
			return RpcErrors.ERR_DATANODE_NOT_REGISTERED;
		} 
		
		// current == null, datanode not in set, adding it now
		_addDataNode(dataNode);
		
		return RpcErrors.ERR_OK;

	}
	
	//---------------
	
	private void _addDataNode(DataNodeBlocks dataNode){
		LOG.info("adding datanode " + CrailUtils.getIPAddressFromBytes(dataNode.getIpAddress()) + ":" + dataNode.getPort() + " of type " + dataNode.getStorageType() + " to storage class " + storageClass);
		DataNodeArray hostMap = affinitySets.get(dataNode.getLocationClass());
		if (hostMap == null){
			hostMap = new DataNodeArray(blockSelection);
			DataNodeArray oldMap = affinitySets.putIfAbsent(dataNode.getLocationClass(), hostMap);
			if (oldMap != null){
				hostMap = oldMap;
			}
		}	
		hostMap.add(dataNode);
		anySet.add(dataNode);
	}
	
	private NameNodeBlockInfo _getAffinityBlock(int affinity) throws InterruptedException {
		NameNodeBlockInfo block = null;
		DataNodeArray affinitySet = affinitySets.get(affinity);
		if (affinitySet != null){
			block = affinitySet.get();
		}
		return block;
	}
	
	public static interface BlockSelection {
		int getNext(int size);
	}
	
	private class RoundRobinBlockSelection implements BlockSelection {
		private AtomicIntegerModulo counter;
		
		public RoundRobinBlockSelection(){
			LOG.info("round robin block selection");
			counter = new AtomicIntegerModulo();
		}
		
		@Override
		public int getNext(int size) {
			return counter.getAndIncrement() % size;
		}
	}
	
	private class RandomBlockSelection implements BlockSelection {
		public RandomBlockSelection(){
			LOG.info("random block selection");
		}		
		
		@Override
		public int getNext(int size) {
			return ThreadLocalRandom.current().nextInt(size);
		}
	}	
	
	private class DataNodeArray {
		private ArrayList<DataNodeBlocks> arrayList;
		private ReentrantReadWriteLock lock;
		private BlockSelection blockSelection;
		
		public DataNodeArray(BlockSelection blockSelection){
			this.arrayList = new ArrayList<DataNodeBlocks>();
			this.lock = new ReentrantReadWriteLock();
			this.blockSelection = blockSelection;
		}
		
		public void add(DataNodeBlocks dataNode){
			lock.writeLock().lock();
			try {
				arrayList.add(dataNode);
			} finally {
				lock.writeLock().unlock();
			}
		}
		
		private NameNodeBlockInfo get() throws InterruptedException {
			lock.readLock().lock();
			try {
				NameNodeBlockInfo block = null;
				int size = arrayList.size();
				if (size > 0){
					int startIndex = blockSelection.getNext(size);
					for (int i = 0; i < size; i++){
						int index = (startIndex + i) % size;
						DataNodeBlocks anyDn = arrayList.get(index);
						if (anyDn.isOnline()){
							block = anyDn.getFreeBlock();
						}
						if (block != null){
							break;
						} 
					}
				}
				return block;
			} finally {
				lock.readLock().unlock();
			}
		}
	}
}

