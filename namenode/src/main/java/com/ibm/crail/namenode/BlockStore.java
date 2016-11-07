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

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.slf4j.Logger;
import com.ibm.crail.conf.CrailConstants;
import com.ibm.crail.namenode.protocol.BlockInfo;
import com.ibm.crail.namenode.protocol.DataNodeInfo;
import com.ibm.crail.namenode.rpc.NameNodeProtocol;
import com.ibm.crail.utils.AtomicIntegerModulo;
import com.ibm.crail.utils.CrailUtils;

public class BlockStore {
	private StorageTier[] storageTiers;
	
	public BlockStore(){
		StringTokenizer tokenizer = new StringTokenizer(CrailConstants.DATANODE_TYPES, ",");
		storageTiers = new StorageTier[tokenizer.countTokens()];
		for (int i = 0; i < tokenizer.countTokens(); i++){
			this.storageTiers[i] = new StorageTier(i);
		}		
	}

	public short addBlock(BlockInfo blockInfo) throws UnknownHostException {
		int storageTier = blockInfo.getDnInfo().getStorageTier();
		return storageTiers[storageTier].addBlock(blockInfo);
	}

	public BlockInfo getBlock(int storageAffinity, int locationAffinity) throws InterruptedException {
		BlockInfo block = null;
		if (storageAffinity > 0){
			if (storageAffinity < storageTiers.length){
				block = storageTiers[storageAffinity].getBlock(locationAffinity);
			}
		}
		if (block == null){
			for (int i = 0; i < storageTiers.length; i++){
				block = storageTiers[i].getBlock(locationAffinity);
				if (block != null){
					break;
				}
			}
		}
		
		return block;
	}

	public DataNodeBlocks getDataNode(DataNodeInfo dnInfo) {
		int tier = dnInfo.getStorageTier();
		return storageTiers[tier].getDataNode(dnInfo);
	}
	
}

class StorageTier {
	private static final Logger LOG = CrailUtils.getLogger();
	
	private int storageTier;
	private ConcurrentHashMap<Long, DataNodeBlocks> membership;
	private ConcurrentHashMap<Integer, DataNodeArray> affinitySets;
	private DataNodeArray anySet;
	private BlockSelection blockSelection;
	
	public StorageTier(int storageTier){
		if (CrailConstants.NAMENODE_BLOCKSELECTION.equalsIgnoreCase("roundrobin")){
			this.blockSelection = new RoundRobinBlockSelection();
		} else {
			this.blockSelection = new RandomBlockSelection();
		}
		this.storageTier = storageTier;
		this.membership = new ConcurrentHashMap<Long, DataNodeBlocks>();
		this.affinitySets = new ConcurrentHashMap<Integer, DataNodeArray>();
		this.anySet = new DataNodeArray(blockSelection);
//		this.anyCounter = new AtomicIntegerModulo();
	}
	
	short addBlock(BlockInfo block) throws UnknownHostException {
		long dnAddress = block.getDnInfo().key();
		DataNodeBlocks current = membership.get(dnAddress);
		if (current == null) {
			current = DataNodeBlocks.fromDataNodeInfo(block.getDnInfo());
			LOG.info("new datanode, address " + current.key() + ", tier " + current.getStorageTier());
			addDataNode(current);
		}

		current.addFreeBlock(block);
		return NameNodeProtocol.ERR_OK;
	}

	BlockInfo getBlock(int affinity) throws InterruptedException {
		BlockInfo block = null;
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
			return NameNodeProtocol.ERR_DATANODE_NOT_REGISTERED;
		} 
		
		// current == null, datanode not in set, adding it now
		_addDataNode(dataNode);
		
		return NameNodeProtocol.ERR_OK;

	}
	
	//---------------
	
	private void _addDataNode(DataNodeBlocks dataNode){
//		LOG.info("adding datanode for affinity " + dataNode.getAffinity());
		DataNodeArray hostMap = affinitySets.get(dataNode.getLocationAffinity());
		if (hostMap == null){
			hostMap = new DataNodeArray(blockSelection);
			DataNodeArray oldMap = affinitySets.putIfAbsent(dataNode.getLocationAffinity(), hostMap);
			if (oldMap != null){
				hostMap = oldMap;
			}
		}	
		hostMap.add(dataNode);
		anySet.add(dataNode);
	}
	
	private BlockInfo _getAffinityBlock(int affinity) throws InterruptedException {
		BlockInfo block = null;
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
		
		private BlockInfo get() throws InterruptedException {
			lock.readLock().lock();
			try {
				BlockInfo block = null;
				int size = arrayList.size();
				int startIndex = blockSelection.getNext(size);
				for (int i = 0; i < size; i++){
					int index = (startIndex + i) % size;
					DataNodeBlocks anyDn = arrayList.get(index);
					block = anyDn.getFreeBlock();
					if (block != null){
						break;
					} 
				}
				return block;
			} finally {
				lock.readLock().unlock();
			}
		}		
	}

	public int getStorageTier() {
		return storageTier;
	}
}

