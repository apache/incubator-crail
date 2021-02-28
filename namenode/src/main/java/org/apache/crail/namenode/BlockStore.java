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

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.*;
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

	short removeDataNode(DataNodeInfo dn) throws IOException {
		int storageClass = dn.getStorageClass();
		return storageClasses[storageClass].removeDatanode(dn);
	}

	short prepareDataNodeForRemoval(DataNodeInfo dn) throws IOException {

		// Despite several potential storageClasses the pair (Ip-addr,port) should
		// nevertheless target only one running datanode instance.
		// Therefore we can iterate over all storageClasses to check whether
		// the requested datanode is part of one of the storageClasses.
		for (StorageClass storageClass : storageClasses) {
			if (storageClass.getDataNode(dn) != null) {
				return storageClass.prepareForRemovalDatanode(dn);
			}
		}

		LOG.error("DataNode: " + dn.toString() + " not found");
		return RpcErrors.ERR_DATANODE_NOT_REGISTERED;
	}

	public double getStorageUsedPercentage() throws Exception {
		long total = 0;
		long free = 0;
		for (StorageClass storageClass : storageClasses) {
			total += storageClass.getTotalBlockCount();
			free += storageClass.getFreeBlockCount();
		}

		// if there is no available capacity (i.e. total number of available blocks is 0),
		// return 1.0 which tells that all storage is used
		if (total != 0) {
			double available = (double) free / (double) total;
			return 1.0 - available;
		} else {
			return 1.0;
		}

	}

	public long getNumberOfBlocksUsed() throws Exception {
		int total = 0;

		for (StorageClass storageClass: storageClasses) {
			total += (storageClass.getTotalBlockCount() - storageClass.getFreeBlockCount());
		}

		return total;
	}

	public long getNumberOfBlocks() throws Exception {
		int total = 0;

		for (StorageClass storageClass: storageClasses) {
			total += storageClass.getTotalBlockCount();
		}

		return total;
	}

	public int getNumberOfRunningDatanodes() {
		int total = 0;

		for (StorageClass storageClass : storageClasses) {
			total += storageClass.getNumberOfRunningDatanodes();
		}

		return total;
	}

	public DataNodeBlocks identifyRemoveCandidate() {

		ArrayList<DataNodeBlocks> dataNodeBlocks = new ArrayList<DataNodeBlocks>();
		for (StorageClass storageClass : storageClasses) {
			dataNodeBlocks.addAll(storageClass.getDataNodeBlocks());
		}

		// sort all datanodes by increasing numbers of available datablocks
		Collections.sort(dataNodeBlocks, new Comparator<DataNodeBlocks>() {
			public int compare(DataNodeBlocks d1, DataNodeBlocks d2) {
				if (d1.getBlockCount() < d2.getBlockCount()) {
					return 1;
				} else if (d1.getBlockCount() > d2.getBlockCount()) {
					return -1;
				} else return 0;
			}
		});

		// iterate over datanodes and return first datanode which is not already scheduled for removal
		for (DataNodeBlocks candidate: dataNodeBlocks) {
			if (!candidate.isScheduleForRemoval()) {
				return candidate;
			}
		}

		// return null if there is no available candidate
		return null;

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
		} else if (CrailConstants.NAMENODE_BLOCKSELECTION.equalsIgnoreCase("sequential")) {
			this.blockSelection = new SequentialBlockSelection();
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

	short prepareForRemovalDatanode(DataNodeInfo dn) throws IOException {

		// this will only mark the datanode for removal
		DataNodeBlocks toBeRemoved = membership.get(dn.key());
		if (toBeRemoved == null) {
			LOG.error("DataNode: " + dn.toString() + " not found");
			return RpcErrors.ERR_DATANODE_NOT_REGISTERED;
		} else {
			toBeRemoved.scheduleForRemoval();
			return RpcErrors.ERR_OK;
		}
	}

	short removeDatanode(DataNodeInfo dn) throws IOException {

		// this will remove the datanode once it does not store any remaining data blocks
		DataNodeBlocks toBeRemoved = membership.get(dn.key());
		if (toBeRemoved == null) {
			LOG.error("DataNode: " + dn.toString() + " not found");
			return RpcErrors.ERR_DATANODE_NOT_REGISTERED;
		} else {
			membership.remove(toBeRemoved.key());
			return RpcErrors.ERR_OK;
		}
	}

	//---------------

	public long getTotalBlockCount() {
		long capacity = 0;

		for (DataNodeBlocks datanode : membership.values()) {
			capacity += datanode.getTotalNumberOfBlocks();
		}

		return capacity;
	}

	public long getFreeBlockCount() {
		long capacity = 0;

		for (DataNodeBlocks datanode : membership.values()) {
			capacity += datanode.getBlockCount();
		}

		return capacity;
	}

	public Collection<DataNodeBlocks> getDataNodeBlocks() {
		return this.membership.values();
	}

	public int getNumberOfRunningDatanodes() {
		return this.membership.size();
	}

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
	
	public class SequentialBlockSelection implements BlockSelection {
		public SequentialBlockSelection(){
			LOG.info("sequential block selection");
		}

		@Override
		public int getNext(int size) {
			return 0;
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
						if (anyDn.isOnline() && !anyDn.isScheduleForRemoval()){
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

