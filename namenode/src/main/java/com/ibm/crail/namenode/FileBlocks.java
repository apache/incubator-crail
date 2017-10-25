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
import java.util.Iterator;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.ibm.crail.CrailNodeType;
import com.ibm.crail.conf.CrailConstants;
import com.ibm.crail.metadata.BlockInfo;

public class FileBlocks extends AbstractNode {
	private ArrayList<NameNodeBlockInfo> blocks;
	private final ReentrantReadWriteLock lock;
	private final Lock readLock;
	private final Lock writeLock;
	
	public FileBlocks(long fd, int fileComponent, CrailNodeType type, int storageClass, int locationClass) {
		super(fd, fileComponent, type, storageClass, locationClass);
		this.blocks = new ArrayList<NameNodeBlockInfo>(CrailConstants.NAMENODE_FILEBLOCKS);
		this.lock = new ReentrantReadWriteLock();
		this.readLock = lock.readLock();
		this.writeLock = lock.writeLock();
	}

	@Override
	public NameNodeBlockInfo getBlock(int index) {
		readLock.lock();
		try {
			if (index < blocks.size()){
				return blocks.get(index);
			} else {
				return null;
			}
		} catch(Exception e){ 
			return null;
		} finally {
			readLock.unlock();
		}
	}

	@Override
	public boolean addBlock(int index, NameNodeBlockInfo block) {
		writeLock.lock();
		try {
			if (index == blocks.size()){
				blocks.add(index, block);
				return true;
			} else {
				return false;
			}
		} finally {
			writeLock.unlock();
		}
	}

	@Override
	public void freeBlocks(BlockStore blockStore) throws UnknownHostException {
		readLock.lock();
		try {
			Iterator<NameNodeBlockInfo> iter = blocks.iterator();
			while (iter.hasNext()){
				NameNodeBlockInfo blockInfo = iter.next();
				blockStore.addBlock(blockInfo);
			}	
		} finally {
			readLock.unlock();
		}
	}

}
