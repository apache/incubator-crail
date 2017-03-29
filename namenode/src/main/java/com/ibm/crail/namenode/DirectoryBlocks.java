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
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

import com.ibm.crail.CrailNodeType;
import com.ibm.crail.namenode.protocol.BlockInfo;

public class DirectoryBlocks extends AbstractNode {
	private ConcurrentHashMap<Integer, BlockInfo> blocks;
	
	DirectoryBlocks(int fileComponent, CrailNodeType type) {
		super(fileComponent, type);
		this.blocks = new ConcurrentHashMap<Integer, BlockInfo>();
	}

	@Override
	public BlockInfo getBlock(int index) {
		return blocks.get(index);
	}

	@Override
	public boolean addBlock(int index, BlockInfo block) {
		BlockInfo old = blocks.putIfAbsent(index, block);
		return old == null;
	}

	@Override
	public void freeBlocks(BlockStore blockStore) throws UnknownHostException {
		Iterator<BlockInfo> iter = blocks.values().iterator();
		while (iter.hasNext()){
			BlockInfo blockInfo = iter.next();
			blockStore.addBlock(blockInfo);
		}	
	}
}
