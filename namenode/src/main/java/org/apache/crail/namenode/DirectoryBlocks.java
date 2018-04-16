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

import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.crail.CrailNodeType;
import org.apache.crail.conf.CrailConstants;
import org.apache.crail.metadata.BlockInfo;

public class DirectoryBlocks extends AbstractNode {
	protected AtomicLong dirOffsetCounter;
	protected ConcurrentHashMap<Integer, AbstractNode> children;	
	private ConcurrentHashMap<Integer, NameNodeBlockInfo> blocks;
	
	DirectoryBlocks(long fd, int fileComponent, CrailNodeType type, int storageClass, int locationClass, boolean enumerable) {
		super(fd, fileComponent, type, storageClass, locationClass, enumerable);
		this.children = new ConcurrentHashMap<Integer, AbstractNode>();
		this.dirOffsetCounter = new AtomicLong(0);
		this.blocks = new ConcurrentHashMap<Integer, NameNodeBlockInfo>();
	}
	
	public AbstractNode putChild(AbstractNode child) throws Exception {
		AbstractNode old = children.putIfAbsent(child.getComponent(), child);
		if (old != null){
			throw new Exception("File exists");
		}
		if (child.isEnumerable()) {
			child.setDirOffset(dirOffsetCounter.getAndAdd(CrailConstants.DIRECTORY_RECORD));
		}
		return old;
	}	
	
	public AbstractNode getChild(int component) {
		return children.get(component);
	}	
	
	public AbstractNode removeChild(int component) {
		return children.remove(component);
	}
	
	@Override
	public NameNodeBlockInfo getBlock(int index) {
		return blocks.get(index);
	}

	@Override
	public boolean addBlock(int index, NameNodeBlockInfo block) {
		BlockInfo old = blocks.putIfAbsent(index, block);
		return old == null;
	}

	@Override
	public void freeBlocks(BlockStore blockStore) throws Exception {
		Iterator<NameNodeBlockInfo> iter = blocks.values().iterator();
		while (iter.hasNext()){
			NameNodeBlockInfo blockInfo = iter.next();
			blockStore.addBlock(blockInfo);
		}	
	}

	@Override
	public long setCapacity(long newcapacity) {
		return this.getCapacity();
	}

	@Override
	public void updateToken() {
	}

	@Override
	public void clearChildren(Queue<AbstractNode> queue) {
		Iterator<AbstractNode> iter = children.values().iterator();
		while(iter.hasNext()){
			AbstractNode child = iter.next();
			queue.add(child);
		}		
	}

	@Override
	public void dump() {
		super.dump();
		for (AbstractNode child : children.values()){
			child.dump();
		}		
	}
}
