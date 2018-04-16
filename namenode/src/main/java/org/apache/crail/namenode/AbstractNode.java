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

import java.util.Queue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

import org.apache.crail.CrailNodeType;
import org.apache.crail.metadata.FileInfo;

public abstract class AbstractNode extends FileInfo implements Delayed {
	private int fileComponent;
	private long delay;
	private int storageClass;
	private int locationClass;
	
	//children manipulation
	//adds or replaces a child, returns previous value or null if there was no mapping
	public abstract AbstractNode putChild(AbstractNode child) throws Exception;
	//get the child with the given component name, returns null if there is no mapping
	public abstract AbstractNode getChild(int component) throws Exception;
	//remove a child, returns previous value of existing or null otherwise
	public abstract AbstractNode removeChild(int component) throws Exception;
	//clear all the children (used by GC)
	public abstract void clearChildren(Queue<AbstractNode> queue) throws Exception;
//	public abstract AbstractNode updateParent() throws Exception;
	
	//block manipulation
	//adds a new block at a given index, returns true if succesful, false otherwise
	public abstract boolean addBlock(int index, NameNodeBlockInfo block) throws Exception;
	//get block at the given index, returns a valid block or null otherwise
	public abstract NameNodeBlockInfo getBlock(int index) throws Exception;
	//clear all the blocks (used by GC)
	public abstract void freeBlocks(BlockStore blockStore) throws Exception;	
	
	public AbstractNode(long fd, int fileComponent, CrailNodeType type, int storageClass, int locationAffinity, boolean enumerable){
		super(fd, type, enumerable);
		
		this.fileComponent = fileComponent;
		this.storageClass = storageClass;
		this.locationClass = locationAffinity;
		this.delay = System.currentTimeMillis();
		this.setModificationTime(System.currentTimeMillis());
	}
	
	void rename(int newFileComponent) throws Exception {
		this.fileComponent = newFileComponent;
	}	

	public int getComponent() {
		return this.fileComponent;
	}
	
	public void dump(){
		System.out.println(this.toString());
	}
	
	@Override
	protected void setDirOffset(long dirOffset) {
		super.setDirOffset(dirOffset);
	}	
	
	@Override
	public String toString() {
		return String.format("%08d\t%08d\t\t%08d\t\t%08d\t\t%08d", getFd(), fileComponent, getCapacity(), getType().getLabel(), getDirOffset());
	}	

	@Override
	public long getDelay(TimeUnit unit) {
		long diff = delay - System.currentTimeMillis();
		long _delay = unit.convert(diff, TimeUnit.MILLISECONDS);
		return _delay;		
	}

	public void setDelay(long delay) {
		this.delay = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(delay);
	}

	@Override
	public int compareTo(Delayed o) {
		return 0;
	}

	public int getStorageClass() {
		return storageClass;
	}

	public int getLocationClass() {
		return locationClass;
	}
}
