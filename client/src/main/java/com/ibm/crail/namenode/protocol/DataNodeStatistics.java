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

package com.ibm.crail.namenode.protocol;

import java.net.UnknownHostException;
import java.nio.ByteBuffer;

public class DataNodeStatistics {
	public static final int CSIZE = 4;
	
	private int freeBlockCount;
	
	public DataNodeStatistics(){
		this.freeBlockCount = 0;
	}
	
	public DataNodeStatistics(int freeBlockCount){
		this.freeBlockCount = freeBlockCount;
	}
	
	public int write(ByteBuffer buffer){
		buffer.putInt(freeBlockCount);
		return CSIZE;
	}
	
	public void update(ByteBuffer buffer) throws UnknownHostException {
		this.freeBlockCount = buffer.getInt();
	}

	public int getFreeBlockCount() {
		return freeBlockCount;
	}

	public void setFreeBlockCount(int blockCount) {
		this.freeBlockCount = blockCount;
	}

	public void setStatistics(DataNodeStatistics statistics) {
		this.freeBlockCount = statistics.getFreeBlockCount();
	}	
}
