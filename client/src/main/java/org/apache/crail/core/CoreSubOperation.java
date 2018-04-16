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

package org.apache.crail.core;

import java.io.IOException;

import org.apache.crail.conf.CrailConstants;
import org.apache.crail.utils.CrailUtils;

public class CoreSubOperation {
	private long fd;
	private long fileOffset;
	private int bufferPosition;
	private int len;
	
	private long blockOffset;
	private long blockStart;
	private long key;
	
	public CoreSubOperation(long fd, long fileOffset, int bufferPosition, int writeLen) throws IOException {
		this.fd = fd;
		this.fileOffset = fileOffset;
		this.bufferPosition = bufferPosition;
		this.len = writeLen;
		
		this.blockOffset = fileOffset % CrailConstants.BLOCK_SIZE;
		this.blockStart = CrailUtils.blockStartAddress(fileOffset);
//		this.key = CoreSubOperation.createKey(fd, blockStart);
		this.key = blockStart;
	}

	public long getFd() {
		return fd;
	}

	public long getBlockOffset() {
		return blockOffset;
	}
	
	public long getBlockStart() {
		return blockStart;
	}

	public int getBufferPosition(){
		return bufferPosition;
	}

	public int getLen() {
		return len;
	}

	@Override
	public String toString() {
		return "fd " + fd + ", fileOffset " + fileOffset + ", blockOffset " + blockOffset + ", len " + len + ", blockStart " + blockStart;
	}

	public long key(){
		return this.key;
	}
	
	public static long createKey(long fd, long fileOffset){
		long offset = CrailUtils.blockStartAddress(fileOffset);
		return offset;
//		return fd + ":" + offset;
	}
}
