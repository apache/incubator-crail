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

package com.ibm.crail.core;

import java.io.IOException;

import com.ibm.crail.conf.CrailConstants;
import com.ibm.crail.utils.CrailUtils;

public class CoreSubOperation {
	private long fd;
	private long blockOffset;
	private long blockStart;
	private int len;
	private String key;
	private long fileOffset;
	
	public CoreSubOperation(long fd, long fileOffset, int writeLen) throws IOException {
		this.fd = fd;
		this.fileOffset = fileOffset;
		this.blockOffset = fileOffset % CrailConstants.BLOCK_SIZE;
		this.len = writeLen;
		blockStart = CrailUtils.blockStartAddress(fileOffset);
		this.key = CoreSubOperation.createKey(fd, blockStart);
	}

	public long getBlockOffset() {
		return blockOffset;
	}

	public int getLen() {
		return len;
	}

	@Override
	public String toString() {
		return "fd " + fd + ", fileOffset " + fileOffset + ", blockOffset " + blockOffset + ", len " + len + ", blockStart " + blockStart;
	}

	public long getBlockStart() {
		return blockStart;
	}
	
	public String key(){
		return this.key;
	}
	
	public static String createKey(long fd, long fileOffset){
		long offset = CrailUtils.blockStartAddress(fileOffset);
		return fd + ":" + offset;
	}

	public long getFd() {
		return fd;
	}
}
