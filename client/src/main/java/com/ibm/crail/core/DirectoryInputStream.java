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
import java.nio.ByteBuffer;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.ibm.crail.CrailResult;
import com.ibm.crail.conf.CrailConstants;


public class DirectoryInputStream extends CoreInputStream {
	private ByteBuffer internalBuf;	
	private CoreFileSystem fs;

	public DirectoryInputStream(CoreFile file, long streamId) throws Exception {
		super(file, streamId, 0);
		this.fs = file.getFileSystem();
		this.internalBuf = fs.allocateBuffer();
		if (internalBuf.capacity() % DirectoryRecord.MaxSize != 0){
			throw new IOException("buffer from cache is not a multiple of " + DirectoryRecord.MaxSize);
		}
		this.internalBuf.clear();
		this.internalBuf.position(this.internalBuf.capacity());		
	}
	
	DirectoryRecord read(String name) throws Exception {
		if (!internalBuf.hasRemaining()){
			internalBuf.clear();
			Future<CrailResult> future = readAsync(internalBuf);
			long res = future != null ? future.get(CrailConstants.DATA_TIMEOUT, TimeUnit.MILLISECONDS).getLen() : -1;
			if (res >= DirectoryRecord.MaxSize){
				internalBuf.flip();
			} else {
				internalBuf.position(internalBuf.capacity());
			}
		}
		return DirectoryRecord.fromBuffer(name, internalBuf);
	}
	
	public void close() throws IOException {
		if (!this.isOpen()){
			return;
		}
		
		super.close();
		fs.freeBuffer(internalBuf);
	}	
	
	//debug
	
	public int getBufCapacity(){
		return internalBuf.capacity();
	}
	
	public int getBufPosition(){
		return internalBuf.position();
	}
	
	public int getBufLimit(){
		return internalBuf.limit();
	}
}
