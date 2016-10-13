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
import java.util.Iterator;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;

import com.ibm.crail.CrailResult;
import com.ibm.crail.conf.CrailConstants;
import com.ibm.crail.utils.CrailUtils;


public class DirectoryInputStream implements Iterator<String> {
	private static final Logger LOG = CrailUtils.getLogger();
	
	private CoreInputStream stream;
	private ByteBuffer internalBuf;	
	private CoreFileSystem fs;
	private String parent;
	private String currentFile;
	
	public DirectoryInputStream(CoreInputStream stream) throws Exception {
		this.stream = stream;
		this.fs = stream.getFile().getFileSystem();
		this.parent = stream.getFile().getPath();
		this.internalBuf = fs.allocateBuffer();
		this.internalBuf.clear();
		this.internalBuf.position(this.internalBuf.capacity());
		this.currentFile = null;
	}
	
	public boolean hasNext() {
		if (currentFile != null){
			return true;
		}
		while(hasRecord()){
			DirectoryRecord record = nextRecord();
			if (record.isValid()){
				currentFile = CrailUtils.combinePath(record.getParent(), record.getFile());
				break;
			}
		}
		return currentFile != null;
	}
	
	public String next(){
		String ret = currentFile;
		currentFile = null;
		return ret;
	}
	
	public boolean hasRecord() {
		if (fetchIfEmpty()){
			if (internalBuf.remaining() >= DirectoryRecord.MaxSize){
				return true;
			}
		}
		try {
			close();
		} catch(Exception e){
			LOG.info("error when closing directory stream " + e.getMessage());
		}
		return false;
	}

	public DirectoryRecord nextRecord() {
		DirectoryRecord record = new DirectoryRecord(parent);
		record.update(internalBuf);
		return record;
	}	
	
	private boolean fetchIfEmpty() {
		try {
			if (internalBuf.remaining() != 0){
				return true;
			}
			
			internalBuf.clear();
			Future<CrailResult> future = stream.read(internalBuf);
			if (future == null){
				internalBuf.position(internalBuf.limit());
				return false;
			}
			
			long ret = future.get(CrailConstants.DATA_TIMEOUT, TimeUnit.MILLISECONDS).getLen();
			if (ret > 0){
				internalBuf.flip();
			} else {
				internalBuf.position(internalBuf.limit());
				return false;
			}
			
			return true;
		} catch(Exception e){
			return false;
		}
	}	
	
	public void close() throws IOException {
		try {
			if (!stream.isOpen()){
				return;
			}			
			
			stream.close();
			fs.freeBuffer(internalBuf);
		} catch (Exception e) {
			throw new IOException(e);
		}
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
