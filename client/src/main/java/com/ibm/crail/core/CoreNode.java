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

import java.util.concurrent.LinkedBlockingQueue;

import com.ibm.crail.CrailBlockLocation;
import com.ibm.crail.CrailKeyValue;
import com.ibm.crail.CrailMultiFile;
import com.ibm.crail.CrailNode;
import com.ibm.crail.CrailNodeType;
import com.ibm.crail.CrailTable;
import com.ibm.crail.metadata.FileInfo;

public class CoreNode implements CrailNode {
	protected CoreFileSystem fs;
	protected FileInfo fileInfo;
	protected String path;
	private LinkedBlockingQueue<CoreSyncOperation> syncOperations;
	
	public static CoreNode create(CoreFileSystem fs, FileInfo fileInfo, String path) {
		if (fileInfo.getType().isContainer()){
			return new CoreDirectory(fs, fileInfo, path);		
		} else {
			return new CoreFile(fs, fileInfo, path);
		}
	}	
	
	protected CoreNode(CoreFileSystem fs, FileInfo fileInfo, String path){
		this.fs = fs;
		this.fileInfo = fileInfo;
		this.path = path;
		this.syncOperations = new LinkedBlockingQueue<CoreSyncOperation>();
	}	

	@Override
	public CoreFileSystem getFileSystem() {
		return fs;
	}

	@Override
	public String getPath() {
		return path;
	}
	
	public long getModificationTime() {
		return fileInfo.getModificationTime();
	}	
	
	public long getCapacity() {
		return fileInfo.getCapacity();
	}
	
	public CrailNodeType getType() {
		return fileInfo.getType();
	}
	
	public long getFd() {
		return fileInfo.getFd();
	}	

	@Override
	public CoreNode syncDir() throws Exception {
		while(!syncOperations.isEmpty()){
			CoreSyncOperation syncOp = syncOperations.poll();
			syncOp.close();
		}			
		return this;
	}
	
	public CoreFile asFile() throws Exception {
		throw new Exception("Type of file unclear");
	}
	
	public CoreDirectory asContainer() throws Exception {
		throw new Exception("Type of file unclear");
	}	
	
	public CoreDirectory asDirectory() throws Exception {
		throw new Exception("Type of file unclear");
	}	
	
	public CrailMultiFile asMultiFile() throws Exception {
		throw new Exception("Type of file unclear");
	}
	
	public CrailTable asTable() throws Exception {
		throw new Exception("Type of file unclear");
	}	
	
	public CrailKeyValue asKeyValue() throws Exception {
		throw new Exception("Type of file unclear");
	}	
	
	public CrailBlockLocation[] getBlockLocations(long start, long len) throws Exception {
		return fs.getBlockLocations(path, start, len);
	}	
	
	protected CoreInputStream getInputStream(long readHint) throws Exception{
		return fs.getInputStream(this, readHint);
	}	
	
	CoreOutputStream getOutputStream(long writeHint) throws Exception {
		return fs.getOutputStream(this, writeHint);
	}	

	void closeInputStream(CoreInputStream coreStream) throws Exception {
		fs.unregisterInputStream(coreStream);
	}
	
	void closeOutputStream(CoreOutputStream coreStream) throws Exception {
		syncDir();
		fs.unregisterOutputStream(coreStream);
	}	
	
	FileInfo getFileInfo(){
		return fileInfo;
	}	
	
	void addSyncOperation(CoreSyncOperation operation){
		this.syncOperations.add(operation);
	}
}

