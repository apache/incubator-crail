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

import com.ibm.crail.CrailMultiFile;
import com.ibm.crail.CrailNode;
import com.ibm.crail.CrailNodeType;
import com.ibm.crail.namenode.protocol.FileInfo;

public class CoreNode implements CrailNode {
	protected CoreFileSystem fs;
	protected FileInfo fileInfo;
	protected String path;
	protected int storageAffinity;
	protected int locationAffinity;	
	private LinkedBlockingQueue<CoreSyncOperation> syncOperations;
	
	public static CoreNode create(CoreFileSystem fs, FileInfo fileInfo, String path, int storageAffinity, int locationAffinity) {
		if (fileInfo.getType().isContainer()){
			return new CoreDirectory(fs, fileInfo, path, storageAffinity, locationAffinity);		
		} else {
			return new CoreFile(fs, fileInfo, path, storageAffinity, locationAffinity);
		}
	}	
	
	protected CoreNode(CoreFileSystem fs, FileInfo fileInfo, String path, int storageAffinity, int locationAffinity){
		this.fs = fs;
		this.fileInfo = fileInfo;
		this.path = path;
		this.storageAffinity = storageAffinity;
		this.locationAffinity = locationAffinity;
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
	
	public int storageAffinity(){
		return storageAffinity;
	}	

	public int locationAffinity() {
		return locationAffinity;
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
	
	public CoreDirectory asDirectory() throws Exception {
		throw new Exception("Type of file unclear");
	}	
	
	public CrailMultiFile asMultiFile() throws Exception {
		throw new Exception("Type of file unclear");
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

//class CoreEarlyNode implements CrailNode {
//	private static final Logger LOG = CrailUtils.getLogger();
//	
//	private CreateNodeFuture createFileFuture;
//	private CrailNode file;
//	private String path;
//	private CoreFileSystem fs;
//	private int storageAffinity;
//	private int locationAffinity;
//
//	public CoreEarlyNode(CreateNodeFuture createFileFuture, String path, CoreFileSystem fs, int storageAffinity, int locationAffinity) {
//		this.createFileFuture = createFileFuture;
//		this.file = null;
//		this.path = path;
//		this.fs = fs;
//		this.storageAffinity = storageAffinity;
//		this.locationAffinity = locationAffinity;
//	}
//
//	@Override
//	public CrailFS getFileSystem() {
//		return fs;
//	}
//
//	@Override
//	public String getPath() {
//		return path;
//	}
//
//	@Override
//	public CrailNode syncDir() throws Exception {
//		return file().syncDir();
//	}
//
//	@Override
//	public long getModificationTime() {
//		try {
//			return file().getModificationTime();
//		} catch(Exception e){
//			LOG.info("Error: " + e.getMessage());
//			return -1;
//		}
//	}
//
//	@Override
//	public long getCapacity() {
//		try {
//			return file().getCapacity();
//		} catch(Exception e){
//			LOG.info("Error: " + e.getMessage());
//			return -1;
//		}
//	}
//	
//	@Override
//	public CrailNodeType getType() {
//		try {
//			return file().getType();
//		} catch(Exception e){
//			LOG.info("Error: " + e.getMessage());
//			return CrailNodeType.DATAFILE;
//		}
//	}	
//
//	@Override
//	public CrailFile asFile() throws Exception {
//		try {
//			return file().asFile();
//		} catch(Exception e){
//			LOG.info("Error: " + e.getMessage());
//			return null;
//		}
//	}
//
//	@Override
//	public CrailDirectory asDirectory() throws Exception {
//		return file().asDirectory();
//	}
//	
//	@Override
//	public CrailMultiFile asMultiFile() throws Exception {
//		return file().asMultiFile();
//	}	
//
//	private synchronized CrailNode file() throws Exception {
//		if (file == null){
//			file = this.createFileFuture.get();
//		}
//		return file;
//	}
//}
