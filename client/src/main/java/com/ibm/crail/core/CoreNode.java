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

import java.util.concurrent.Future;
import com.ibm.crail.CrailNode;
import com.ibm.crail.namenode.protocol.FileInfo;

public class CoreNode implements CrailNode {
	protected CoreFileSystem fs;
	protected FileInfo fileInfo;
	protected String path;
	protected int storageAffinity;
	protected int locationAffinity;	
	
	protected CoreNode(CoreFileSystem fs, FileInfo fileInfo, String path, int storageAffinity, int locationAffinity){
		this.fs = fs;
		this.fileInfo = fileInfo;
		this.path = path;
		this.storageAffinity = storageAffinity;
		this.locationAffinity = locationAffinity;		
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
	
	public boolean isDir() {
		return fileInfo.isDir();
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
		return this;
	}
	
	FileInfo getFileInfo(){
		return fileInfo;
	}
	
	public CoreFile asFile() throws Exception {
		throw new Exception("Type of file unclear");
	}
	
	public CoreDirectory asDirectory() throws Exception {
		throw new Exception("Type of file unclear");
	}	
	
}

class CoreRenamedNode extends CoreNode {
	private Future<?> srcDirFuture;
	private Future<?> dstDirFuture;
	private DirectoryOutputStream srcStream;
	private DirectoryOutputStream dstStream;
	

	protected CoreRenamedNode(CoreFileSystem fs, FileInfo fileInfo, String path, Future<?> srcDirFuture, Future<?> dstDirFuture, DirectoryOutputStream srcStream, DirectoryOutputStream dstStream){
		super(fs, fileInfo, path, 0, 0);
		this.srcDirFuture = srcDirFuture;
		this.dstDirFuture = dstDirFuture;
		this.srcStream = srcStream;
		this.dstStream = dstStream;
	}

	@Override
	public CoreNode syncDir() throws Exception {
		if (srcDirFuture != null) {
			srcDirFuture.get();
			srcDirFuture = null;
		}
		if (dstDirFuture != null) {
			dstDirFuture.get();
			dstDirFuture = null;
		}		
		if (srcStream != null){
			srcStream.close();
			srcStream = null;
		}
		if (dstStream != null){
			dstStream.close();
			dstStream = null;
		}
		return this;
	}
}

class CoreDeleteNode extends CoreNode {
	private Future<?> dirFuture;
	private DirectoryOutputStream dirStream;	
	
	public CoreDeleteNode(CoreFileSystem fs, FileInfo fileInfo, String path, Future<?> dirFuture, DirectoryOutputStream dirStream){
		super(fs, fileInfo, path, 0, 0);
		this.dirFuture = dirFuture;
		this.dirStream = dirStream;
	}
	
	@Override
	public CoreNode syncDir() throws Exception {
		if (dirFuture != null) {
			dirFuture.get();
			dirFuture = null;
		}
		if (dirStream != null){
			dirStream.close();
			dirStream = null;
		}
		return this;
	}
	
}
