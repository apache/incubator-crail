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

import org.slf4j.Logger;

import com.ibm.crail.CrailBlockLocation;
import com.ibm.crail.CrailDirectory;
import com.ibm.crail.CrailFS;
import com.ibm.crail.CrailFile;
import com.ibm.crail.CrailInputStream;
import com.ibm.crail.CrailNode;
import com.ibm.crail.CrailOutputStream;
import com.ibm.crail.namenode.protocol.FileInfo;
import com.ibm.crail.namenode.protocol.FileType;
import com.ibm.crail.utils.CrailUtils;

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
	
	public CoreFile asFile() throws Exception {
		throw new Exception("Type of file unclear");
	}
	
	public CoreDirectory asDirectory() throws Exception {
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
		fs.unregisterOutputStream(coreStream);
	}	
	
	FileInfo getFileInfo(){
		return fileInfo;
	}	
}

class CoreCreateNode extends CoreNode {
	private Future<?> dirFuture;
	private DirectoryOutputStream dirStream;	
	
	public CoreCreateNode(CoreFileSystem fs, String path, FileType type, int storageAffinity, int locationAffinity, FileInfo fileInfo, Future<?> dirFuture, DirectoryOutputStream dirStream){
		super(fs, fileInfo, path, 0, 0);
		this.dirFuture = dirFuture;
		this.dirStream = dirStream;
	}
	
	@Override
	public synchronized CoreNode syncDir() throws Exception {
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

	@Override
	void closeOutputStream(CoreOutputStream coreStream) throws Exception {
		syncDir();
		super.closeOutputStream(coreStream);
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
	public synchronized CoreNode syncDir() throws Exception {
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

	@Override
	void closeOutputStream(CoreOutputStream coreStream) throws Exception {
		syncDir();
		super.closeOutputStream(coreStream);
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
	public synchronized CoreNode syncDir() throws Exception {
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

	@Override
	void closeOutputStream(CoreOutputStream coreStream) throws Exception {
		syncDir();
		super.closeOutputStream(coreStream);
	}
}

class CoreEarlyNode implements CrailNode {
	private static final Logger LOG = CrailUtils.getLogger();
	
	private CreateNodeFuture createFileFuture;
	private CrailNode file;
	private String path;
	private CoreFileSystem fs;
	private int storageAffinity;
	private int locationAffinity;

	public CoreEarlyNode(CreateNodeFuture createFileFuture, String path, CoreFileSystem fs, int storageAffinity, int locationAffinity) {
		this.createFileFuture = createFileFuture;
		this.file = null;
		this.path = path;
		this.fs = fs;
		this.storageAffinity = storageAffinity;
		this.locationAffinity = locationAffinity;
	}

	@Override
	public CrailFS getFileSystem() {
		return fs;
	}

	@Override
	public String getPath() {
		return path;
	}

	@Override
	public CrailNode syncDir() throws Exception {
		return file().syncDir();
	}

	@Override
	public long getModificationTime() {
		try {
			return file().getModificationTime();
		} catch(Exception e){
			LOG.info("Error: " + e.getMessage());
			return -1;
		}
	}

	@Override
	public long getCapacity() {
		try {
			return file().getCapacity();
		} catch(Exception e){
			LOG.info("Error: " + e.getMessage());
			return -1;
		}
	}

	@Override
	public boolean isDir() {
		try {
			return file().isDir();
		} catch(Exception e){
			LOG.info("Error: " + e.getMessage());
			return false;
		}
	}

	@Override
	public CrailFile asFile() throws Exception {
		try {
			return file().asFile();
		} catch(Exception e){
			LOG.info("Error: " + e.getMessage());
			return null;
		}
	}

	@Override
	public CrailDirectory asDirectory() throws Exception {
		return file().asDirectory();
	}

	private synchronized CrailNode file() throws Exception {
		if (file == null){
			file = this.createFileFuture.get();
		}
		return file;
	}
}
