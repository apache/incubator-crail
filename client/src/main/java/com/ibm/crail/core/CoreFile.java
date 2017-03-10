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

import java.util.concurrent.Semaphore;
import com.ibm.crail.CrailBlockLocation;
import com.ibm.crail.CrailDirectory;
import com.ibm.crail.CrailFS;
import com.ibm.crail.CrailFile;
import com.ibm.crail.CrailInputStream;
import com.ibm.crail.CrailMultiFile;
import com.ibm.crail.CrailNode;
import com.ibm.crail.CrailNodeType;
import com.ibm.crail.CrailOutputStream;
import com.ibm.crail.namenode.protocol.FileInfo;

public class CoreFile extends CoreNode implements CrailFile {
	private Semaphore outputStreams;
	
	public CoreFile(CoreFileSystem fs, FileInfo fileInfo, String path, int storageAffinity, int locationAffinity){
		super(fs, fileInfo, path, storageAffinity, locationAffinity);
		this.outputStreams = new Semaphore(1);
	}
	
	public CrailInputStream getDirectInputStream(long readHint) throws Exception{
		if (fileInfo.getType().isDirectory()){
			throw new Exception("Cannot open stream for directory");
		}		
		
		return super.getInputStream(readHint);
	}	
	
	public synchronized CrailOutputStream getDirectOutputStream(long writeHint) throws Exception {
		if (fileInfo.getType().isDirectory()){
			throw new Exception("Cannot open stream for directory");
		}		
		if (fileInfo.getToken() == 0){
			throw new Exception("File is in read mode, cannot create outputstream, fd " + fileInfo.getFd());
		}
		if (!outputStreams.tryAcquire()){
			throw new Exception("Only one concurrent output stream per file allowed");
		}
		return super.getOutputStream(writeHint);
	}
	
	public CrailBlockLocation[] getBlockLocations(long start, long len) throws Exception{
		return fs.getBlockLocations(path, start, len);
	}	
	
	public long getToken() {
		return fileInfo.getToken();
	}

	public boolean tokenFree(){
		return fileInfo.tokenFree();
	}

	public CoreFile asFile() throws Exception {
		return this;
	}

	void closeOutputStream(CoreOutputStream stream) throws Exception {
		super.closeOutputStream(stream);
		outputStreams.release();
	}
}

class CoreEarlyFile implements CrailFile {
	private CoreFileSystem fs;
	private String path;
	private CrailNodeType type;
	private int storageAffnity;
	private int locationAffnity;
	private CreateNodeFuture future;
	private CrailFile file;
	
	public CoreEarlyFile(CoreFileSystem fs, String path, CrailNodeType type, int storageAffinity, int locationAffinity, CreateNodeFuture future) {
		this.fs = fs;
		this.path = path;
		this.type = type;
		this.future = future;
		this.file = null;
	}

	public CrailInputStream getDirectInputStream(long readHint) throws Exception{
		return file().getDirectInputStream(readHint);
	}	
	
	public synchronized CrailOutputStream getDirectOutputStream(long writeHint) throws Exception {
		return file().getDirectOutputStream(writeHint);
	}
	
	public CrailBlockLocation[] getBlockLocations(long start, long len) throws Exception{
		return fs.getBlockLocations(path, start, len);
	}	
	
	public long getToken() {
		return file().getToken();
	}

	public CrailFile asFile() throws Exception {
		return this;
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
		return file().getModificationTime();
	}

	@Override
	public long getCapacity() {
		return file().getCapacity();
	}

	@Override
	public CrailNodeType getType() {
		return type;
	}

	@Override
	public CrailDirectory asDirectory() throws Exception {
		throw new Exception("this is not a directory");
	}

	@Override
	public CrailMultiFile asMultiFile() throws Exception {
		throw new Exception("this is not a multifile");
	}

	@Override
	public int locationAffinity() {
		return this.locationAffnity;
	}

	@Override
	public int storageAffinity() {
		return this.storageAffnity;
	}

	@Override
	public long getFd() {
		return file().getFd();
	}

	private synchronized CrailFile file() {
		try {
			if (file == null){
				file = this.future.get().asFile();
			}
			return file;
		} catch(Exception e){
			throw new RuntimeException(e);
		}
	}
}

