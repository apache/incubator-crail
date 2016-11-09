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

import com.ibm.crail.CrailBlockLocation;
import com.ibm.crail.CrailDirectory;
import com.ibm.crail.CrailFS;
import com.ibm.crail.CrailFile;
import com.ibm.crail.CrailInputStream;
import com.ibm.crail.CrailNode;
import com.ibm.crail.CrailOutputStream;
import com.ibm.crail.namenode.protocol.FileInfo;

abstract class CoreFile extends CoreNode implements CrailFile {
	private CrailOutputStream outputStream;
	
	protected CoreFile(CoreFileSystem fs, FileInfo fileInfo, String path, int storageAffinity, int locationAffinity){
		super(fs, fileInfo, path, storageAffinity, locationAffinity);
		this.outputStream = null;
	}
	
	public CrailInputStream getDirectInputStream(long readHint) throws Exception{
		if (fileInfo.isDir()){
			throw new Exception("Cannot open stream for directory");
		}		
		
		return fs.getInputStream(this, readHint);
	}	
	
	public synchronized CrailOutputStream getDirectOutputStream(long writeHint) throws Exception {
		if (fileInfo.isDir()){
			throw new Exception("Cannot open stream for directory");
		}		
		if (fileInfo.getToken() == 0){
			throw new Exception("File is in read mode, cannot create outputstream, fd " + fileInfo.getFd());
		}
		if (outputStream == null){
			outputStream = fs.getOutputStream(this, writeHint);
		}
		return outputStream;
	}
	
	public CrailBlockLocation[] getBlockLocations(long start, long len) throws Exception{
		return fs.getBlockLocations(path, start, len);
	}	
	
	public void close() throws Exception {
		if (fileInfo.getToken() > 0){
			fs.closeFile(fileInfo);
		}
	}	
	
	public long getToken() {
		return fileInfo.getToken();
	}

	public boolean tokenFree(){
		return fileInfo.tokenFree();
	}

	@Override
	public CoreFile asFile() throws Exception {
		return this;
	}
}

class CoreEarlyFile implements CrailFile {
	private CreateFileFuture createFileFuture;
	private CrailFile file;

	public CoreEarlyFile(CreateFileFuture createFileFuture) {
		this.createFileFuture = createFileFuture;
		this.file = null;
	}

	@Override
	public CrailFS getFileSystem() {
		return file().getFileSystem();
	}

	@Override
	public String getPath() {
		return file().getPath();
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
	public boolean isDir() {
		return file().isDir();
	}

	@Override
	public CrailFile asFile() throws Exception {
		return file().asFile();
	}

	@Override
	public CrailDirectory asDirectory() throws Exception {
		return file().asDirectory();
	}

	@Override
	public CrailInputStream getDirectInputStream(long readHint)
			throws Exception {
		return file().getDirectInputStream(readHint);
	}

	@Override
	public CrailOutputStream getDirectOutputStream(long writeHint)
			throws Exception {
		return file().getDirectOutputStream(writeHint);
	}

	@Override
	public CrailBlockLocation[] getBlockLocations(long start, long len)
			throws Exception {
		return file().getBlockLocations(start, len);
	}

	@Override
	public int locationAffinity() {
		return file().locationAffinity();
	}

	@Override
	public int storageAffinity() {
		return file().storageAffinity();
	}

	@Override
	public long getToken() {
		return file().getToken();
	}

	@Override
	public long getFd() {
		return file().getFd();
	}

	@Override
	public void close() throws Exception {
		file().close();
	}
	
	private CrailFile file(){
		if (file == null){
			try {
				file = this.createFileFuture.get();
			} catch(Exception e){
				e.printStackTrace();
			}
		}
		return file;
	}
	
}

class CoreCreateFile extends CoreFile {
	private Future<?> dirFuture;
	private DirectoryOutputStream dirStream;	
	
	public CoreCreateFile(CoreFileSystem fs, FileInfo fileInfo, String path, int storageAffinity, int locationAffinity, Future<?> dirFuture, DirectoryOutputStream dirStream){
		super(fs, fileInfo, path, storageAffinity, locationAffinity);
		this.dirFuture = dirFuture;
		this.dirStream = dirStream;
	}
	
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

class CoreLookupFile extends CoreFile {
	protected CoreLookupFile(CoreFileSystem fs, FileInfo fileInfo, String path) {
		super(fs, fileInfo, path, 0, 0);
	}
}

