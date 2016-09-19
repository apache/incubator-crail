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

import java.nio.ByteBuffer;
import java.util.concurrent.Future;

import com.ibm.crail.CrailFile;
import com.ibm.crail.CrailInputStream;
import com.ibm.crail.CrailOutputStream;
import com.ibm.crail.namenode.protocol.FileInfo;

public abstract class CoreFile extends CrailFile {
	private CoreFileSystem fs;
	private FileInfo fileInfo;
	private String path;
	private int storageAffinity;
	private int locationAffinity;
	
	protected CoreFile(CoreFileSystem fs, FileInfo fileInfo, String path, int storageAffinity, int locationAffinity){
		this.fs = fs;
		this.fileInfo = fileInfo;
		this.path = path;
		this.storageAffinity = storageAffinity;
		this.locationAffinity = locationAffinity;
	}
	
	public CrailInputStream getDirectInputStream(long readHint) throws Exception{
		if (fileInfo.isDir()){
			throw new Exception("Cannot open stream for directory");
		}		
		
		return fs.getInputStream(this, readHint);
	}	
	
	public CrailOutputStream getDirectOutputStream(long writeHint) throws Exception {
		if (fileInfo.getToken() == 0){
			throw new Exception("File is in read mode, cannot create outputstream, fd " + fileInfo.getFd());
		}
		return fs.getOutputStream(this, writeHint);
	}	
	
	public abstract CrailFile syncDir() throws Exception;
	
	public CoreFileSystem getFileSystem(){
		return this.fs;
	}
	
	public void close() throws Exception {
		if (fileInfo.getToken() > 0){
			fs.closeFile(fileInfo);
		}
	}	
	
	FileInfo getFileInfo(){
		return this.fileInfo;
	}
	
	//---------------
	
	public boolean isDir() {
		return fileInfo.isDir();
	}

	public long getModificationTime() {
		return fileInfo.getModificationTime();
	}

	public long getCapacity() {
		return fileInfo.getCapacity();
	}
	
	public int storageAffinity(){
		return storageAffinity;
	}	

	public int locationAffinity() {
		return locationAffinity;
	}
	
	public long getToken() {
		return fileInfo.getToken();
	}

	public boolean tokenFree(){
		return fileInfo.tokenFree();
	}	
	
	public long getFd() {
		return fileInfo.getFd();
	}

	@Override
	public String getPath() {
		return path;
	}
}

class CoreCreateFile extends CoreFile {
	private Future<?> dirFuture;
	private ByteBuffer dirBuffer;
	private CoreOutputStream dirStream;	
	
	public CoreCreateFile(CoreFileSystem fs, FileInfo fileInfo, String path, int storageAffinity, int locationAffinity, Future<?> dirFuture, ByteBuffer dirBuffer, CoreOutputStream dirStream){
		super(fs, fileInfo, path, storageAffinity, locationAffinity);
		this.dirFuture = dirFuture;
		this.dirBuffer = dirBuffer;		
		this.dirStream = dirStream;
	}
	
	@Override
	public CrailFile syncDir() throws Exception {
		if (dirFuture != null) {
			dirFuture.get();
			dirFuture = null;
		}
		if (dirBuffer != null) {
			this.getFileSystem().freeBuffer(dirBuffer);
			dirBuffer = null;
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

	@Override
	public CrailFile syncDir() throws Exception {
		return this;
	}
	
}


class CoreRenamedFile extends CoreFile {
	private Future<?> srcDirFuture;
	private Future<?> dstDirFuture;
	private ByteBuffer srcDirBuffer;
	private ByteBuffer dstDirBuffer;
	private CoreOutputStream srcStream;
	private CoreOutputStream dstStream;
	

	protected CoreRenamedFile(CoreFileSystem fs, FileInfo fileInfo, String path, Future<?> srcDirFuture, Future<?> dstDirFuture, ByteBuffer srcDirBuffer, ByteBuffer dstDirBuffer, CoreOutputStream srcStream, CoreOutputStream dstStream){
		super(fs, fileInfo, path, 0, 0);
		this.srcDirFuture = srcDirFuture;
		this.dstDirFuture = dstDirFuture;
		this.srcDirBuffer = srcDirBuffer;
		this.dstDirBuffer = dstDirBuffer;
		this.srcStream = srcStream;
		this.dstStream = dstStream;
	}

	@Override
	public CrailFile syncDir() throws Exception {
		if (srcDirFuture != null) {
			srcDirFuture.get();
			srcDirFuture = null;
		}
		if (dstDirFuture != null) {
			dstDirFuture.get();
			dstDirFuture = null;
		}		
		if (srcDirBuffer != null) {
			this.getFileSystem().freeBuffer(srcDirBuffer);
			srcDirBuffer = null;
		}
		if (dstDirBuffer != null) {
			this.getFileSystem().freeBuffer(dstDirBuffer);
			dstDirBuffer = null;
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

class CoreDeleteFile extends CoreFile {
	private Future<?> dirFuture;
	private ByteBuffer dirBuffer;
	private CoreOutputStream dirStream;	
	
	public CoreDeleteFile(CoreFileSystem fs, FileInfo fileInfo, String path, Future<?> dirFuture, ByteBuffer dirBuffer, CoreOutputStream dirStream){
		super(fs, fileInfo, path, 0, 0);
		this.dirFuture = dirFuture;
		this.dirBuffer = dirBuffer;		
		this.dirStream = dirStream;
	}
	
	@Override
	public CrailFile syncDir() throws Exception {
		if (dirFuture != null) {
			dirFuture.get();
			dirFuture = null;
		}
		if (dirBuffer != null) {
			this.getFileSystem().freeBuffer(dirBuffer);
			dirBuffer = null;
		}
		if (dirStream != null){
			dirStream.close();
			dirStream = null;
		}
		return this;
	}
	
}

class CoreDirFile extends CoreFile {

	protected CoreDirFile(CoreFileSystem fs, FileInfo fileInfo, String path) {
		super(fs, fileInfo, path, 0, 0);
	}

	@Override
	public CrailFile syncDir() throws Exception {
		return this;
	}
	
}

