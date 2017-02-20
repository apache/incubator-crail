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
import com.ibm.crail.CrailFile;
import com.ibm.crail.CrailInputStream;
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

