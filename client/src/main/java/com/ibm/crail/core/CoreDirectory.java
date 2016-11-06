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

import java.util.Iterator;
import java.util.concurrent.Future;

import com.ibm.crail.CrailDirectory;
import com.ibm.crail.conf.CrailConstants;
import com.ibm.crail.namenode.protocol.FileInfo;

class CoreDirectory extends CoreNode implements CrailDirectory {
	
	protected CoreDirectory(CoreFileSystem fs, FileInfo fileInfo, String path){
		super(fs, fileInfo, path, 0, 0);
		this.fs = fs;
		this.fileInfo = fileInfo;
		this.path = path;
	}	

	@Override
	public int files() {
		return (int) fileInfo.getCapacity()/CrailConstants.DIRECTORY_RECORD;
	}

	@Override
	public Iterator<String> listEntries() throws Exception {
		return fs.listEntries(path);
	}

	@Override
	public CoreDirectory asDirectory() throws Exception {
		return this;
	}
}

class CoreMakeDirectory extends CoreDirectory {
	private Future<?> dirFuture;
	private DirectoryOutputStream dirStream;			

	protected CoreMakeDirectory(CoreFileSystem fs, FileInfo fileInfo, String path, Future<?> dirFuture, DirectoryOutputStream dirStream) {
		super(fs, fileInfo, path);
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
