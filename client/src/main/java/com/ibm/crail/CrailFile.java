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

package com.ibm.crail;

public abstract class CrailFile {
	public abstract CrailInputStream getDirectInputStream(long readHint) throws Exception;
	public abstract CrailOutputStream getDirectOutputStream(long writeHint) throws Exception;
	public abstract CrailFile syncDir() throws Exception;
	public abstract void close() throws Exception;
	public abstract CrailFS getFileSystem();
	public abstract boolean isDir();
	public abstract long getModificationTime();
	public abstract long getCapacity();
	public abstract int locationAffinity();
	public abstract int storageAffinity();
	public abstract String getPath(); 
	public abstract long getToken();
	public abstract long getFd();

	public CrailBufferedInputStream getBufferedInputStream(long readHint) throws Exception {
		CrailInputStream stream = getDirectInputStream(readHint);
		return new CrailBufferedInputStream(getFileSystem(), stream);
	}
	
	public CrailBufferedOutputStream getBufferedOutputStream(long writeHint) throws Exception {
		CrailOutputStream stream = getDirectOutputStream(writeHint);
		return new CrailBufferedOutputStream(getFileSystem(), stream);
	}
}
