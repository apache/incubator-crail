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

public interface CrailFile extends CrailNode {
	public abstract CrailInputStream getDirectInputStream(long readHint) throws Exception;
	public abstract CrailOutputStream getDirectOutputStream(long writeHint) throws Exception;
	public abstract CrailBlockLocation[] getBlockLocations(long start, long len) throws Exception;
	public abstract int locationAffinity();
	public abstract int storageAffinity();
	public abstract long getToken();
	public abstract long getFd();

	default CrailBufferedInputStream getBufferedInputStream(long readHint) throws Exception {
		return new CrailBufferedInputStream(this, readHint);
	}
	
	default CrailBufferedOutputStream getBufferedOutputStream(long writeHint) throws Exception {
		return new CrailBufferedOutputStream(this, writeHint);
	}
}
