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

package com.ibm.crail.storage.rdma.client;

import java.nio.ByteBuffer;

public class RdmaBlockIndex {
	private int key;
	private long addr;
	private String path;
	
	public RdmaBlockIndex(){
		this.key = 0;
		this.addr = 0;
		this.path = "";
	}
	
	public RdmaBlockIndex(int key, long addr, String path){
		this.key = key;
		this.addr = addr;
		this.path = path;
	}
	
	public void update(ByteBuffer buffer){
		this.key = buffer.getInt();
		this.addr = buffer.getLong();
		int pathLength = buffer.getInt();
		byte[] pathBuffer = new byte[pathLength];
		buffer.get(pathBuffer);
		this.path = new String(pathBuffer);
	}
	
	public void write(ByteBuffer buffer){
		buffer.putInt(key);
		buffer.putLong(addr);
		byte[] pathBuffer = path.getBytes();
		buffer.putInt(pathBuffer.length);
		buffer.put(pathBuffer);
	}

	public int getKey() {
		return key;
	}

	public void setKey(int key) {
		this.key = key;
	}

	public long getAddr() {
		return addr;
	}

	public void setAddr(long addr) {
		this.addr = addr;
	}

	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}

	@Override
	public String toString() {
		return "key " + key + ", addr " + addr + ", path " + path;
	}
}
