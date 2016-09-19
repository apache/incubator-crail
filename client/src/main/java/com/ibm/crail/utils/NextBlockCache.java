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

package com.ibm.crail.utils;

import java.util.concurrent.ConcurrentHashMap;

import com.ibm.crail.namenode.rpc.RpcNameNodeFuture;
import com.ibm.crail.namenode.rpc.RpcResponseMessage;

public class NextBlockCache {
	private ConcurrentHashMap<Long, FileNextBlockCache> nextBlockCache;
	
	public NextBlockCache(){
		this.nextBlockCache = new ConcurrentHashMap<Long, FileNextBlockCache>();  
	}
	
	public FileNextBlockCache getFileBlockCache(long fd){
		FileNextBlockCache fileBlockCache = nextBlockCache.get(fd);
		if (fileBlockCache == null){
			fileBlockCache = new FileNextBlockCache(fd);
			FileNextBlockCache oldFileBlockCache = nextBlockCache.putIfAbsent(fd, fileBlockCache);
			if (oldFileBlockCache != null){
				fileBlockCache = oldFileBlockCache;
			}
		}
		return fileBlockCache;
	}
	
	public void remove(long fd) {
		nextBlockCache.remove(fd);
	}	
	
	public static class FileNextBlockCache {
		private long fd;
		private ConcurrentHashMap<String, RpcNameNodeFuture<RpcResponseMessage.GetBlockRes>> fileBlockCache;
		
		public FileNextBlockCache(long fd){
			this.fd = fd;
			this.fileBlockCache = new ConcurrentHashMap<String, RpcNameNodeFuture<RpcResponseMessage.GetBlockRes>>();
		}

		public void put(String key, RpcNameNodeFuture<RpcResponseMessage.GetBlockRes> block){
			this.fileBlockCache.putIfAbsent(key, block);
		}
		
		public RpcNameNodeFuture<RpcResponseMessage.GetBlockRes> get(String key){
			return this.fileBlockCache.get(key);
		}

		public boolean containsKey(String key) {
			return this.fileBlockCache.containsKey(key);
		}

		public long getFd() {
			return fd;
		}
	}
}
