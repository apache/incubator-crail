/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.crail.utils;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.crail.rpc.RpcFuture;
import org.apache.crail.rpc.RpcGetBlock;

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
	
	public void purge() {
		nextBlockCache.clear();
	}

	public static class FileNextBlockCache {
		private long fd;
		private ConcurrentHashMap<Long, RpcFuture<RpcGetBlock>> fileBlockCache;
		
		public FileNextBlockCache(long fd){
			this.fd = fd;
			this.fileBlockCache = new ConcurrentHashMap<Long, RpcFuture<RpcGetBlock>>();
		}

		public void put(long blockstart, RpcFuture<RpcGetBlock> block){
			this.fileBlockCache.putIfAbsent(blockstart, block);
		}
		
		public RpcFuture<RpcGetBlock> get(long blockstart){
			return this.fileBlockCache.get(blockstart);
		}

		public boolean containsKey(long blockstart) {
			return this.fileBlockCache.containsKey(blockstart);
		}

		public long getFd() {
			return fd;
		}
	}
}
