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

package com.ibm.crail.storage.rdma;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.ibm.disni.util.MemoryUtils;
import com.ibm.disni.rdma.verbs.IbvMr;
import com.ibm.disni.rdma.verbs.IbvPd;

public class MrCache {
	private ConcurrentHashMap<Integer, DeviceMrCache> cache;
	private AtomicLong cacheOps;	
	private AtomicLong cacheMisses;
	
	public MrCache(){
		this.cache = new ConcurrentHashMap<Integer, DeviceMrCache>();
		this.cacheMisses = new AtomicLong(0);
		this.cacheOps = new AtomicLong(0);
	}
	
	public DeviceMrCache getDeviceCache(IbvPd pd){
		DeviceMrCache deviceCache = cache.get(pd.getHandle());
		if (deviceCache == null) {
			deviceCache = new DeviceMrCache(pd);
			DeviceMrCache oldLine = cache.putIfAbsent(pd.getHandle(), deviceCache);
			if (oldLine != null) {
				deviceCache = oldLine;
			}
		}
		return deviceCache;
	}
	
	public void close() throws IOException {
		for (Iterator<DeviceMrCache> iter1 = cache.values().iterator(); iter1.hasNext(); ){
			DeviceMrCache line = iter1.next();
			line.close();
		}
		cache.clear();			
	}
	
	public long ops() {
		return cacheOps.get();
	}
	
	public long missed() {
		return cacheMisses.get();
	}	
	
	public void reset(){
		this.cacheOps.set(0);
		this.cacheMisses.set(0);
	}
	
	public static class DeviceMrCache {
		private IbvPd pd;
		private ConcurrentHashMap<Long, IbvMr> device;
		
		public DeviceMrCache(IbvPd pd){
			this.pd = pd;
			this.device = new ConcurrentHashMap<Long, IbvMr>();
		}
		
		public IbvMr get(ByteBuffer buffer) throws IOException{
			long address = MemoryUtils.getAddress(buffer);
			IbvMr mr = device.get(address);
			return mr;
		}
		
		public void put(IbvMr mr) throws IOException{
			device.put(mr.getAddr(), mr);
		}		
		
		public void close() throws IOException {
			for (Iterator<IbvMr> iter2 = device.values().iterator(); iter2.hasNext(); ){
				IbvMr mr = iter2.next();
				mr.deregMr();
			}
			device.clear();
		}

		public IbvPd getPd() {
			return pd;
		}
	}
}

