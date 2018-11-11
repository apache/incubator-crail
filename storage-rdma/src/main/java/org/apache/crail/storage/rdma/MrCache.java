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

package org.apache.crail.storage.rdma;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.crail.CrailBuffer;

import com.ibm.disni.verbs.IbvMr;
import com.ibm.disni.verbs.IbvPd;

public class MrCache {
	private ConcurrentHashMap<Integer, DeviceMrCache> cache;
	private AtomicLong cacheOps;	
	private AtomicLong cacheMisses;
	
	public MrCache(){
		this.cache = new ConcurrentHashMap<Integer, DeviceMrCache>();
		this.cacheMisses = new AtomicLong(0);
		this.cacheOps = new AtomicLong(0);
	}
	
	public DeviceMrCache getDeviceCache(IbvPd pd) throws IOException {
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
		
		public IbvMr get(CrailBuffer buffer) throws IOException{
			IbvMr mr = device.get(buffer.address());
			return mr;
		}
		
		public void put(IbvMr mr) throws IOException{
			device.put(mr.getAddr(), mr);
		}		
		
		public void close() throws IOException {
			for (Iterator<IbvMr> iter2 = device.values().iterator(); iter2.hasNext(); ){
				IbvMr mr = iter2.next();
				mr.deregMr().free();
			}
			device.clear();
		}

		public IbvPd getPd() {
			return pd;
		}
	}
}

