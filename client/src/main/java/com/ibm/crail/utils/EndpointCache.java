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

import java.io.IOException;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;

import com.ibm.crail.CrailStatistics.StatisticsProvider;
import com.ibm.crail.conf.CrailConstants;
import com.ibm.crail.namenode.protocol.DataNodeInfo;
import com.ibm.crail.storage.StorageTier;
import com.ibm.crail.storage.StorageEndpoint;
import com.ibm.crail.*;

public class EndpointCache implements CrailStatistics.StatisticsProvider {
	private static final Logger LOG = CrailUtils.getLogger();
	
	private boolean isOpen;
	private ConcurrentHashMap<Integer, StorageEndpointCache> storageCaches = new ConcurrentHashMap<Integer, StorageEndpointCache>();
	
	public EndpointCache(int fsId, LinkedList<StorageTier> storageGroups){
		int storageTier = 0;
		for (StorageTier group : storageGroups){
			StorageEndpointCache cache = new StorageEndpointCache(fsId, group);
			LOG.info("adding tier to cache " + storageTier);
			storageCaches.put(storageTier++, cache);
		}
		this.isOpen = true;
	}
	
	@Override
	public String providerName() {
		return "EndpointCache";
	}

	@Override
	public String printStatistics() {
		return "size " + size();
	}
	
	public void mergeStatistics(StatisticsProvider provider){
		
	}

	@Override
	public void resetStatistics() {
	}	
	
	public StorageEndpoint getDataEndpoint(DataNodeInfo dataNodeInfo) throws IOException, InterruptedException {
		return storageCaches.get(dataNodeInfo.getStorageTier()).getDataEndpoint(dataNodeInfo);
	}
	
	public int size() {
		int size = 0;
		for (StorageEndpointCache cache : storageCaches.values()){
			size += cache.size();
		}
		return size;
	}	
	
	public void close() throws IOException{
		if (!isOpen){
			return;
		}
		
		for (StorageEndpointCache cache : storageCaches.values()){
			cache.close();
		}
	}
	
	//-------------------------------
	
	public static class StorageEndpointCache {
		private StorageTier datanodeGroup;
		private ConcurrentHashMap<Long, Object> locktable;
		private ConcurrentHashMap<Long, StorageEndpoint> cache;
		private int fsId;
		private boolean isOpen;
		
		public StorageEndpointCache(int fsId, StorageTier datanodeGroup){
			this.fsId = fsId;
			this.datanodeGroup = datanodeGroup;
			this.cache = new ConcurrentHashMap<Long, StorageEndpoint>();
			this.locktable = new ConcurrentHashMap<Long, Object>();
			this.isOpen = true;
		}	
		
		public void close() throws IOException {
			if (!isOpen){
				return;
			}
			
			try {
				datanodeGroup.close();
			} catch(Exception e){
				throw new IOException(e);
			}
		}

		public StorageEndpoint getDataEndpoint(DataNodeInfo dataNodeInfo) throws IOException, InterruptedException {
			StorageEndpoint endpoint = cache.get(dataNodeInfo.key());
			if (endpoint == null) {
				Object lock = getLock(dataNodeInfo.key());
				synchronized (lock) {
					endpoint = cache.get(dataNodeInfo.key());
					if (endpoint == null){
						endpoint = datanodeGroup.createEndpoint(CrailUtils.datanodeInfo2SocketAddr(dataNodeInfo));
						cache.put(dataNodeInfo.key(), endpoint);
						if (CrailConstants.DEBUG) {
							LOG.info("EndpointCache miss " + CrailUtils.datanodeInfo2SocketAddr(dataNodeInfo) + ", fsId " + fsId + ", cache size " + cache.size());
						}
					}
				}
			} else {
				if (CrailConstants.DEBUG) {
					LOG.info("EndpointCache hit " + CrailUtils.datanodeInfo2SocketAddr(dataNodeInfo) + ", fsId " + fsId);
				}
			}
			return endpoint;
		}

		public int size() {
			return cache.size();
		}
		
		private Object getLock(long key){
			Object lock = locktable.get(key);
			if (lock == null){
				lock = new Object();
				Object oldLock = locktable.putIfAbsent(key, lock);
				if (oldLock != null){
					lock = oldLock;
				}
			
			}
			return lock;
		}		
	}
}