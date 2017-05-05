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

package com.ibm.crail.memory;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.ibm.crail.CrailStatistics.StatisticsProvider;
import com.ibm.crail.conf.CrailConstants;
import com.ibm.crail.utils.CrailUtils;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;

import com.ibm.crail.*;

public abstract class BufferCache implements CrailStatistics.StatisticsProvider {
	private static final Logger LOG = CrailUtils.getLogger();
	private LinkedBlockingQueue<CrailBuffer> cache;
	
	private AtomicLong cacheGet;
	private AtomicLong cachePut;
	private AtomicLong cacheMisses;
	private AtomicLong cacheOut;
	private AtomicLong cacheMax;
	
	private AtomicLong cacheMissesMap;
	private AtomicLong cacheMissesHeap;		
	
	public BufferCache() throws IOException{
		this.cache = new LinkedBlockingQueue<CrailBuffer>();
		
		this.cacheGet = new AtomicLong(0);
		this.cachePut = new AtomicLong(0);
		this.cacheMisses = new AtomicLong(0);
		this.cacheOut = new AtomicLong(0);
		this.cacheMax = new AtomicLong(0);
		
		this.cacheMissesMap = new AtomicLong(0);
		this.cacheMissesHeap = new AtomicLong(0);			
	}
	
	@Override
	public String providerName() {
		return "cache/buffer";
	}

	@Override
	public String printStatistics() {
		return "cacheGet " + cacheGet.get() + ", cachePut " + cachePut.get() + ", cacheMiss " + cacheMisses.get() + ", cacheSize " + cache.size() +  ", cacheMax " + cacheMax.get() + ", mapMiss " + cacheMissesMap.get() + ", mapHeap " + cacheMissesHeap.get();
	}
	
	public void resetStatistics(){
		this.cacheGet.set(0);
		this.cachePut.set(0);
		this.cacheMisses.set(0);
		this.cacheOut.set(0);
		this.cacheMax.set(0);
		this.cacheMissesMap.set(0);
		this.cacheMissesHeap.set(0);
	}	
	
	public void mergeStatistics(StatisticsProvider provider){
		
	}
	
	public CrailBuffer getBuffer() throws IOException {
		cacheGet.incrementAndGet();
		cacheOut.incrementAndGet();
		cacheMax.updateAndGet(x -> Math.max(x, cacheOut.get()));
		
		CrailBuffer buffer = cache.poll();
		if (buffer == null){
			synchronized(this){
				buffer = cache.poll();
				if (buffer == null){
					cacheMisses.incrementAndGet();
					buffer = allocateBuffer();
					if (buffer == null){
						buffer = OffHeapBuffer.wrap(ByteBuffer.allocateDirect(CrailConstants.BUFFER_SIZE));
						cacheMissesHeap.incrementAndGet();
					} else {
						cacheMissesMap.incrementAndGet();
					}
				}
			}
		} 
		
		buffer.clear();
		return buffer;
	}
	
	public void putBuffer(CrailBuffer buffer) throws IOException{
		if (buffer != null){
			cachePut.incrementAndGet();
			cacheOut.decrementAndGet();
			putBufferInternal(buffer);
		}
	}
	
	public void putBufferInternal(CrailBuffer buffer) throws IOException{
		cache.add(buffer);
	}	
	
	public void close(){
		cache.clear();
	}
	
	public abstract CrailBuffer allocateBuffer() throws IOException;

	@SuppressWarnings("unchecked")
	public static BufferCache createInstance(String name) throws Exception {
		Class<?> nodeClass = Class.forName(name);
		if (BufferCache.class.isAssignableFrom(nodeClass)){
			Class<? extends BufferCache> bufferCacheClass = (Class<? extends BufferCache>) nodeClass;
			BufferCache bufferCache = bufferCacheClass.newInstance();
			return bufferCache;
		} else {
			throw new Exception("Cannot instantiate storage client of type " + name);
		}
		
	}		
}

