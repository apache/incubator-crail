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
import java.nio.ByteBuffer;

import com.ibm.crail.conf.CrailConstants;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public class DirectBufferCache {
	private LinkedBlockingQueue<ByteBuffer> cache;
	
	private AtomicLong cacheGet;
	private AtomicLong cachePut;
	private AtomicLong cacheMisses;
	private AtomicLong cacheOut;
	private AtomicLong cacheMax;
	
	public DirectBufferCache() throws IOException{
		this.cache = new LinkedBlockingQueue<ByteBuffer>();
		
		this.cacheGet = new AtomicLong(0);
		this.cachePut = new AtomicLong(0);
		this.cacheMisses = new AtomicLong(0);
		this.cacheOut = new AtomicLong(0);
		this.cacheMax = new AtomicLong(0);
	}
	
	public ByteBuffer getBuffer() throws IOException {
		cacheGet.incrementAndGet();
		cacheOut.incrementAndGet();
		cacheMax.updateAndGet(x -> Math.max(x, cacheOut.get()));
		
		ByteBuffer buffer = cache.poll();
		if (buffer == null){
			synchronized(this){
				buffer = cache.poll();
				if (buffer == null){
					cacheMisses.incrementAndGet();
					buffer = allocateBuffer();
				}
			}
		} 
		
		buffer.clear();
		return buffer;
	}
	
	public void putBuffer(ByteBuffer buffer) throws IOException{
		cachePut.incrementAndGet();
		cacheOut.decrementAndGet();
		putBufferInternal(buffer);
	}
	
	public void putBufferInternal(ByteBuffer buffer) throws IOException{
		cache.add(buffer);
	}	
	
	public long get() {
		return cacheGet.get();
	}
	
	public long put(){
		return cachePut.get();
	}
	
	public long missed() {
		return cacheMisses.get();
	}
	
	public long max() {
		return cacheMax.get();
	}
	
	public long size() {
		return cache.size();
	}	
	
	public void close(){
		cache.clear();
	}

	public void reset(){
		this.cacheGet.set(0);
		this.cachePut.set(0);
		this.cacheMisses.set(0);
		this.cacheOut.set(0);
		this.cacheMax.set(0);
	}
	
	protected ByteBuffer allocateBuffer() throws IOException{
		return ByteBuffer.allocateDirect(CrailConstants.BUFFER_SIZE);
	}

	public ByteBuffer getAllocationBuffer(ByteBuffer buffer) {
		return null;
	}

	public long missedHeap() {
		return 0;
	}

	public long missedMap() {
		return 0;
	}
}

