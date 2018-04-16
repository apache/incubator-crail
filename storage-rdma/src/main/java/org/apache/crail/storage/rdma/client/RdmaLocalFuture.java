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

package org.apache.crail.storage.rdma.client;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.crail.storage.StorageFuture;
import org.apache.crail.storage.StorageResult;

import sun.misc.Unsafe;

public class RdmaLocalFuture implements StorageFuture, StorageResult {
	private Unsafe unsafe;
	private long srcAddr;
	private long dstAddr;
	private int remaining;
	
	private int len;
	private boolean isDone;

	public RdmaLocalFuture(Unsafe unsafe, long srcAddr, long dstAddr, int remaining) {
		this.unsafe = unsafe;
		this.srcAddr = srcAddr;
		this.dstAddr = dstAddr;
		this.remaining = remaining;
		
		this.len = 0;
		this.isDone = false;
	}


	@Override
	public int getLen() {
		return len;
	}

	@Override
	public boolean cancel(boolean mayInterruptIfRunning) {
		return false;
	}

	@Override
	public boolean isCancelled() {
		return false;
	}

	@Override
	public boolean isDone() {
		if (!isDone){
			getDone();
		}		
		return isDone;
	}

	@Override
	public StorageResult get() throws InterruptedException, ExecutionException {
		if (!isDone){
			getDone();
		}
		return this;
	}

	@Override
	public StorageResult get(long timeout, TimeUnit unit)
			throws InterruptedException, ExecutionException, TimeoutException {
		if (!isDone){
			getDone();
		}		
		return this;
	}
	
	@Override
	public boolean isSynchronous() {
		return true;
	}
	
	void getDone(){
		unsafe.copyMemory(srcAddr, dstAddr, remaining);
		len = remaining;
		isDone = true;		
	}
}
