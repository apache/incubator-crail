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
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.crail.storage.StorageFuture;
import org.apache.crail.storage.StorageResult;

public class RdmaActiveFuture implements StorageFuture, StorageResult {
	protected static int RPC_PENDING = 0;
	protected static int RPC_DONE = 1;
	protected static int RPC_ERROR = 2;		
	
	private long wrid;
	private int len;
	private boolean isWrite;
	private AtomicInteger status;

	public RdmaActiveFuture(long wrid, int len, boolean isWrite) {
		this.wrid = wrid;
		this.len = len;
		this.isWrite = isWrite;	
		this.status = new AtomicInteger(RPC_PENDING);
	}	
	
	public long getWrid() {
		return wrid;
	}
	
	@Override
	public synchronized StorageResult get() throws InterruptedException, ExecutionException {
		if (status.get() == RPC_PENDING){
			try {
				wait();
			} catch (Exception e) {
				status.set(RPC_ERROR);
				throw new InterruptedException(e.getMessage());
			}
		}
		
		if (status.get() == RPC_DONE){
			return this;
		} else if (status.get() == RPC_PENDING){
			throw new InterruptedException("RPC timeout");
		} else {
			throw new InterruptedException("RPC error");
		}
	}

	@Override
	public synchronized StorageResult get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException {
		if (status.get() == RPC_PENDING){
			try {
				wait(timeout);
			} catch (Exception e) {
				status.set(RPC_ERROR);
				throw new InterruptedException(e.getMessage());
			}
		}
		
		if (status.get() == RPC_DONE){
			return this;
		} else if (status.get() == RPC_PENDING){
			throw new InterruptedException("RPC timeout");
		} else {
			throw new InterruptedException("RPC error");
		}
	}	
	
	public boolean isDone(){
		return status.get() > 0;
	}
	
	public synchronized void signal(){
		status.set(RPC_DONE);
		notify();
	}

	public int getLen() {
		if (status.get() == RPC_DONE){
			return len;
		} else if (status.get() == RPC_PENDING){
			return 0;
		} else {
			return -1;
		}
	}

	public boolean isWrite() {
		return isWrite;
	}

	@Override
	public boolean cancel(boolean mayInterruptIfRunning) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isCancelled() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isSynchronous() {
		return false;
	}
}
