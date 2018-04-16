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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.crail.conf.CrailConstants;

public abstract class MultiFuture<R,T> implements Future<T> {
	protected static int RPC_PENDING = 0;
	protected static int RPC_DONE = 1;
	protected static int RPC_ERROR = 2;			
	
	private LinkedBlockingQueue<Future<R>> pendingDataOps;
	private AtomicInteger status;
	private Exception exception;
	
	public abstract void aggregate(R obj);
	public abstract T getAggregate();	
	
	public MultiFuture(){
		this.pendingDataOps = new LinkedBlockingQueue<Future<R>>();
		this.status = new AtomicInteger(RPC_PENDING);
		this.exception = null;
	}
	
	public synchronized boolean isDone() {
		if (status.get() == RPC_PENDING) {
			try {
				Future<R> dataFuture = pendingDataOps.peek();
				while (dataFuture != null && dataFuture.isDone()) {
					dataFuture = pendingDataOps.poll();
					R result = dataFuture.get();
					this.aggregate(result);
					dataFuture = pendingDataOps.peek();
				}
				if (pendingDataOps.isEmpty() && status.get() == RPC_PENDING) {
					completeOperation();
				}
			} catch (Exception e) {
				status.set(RPC_ERROR);
				this.exception = e;
			}
		}
		
		return status.get() > 0;
	}	
	
	public synchronized T get() throws InterruptedException, ExecutionException {
		if (this.exception != null){
			throw new ExecutionException(exception);
		}		
		
		if (status.get() == RPC_PENDING){
			try {
				for (Future<R> dataFuture = pendingDataOps.poll(); dataFuture != null; dataFuture = pendingDataOps.poll()){
					R result = dataFuture.get();
					this.aggregate(result);
				}
				completeOperation();
			} catch (Exception e) {
				status.set(RPC_ERROR);
				this.exception = e;
			}
		}
		
		if (status.get() == RPC_DONE){
			return this.getAggregate();
		} else if (status.get() == RPC_PENDING){
			throw new InterruptedException("RPC timeout");
		} else if (exception != null) {
			throw new ExecutionException(exception);
		} else {
			throw new InterruptedException("RPC error");
		}
	}

	@Override
	public synchronized T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
		if (this.exception != null){
			throw new ExecutionException(exception);
		}		
		
		if (status.get() == RPC_PENDING){
			try {
				for (Future<R> dataFuture = pendingDataOps.poll(); dataFuture != null; dataFuture = pendingDataOps.poll()){
					R result = dataFuture.get(CrailConstants.DATA_TIMEOUT, TimeUnit.MILLISECONDS);
					this.aggregate(result);
				}
				completeOperation();
			} catch (Exception e) {
				status.set(RPC_ERROR);
				this.exception = e;
			}
		}
		
		if (status.get() == RPC_DONE){
			return this.getAggregate();
		} else if (status.get() == RPC_PENDING){
			throw new InterruptedException("RPC timeout");
		} else if (exception != null) {
			throw new ExecutionException(exception);
		} else {
			throw new InterruptedException("RPC error");
		}
	}

	@Override
	public boolean cancel(boolean mayInterruptIfRunning) {
		return false;
	}

	@Override
	public boolean isCancelled() {
		return false;
	}	
	
	public synchronized void add(Future<R> dataFuture) {
		this.pendingDataOps.add(dataFuture);
	}	
	
	public void completeOperation(){
		if (status.get() != RPC_DONE){
			status.set(RPC_DONE);
		}
	}
}
