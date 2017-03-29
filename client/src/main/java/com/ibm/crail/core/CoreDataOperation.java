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

package com.ibm.crail.core;

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import com.ibm.crail.CrailResult;
import com.ibm.crail.conf.CrailConstants;
import com.ibm.crail.storage.DataResult;
import com.ibm.crail.utils.BufferCheckpoint;

class CoreDataOperation implements Future<CrailResult>, CrailResult {
	protected static int RPC_PENDING = 0;
	protected static int RPC_DONE = 1;
	protected static int RPC_ERROR = 2;		
	
	//init state
	private CoreStream stream;
	private ByteBuffer buffer;
	private long fileOffset;
	private int bufferPosition;
	private int bufferLimit;
	private int operationLength;
	
	//current state
	private BufferCheckpoint bufferCheckpoint;
	private LinkedBlockingQueue<Future<DataResult>> pendingDataOps;
	private int inProcessLen;
	private long completedLen;
	private AtomicInteger status;
	private Exception exception;
	
	public CoreDataOperation(CoreStream stream, ByteBuffer buffer) throws Exception{
		this.stream = stream;
		this.buffer = buffer;
		this.fileOffset = stream.position();
		this.bufferPosition = buffer.position();
		this.bufferLimit = buffer.limit();
		this.operationLength = buffer.remaining();
		this.inProcessLen = 0;
		this.completedLen = 0;
		
		if (operationLength > 0){
			this.pendingDataOps = new LinkedBlockingQueue<Future<DataResult>>();
			this.exception = null;
			this.status = new AtomicInteger(RPC_PENDING);
			this.bufferCheckpoint = stream.getBufferCheckpoint();
			if (CrailConstants.DEBUG){
				this.bufferCheckpoint.checkIn(buffer);
			}		
		} else {
			this.status = new AtomicInteger(RPC_DONE);			
		}
		
	}
	
	public synchronized boolean isDone() {
		if (status.get() == RPC_PENDING) {
			try {
				Future<DataResult> dataFuture = pendingDataOps.peek();
				while (dataFuture != null && dataFuture.isDone()) {
					dataFuture = pendingDataOps.poll();
					DataResult result = dataFuture.get();
					completedLen += result.getLen();
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
	
	public synchronized CrailResult get() throws InterruptedException, ExecutionException {
		if (this.exception != null){
			throw new ExecutionException(exception);
		}		
		
		if (status.get() == RPC_PENDING){
			try {
				for (Future<DataResult> dataFuture = pendingDataOps.poll(); dataFuture != null; dataFuture = pendingDataOps.poll()){
					DataResult result = dataFuture.get();
					completedLen += result.getLen();
				}
				completeOperation();
			} catch (Exception e) {
				status.set(RPC_ERROR);
				this.exception = e;
			}
		}
		
		if (status.get() == RPC_DONE){
			return this;
		} else if (status.get() == RPC_PENDING){
			throw new InterruptedException("RPC timeout");
		} else if (exception != null) {
			throw new ExecutionException(exception);
		} else {
			throw new InterruptedException("RPC error");
		}
	}

	@Override
	public synchronized CrailResult get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
		if (this.exception != null){
			throw new ExecutionException(exception);
		}		
		
		if (status.get() == RPC_PENDING){
			try {
				for (Future<DataResult> dataFuture = pendingDataOps.poll(); dataFuture != null; dataFuture = pendingDataOps.poll()){
					DataResult result = dataFuture.get(CrailConstants.DATA_TIMEOUT, TimeUnit.MILLISECONDS);
					completedLen += result.getLen();
				}
				completeOperation();
			} catch (Exception e) {
				status.set(RPC_ERROR);
				this.exception = e;
			}
		}
		
		if (status.get() == RPC_DONE){
			return this;
		} else if (status.get() == RPC_PENDING){
			throw new InterruptedException("RPC timeout");
		} else if (exception != null) {
			throw new ExecutionException(exception);
		} else {
			throw new InterruptedException("RPC error");
		}
	}
	
	public long getLen() {
		return completedLen;
	}
	
	void incProcessedLen(int opLen){
		this.inProcessLen += opLen;
	}
	
	long getInProcessLen(){
		return this.inProcessLen;
	}
	
	int getBufferLimit(){
		return bufferLimit;
	}
	
	int remaining() {
		return operationLength - inProcessLen;
	}	
	
	int getCurrentBufferPosition(){
		return bufferPosition + inProcessLen;
	}
	
	long getCurrentStreamPosition(){
		return fileOffset + inProcessLen;
	}
	
	boolean isProcessed() {
		return inProcessLen == operationLength;
	}
	
	public boolean cancel(boolean mayInterruptIfRunning) {
		return false;
	}

	public boolean isCancelled() {
		return false;
	}
	
	synchronized void add(Future<DataResult> dataFuture) {
		this.pendingDataOps.add(dataFuture);
	}	
	
	//-----------
	
	private void completeOperation(){
		if (status.get() != RPC_DONE){
			status.set(RPC_DONE);
			stream.update(fileOffset + completedLen);
			if (CrailConstants.DEBUG){
				bufferCheckpoint.checkOut(buffer);
			}
		}
	}
}
