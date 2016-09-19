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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import com.ibm.crail.CrailResult;
import com.ibm.crail.conf.CrailConstants;
import com.ibm.crail.datanode.DataResult;
import com.ibm.crail.utils.BufferCheckpoint;

public class CoreDataOperation implements Future<CrailResult>, CrailResult {
	protected static int RPC_PENDING = 0;
	protected static int RPC_DONE = 1;
	protected static int RPC_ERROR = 2;		
	
	private CoreStream stream;
	private ByteBuffer buffer;
	private LinkedBlockingQueue<Future<DataResult>> pendingDataOps;
	private long fileOffset;
	private int bufferPosition;
	private int bufferLimit;
	private long len;
	private AtomicInteger status;
	private BufferCheckpoint bufferCheckpoint;
	private Exception exception;
	
	public static CoreDataOperation newOp(CoreStream stream, BufferCheckpoint bufferCheckpoint, ByteBuffer buffer) throws Exception {
		return new CoreDataOperation(stream, bufferCheckpoint, buffer);
	}
	
	public static CoreDataOperation noOp(CoreStream stream) throws IOException {
		return new CoreDataOperation(stream);
	}	
	
	private CoreDataOperation(CoreStream stream){
		this.stream = stream;
		this.fileOffset = stream.position();
		this.buffer = null;
		this.bufferPosition = 0;
		this.bufferLimit = 0;
		this.bufferCheckpoint = null;
		
		this.pendingDataOps = null;
		this.len = 0;
		this.status = new AtomicInteger(RPC_DONE);
		this.exception = null;
	}
	
	private CoreDataOperation(CoreStream stream, BufferCheckpoint bufferCheckpoint, ByteBuffer buffer) throws Exception{
		this.stream = stream;
		this.fileOffset = stream.position();
		this.buffer = buffer;
		this.bufferPosition = buffer.position();
		this.bufferLimit = buffer.limit();
		this.bufferCheckpoint = bufferCheckpoint;
		
		this.pendingDataOps = new LinkedBlockingQueue<Future<DataResult>>();
		this.len = 0;
		this.status = new AtomicInteger(RPC_PENDING);
		
		if (CrailConstants.DEBUG){
			this.bufferCheckpoint.checkIn(buffer);
		}
	}
	
	public synchronized boolean isDone() {
		if (status.get() == RPC_PENDING) {
			try {
				Future<DataResult> dataFuture = pendingDataOps.peek();
				while (dataFuture != null && dataFuture.isDone()) {
					dataFuture = pendingDataOps.poll();
					DataResult result = dataFuture.get();
					len += result.getLen();
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
					len += result.getLen();
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
					len += result.getLen();
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
	
	synchronized void add(Future<DataResult> dataFuture) {
		this.pendingDataOps.add(dataFuture);
	}		

	public long getLen() {
		return len;
	}

	public long getFileOffset() {
		return fileOffset;
	}

	public int position() {
		return bufferPosition;
	}
	
	public int position(int newPosition) {
		bufferPosition = newPosition;
		return bufferPosition;
	}	
	
	public int limit() {
		return bufferLimit;
	}	
	
	public int remaining(){
		return bufferLimit - bufferPosition;
	}

	@Override
	public boolean cancel(boolean mayInterruptIfRunning) {
		return false;
	}

	@Override
	public boolean isCancelled() {
		return false;
	}
	
	//-----------
	
	private void completeOperation(){
		if (status.get() != RPC_DONE){
			status.set(RPC_DONE);
			stream.update(fileOffset + len);
			if (CrailConstants.DEBUG){
				bufferCheckpoint.checkOut(buffer);
			}
		}
	}
}
