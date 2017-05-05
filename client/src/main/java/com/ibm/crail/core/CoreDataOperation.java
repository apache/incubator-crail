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

import com.ibm.crail.CrailBuffer;
import com.ibm.crail.CrailResult;
import com.ibm.crail.conf.CrailConstants;
import com.ibm.crail.storage.StorageFuture;
import com.ibm.crail.storage.StorageResult;
import com.ibm.crail.utils.BufferCheckpoint;
import com.ibm.crail.utils.MultiFuture;

class CoreDataOperation extends MultiFuture<StorageResult, CrailResult> implements CrailResult {
	private CoreStream stream;
	private CrailBuffer buffer;
	private long fileOffset;
	private int bufferPosition;
	private int bufferLimit;
	private int operationLength;
	
	//current state
	private BufferCheckpoint bufferCheckpoint;
	private int inProcessLen;
	private long completedLen;
	private boolean isSynchronous;
	
	public CoreDataOperation(CoreStream stream, CrailBuffer buffer) throws Exception{
		this.stream = stream;
		this.buffer = buffer;
		this.fileOffset = stream.position();
		this.bufferPosition = buffer.position();
		this.bufferLimit = buffer.limit();
		this.operationLength = buffer.remaining();
		this.inProcessLen = 0;
		this.completedLen = 0;
		this.isSynchronous = false;
		
		if (operationLength > 0){
			this.bufferCheckpoint = stream.getBufferCheckpoint();
			if (CrailConstants.DEBUG){
				this.bufferCheckpoint.checkIn(buffer);
			}		
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
	
	public synchronized void add(StorageFuture dataFuture) {
		super.add(dataFuture);
		if (dataFuture.isSynchronous()){
			this.isSynchronous = true;
		}
	}	
	
	//-----------
	
	public void completeOperation(){
		super.completeOperation();
		if (this.isDone()){
			stream.update(fileOffset + completedLen);
			if (CrailConstants.DEBUG){
				bufferCheckpoint.checkOut(buffer);
			}
		}
	}

	boolean isSynchronous() {
		return isSynchronous;
	}

	@Override
	public void aggregate(StorageResult result) {
		completedLen += result.getLen();
	}

	@Override
	public CrailResult getAggregate() {
		return this;
	}
}
