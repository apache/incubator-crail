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
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;

import com.ibm.crail.CrailBuffer;
import com.ibm.crail.CrailInputStream;
import com.ibm.crail.CrailResult;
import com.ibm.crail.conf.CrailConstants;
import com.ibm.crail.metadata.BlockInfo;
import com.ibm.crail.storage.StorageEndpoint;
import com.ibm.crail.storage.StorageFuture;
import com.ibm.crail.utils.CrailImmediateOperation;
import com.ibm.crail.utils.CrailUtils;

public class CoreInputStream extends CoreStream implements CrailInputStream { 
	private static final Logger LOG = CrailUtils.getLogger();
	private AtomicLong inFlight;
	private long readHint;
	private CrailImmediateOperation noOp;
	private boolean open;
	
	public CoreInputStream(CoreNode file, long streamId, long readHint) throws Exception {
		super(file, streamId, 0);
		this.inFlight = new AtomicLong(0);
		this.readHint = Math.max(0, Math.min(file.getCapacity(), readHint));
		this.noOp = new CrailImmediateOperation(0);
		this.open = true;
		if (CrailConstants.DEBUG){
			LOG.info("CoreInputStream: open, path  " + file.getPath() + ", fd " + file.getFd() + ", streamId " + streamId + ", isDir " + file.getType().isDirectory() + ", readHint " + this.readHint);
		}
	}
	
	final public Future<CrailResult> read(CrailBuffer dataBuf) throws Exception {
		if (!open) {
			throw new IOException("stream already closed");
		}
		if (dataBuf.remaining() <= 0) {
			return noOp;
		}
		if (position() >= getFile().getCapacity()) {
			return null;
		}
		
		long fileAvailable = available();
		long bufAvailable = (long) dataBuf.remaining();
		if (fileAvailable < bufAvailable){
			int _fileAvailable = (int) fileAvailable;
			dataBuf.limit(dataBuf.position() + _fileAvailable);
		}
		
		inFlight.incrementAndGet();
		CoreDataOperation future = dataOperation(dataBuf);
		if (position() < readHint){
			prefetchMetadata();
		}	
		if (future.isSynchronous()){
			future.get();
		}
		return future;
	}
	
	final public int available() {
		long available = Math.max(0, getFile().getCapacity() - position());
		long maxint = (long) Integer.MAX_VALUE;
		if (available < maxint){
			return (int) available;
		} else {
			return Integer.MAX_VALUE;
		}		
	}	
	
	final public void seek(long pos) throws IOException {
		long oldPos = position();
		super.seek(pos);
		long newPos = position();
		if (oldPos != newPos){
			this.readHint = 0;
		}
	}
	
	public void close() throws Exception {
		if (!open){
			return;
		}
		if (inFlight.get() != 0){
			LOG.info("Cannot close, pending operations, opcount " + inFlight.get() + ", path " + getFile().getPath());
			throw new IOException("Cannot close, pending operations, opcount " + inFlight.get());
		}
		
		updateIOStats();
		node.closeInputStream(this);
		open = false;
		if (CrailConstants.DEBUG){
			LOG.info("CoreInputStream, close, path " + this.getFile().getPath() + ", fd " + getFile().getFd() + ", streamId " + getStreamId());
		}	
	}
	
	// --------------------------
	
	StorageFuture trigger(StorageEndpoint endpoint, CoreSubOperation opDesc, CrailBuffer buffer, BlockInfo block) throws Exception {
		StorageFuture future = endpoint.read(buffer, block, opDesc.getBlockOffset());
		return future;
	}	
	
	void update(long newCapacity) {
		inFlight.decrementAndGet();
	}
}
