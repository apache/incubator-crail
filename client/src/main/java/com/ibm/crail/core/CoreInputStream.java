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
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;

import sun.nio.ch.DirectBuffer;

import com.ibm.crail.CrailInputStream;
import com.ibm.crail.CrailResult;
import com.ibm.crail.conf.CrailConstants;
import com.ibm.crail.datanode.DataNodeEndpoint;
import com.ibm.crail.datanode.DataResult;
import com.ibm.crail.namenode.protocol.BlockInfo;
import com.ibm.crail.utils.CrailUtils;

public class CoreInputStream extends CoreStream implements CrailInputStream { 
	private static final Logger LOG = CrailUtils.getLogger();
	private AtomicLong inFlight;
	private long readHint;
	
	public CoreInputStream(CoreFile file, long streamId, long readHint) throws Exception {
		super(file, streamId, 0);
		this.inFlight = new AtomicLong(0);
		this.readHint = Math.max(0, Math.min(file.getCapacity(), readHint));
		if (CrailConstants.DEBUG){
			LOG.info("CoreInputStream: open, path  " + file.getPath() + ", fd " + file.getFd() + ", streamId " + streamId + ", isDir " + file.isDir() + ", readHint " + this.readHint);
		}
	}
	
	public int read(ByteBuffer dataBuf) throws Exception {
		return (int) readAsync(dataBuf).get().getLen();
	}
	
	final public Future<CrailResult> readAsync(ByteBuffer dataBuf) throws Exception {
		if (!isOpen()) {
			throw new IOException("stream already closed");
		}
		if (!(dataBuf instanceof DirectBuffer)) {
			throw new IOException("buffer not offheap");
		}		
		if (position() >= getFile().getCapacity()) {
			return null;
		}
		if (dataBuf.remaining() <= 0) {
			return CoreDataOperation.noOp(this);
		}
		
		long fileAvailable = available();
		long bufAvailable = (long) dataBuf.remaining();
		if (fileAvailable < bufAvailable){
			int _fileAvailable = (int) fileAvailable;
			dataBuf.limit(dataBuf.position() + _fileAvailable);
		}
		
		inFlight.incrementAndGet();
		Future<CrailResult> future = dataOperation(dataBuf);
		long nextOffset = CrailUtils.nextBlockAddress(position());
		if (nextOffset < this.getReadHint()){
			prefetchMetadata();
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
	
	final public long getReadHint() {
		return this.readHint;
	}		
	
	public synchronized void close() throws IOException {
		if (!isOpen()){
			return;
		}			
		
		if (inFlight.get() != 0){
			LOG.info("Cannot close, pending operations, opcount " + inFlight.get());
			throw new IOException("Cannot close, pending operations, opcount " + inFlight.get());
		}
		super.close();
		if (CrailConstants.DEBUG){
			LOG.info("CoreInputStream, close, path " + this.getFile().getPath() + ", fd " + getFile().getFd() + ", streamId " + getStreamId());
		}	
	}
	
	// --------------------------
	
	public Future<DataResult> trigger(DataNodeEndpoint endpoint, CoreSubOperation opDesc, ByteBuffer buffer, ByteBuffer region, BlockInfo block) throws Exception {
		Future<DataResult> future = endpoint.read(buffer, region, block, opDesc.getBlockOffset());
		return future;
	}	
	
	public void update(long newCapacity) {
		inFlight.decrementAndGet();
	}
	
	public long getInFlight(){
		return inFlight.get();
	}	
}
