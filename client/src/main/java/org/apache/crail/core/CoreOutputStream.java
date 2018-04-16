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

package org.apache.crail.core;

import java.io.IOException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.crail.CrailBuffer;
import org.apache.crail.CrailOutputStream;
import org.apache.crail.CrailResult;
import org.apache.crail.conf.CrailConstants;
import org.apache.crail.metadata.BlockInfo;
import org.apache.crail.storage.StorageEndpoint;
import org.apache.crail.storage.StorageFuture;
import org.apache.crail.utils.CrailImmediateOperation;
import org.apache.crail.utils.CrailUtils;
import org.slf4j.Logger;

public class CoreOutputStream extends CoreStream implements CrailOutputStream {
	private static final Logger LOG = CrailUtils.getLogger();
	private AtomicLong inFlight;
	private long writeHint;
	private CrailImmediateOperation noOp;
	private boolean open;
	
	public CoreOutputStream(CoreNode file, long streamId, long writeHint) throws Exception {
		super(file, streamId, file.getCapacity());
		this.writeHint = Math.max(0, writeHint);
		this.inFlight = new AtomicLong(0);
		this.noOp = new CrailImmediateOperation(0);
		this.open = true;
		if (CrailConstants.DEBUG){
			LOG.info("CoreOutputStream, open, path " + file.getPath() + ", fd " + file.getFd() + ", streamId " + streamId + ", isDir " + file.getType().isDirectory() + ", writeHint " + this.writeHint);
		}
	}
	
	final public Future<CrailResult> write(CrailBuffer dataBuf) throws Exception {
		if (!open) {
			throw new IOException("Stream closed, cannot write");
		}
		if (dataBuf.remaining() <= 0) {
			return noOp;
		}
		
		inFlight.incrementAndGet();
		CoreDataOperation future = dataOperation(dataBuf);
		if (position() < writeHint){
			prefetchMetadata();
		} 	
		if (future.isSynchronous()){
			future.get();
		}		
		return future;
	}	
	
	final public long getWriteHint() {
		return this.writeHint;
	}
	
	public Future<Void> sync() throws IOException {
		if (inFlight.get() != 0){
			LOG.info("Cannot sync, pending operations, opcount " + inFlight.get());
			throw new IOException("Cannot close, pending operations, opcount " + inFlight.get());
		}		
		return super.sync();
	}
	
	public void close() throws Exception {
		if (!open){
			return;
		}
		if (inFlight.get() != 0){
			LOG.info("Cannot close, pending operations, opcount " + inFlight.get() + ", path " + getFile().getPath());
			throw new IOException("Cannot close, pending operations, opcount " + inFlight.get() + ", fd " + getFile().getFd() + ", streamId " + getStreamId() + ", capacity " + getFile().getCapacity());
		}
		
		sync().get();
		updateIOStats();
		node.closeOutputStream(this);
		open = false;
		if (CrailConstants.DEBUG){
			LOG.info("CoreOutputStream, close, path " + this.getFile().getPath() + ", fd " + getFile().getFd() + ", streamId " + getStreamId() + ", capacity " + getFile().getCapacity());
		}	
	}
	
	// ----------------------
	
	StorageFuture trigger(StorageEndpoint endpoint, CoreSubOperation opDesc, CrailBuffer buffer, BlockInfo block) throws Exception {
		StorageFuture dataFuture = endpoint.write(buffer, block, opDesc.getBlockOffset());
		return dataFuture;		
	}	
	
	synchronized void update(long newCapacity) {
		inFlight.decrementAndGet();
		setCapacity(newCapacity);
	}
}
