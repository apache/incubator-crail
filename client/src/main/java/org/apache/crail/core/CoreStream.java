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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.crail.CrailBuffer;
import org.apache.crail.conf.CrailConstants;
import org.apache.crail.metadata.BlockInfo;
import org.apache.crail.metadata.FileInfo;
import org.apache.crail.rpc.RpcConnection;
import org.apache.crail.rpc.RpcErrors;
import org.apache.crail.rpc.RpcFuture;
import org.apache.crail.rpc.RpcGetBlock;
import org.apache.crail.storage.StorageEndpoint;
import org.apache.crail.storage.StorageFuture;
import org.apache.crail.utils.BufferCheckpoint;
import org.apache.crail.utils.CrailUtils;
import org.apache.crail.utils.EndpointCache;
import org.apache.crail.utils.BlockCache.FileBlockCache;
import org.apache.crail.utils.NextBlockCache.FileNextBlockCache;
import org.slf4j.Logger;

public abstract class CoreStream {
	private static final Logger LOG = CrailUtils.getLogger();

	protected CoreDataStore fs;
	protected CoreNode node;

	private EndpointCache endpointCache;
	private RpcConnection namenodeClientRpc;
	private FileBlockCache blockCache;
	private FileNextBlockCache nextBlockCache;
	private BufferCheckpoint bufferCheckpoint;
	private FileInfo fileInfo;
	private long position;
	private long syncedCapacity;
	private long streamId;
	private CoreIOStatistics ioStats;
	private HashMap<Integer, CoreSubOperation> blockMap;
	private LinkedList<RpcFuture<RpcGetBlock>> pendingBlocks;

	abstract StorageFuture trigger(StorageEndpoint endpoint, CoreSubOperation opDesc, CrailBuffer buffer, BlockInfo block) throws Exception;
	abstract void update(long newCapacity);

	CoreStream(CoreNode node, long streamId, long fileOffset) throws Exception {
		this.node = node;
		this.fs = node.getFileSystem();
		this.fileInfo = node.getFileInfo();
		this.endpointCache = fs.getDatanodeEndpointCache();
		this.namenodeClientRpc = fs.getNamenodeClientRpc();
		this.blockCache = fs.getBlockCache(fileInfo.getFd());
		this.nextBlockCache = fs.getNextBlockCache(fileInfo.getFd());
		this.bufferCheckpoint = fs.getBufferCheckpoint();

		this.position = fileOffset;
		this.syncedCapacity = fileInfo.getCapacity();
		this.streamId = streamId;
		this.ioStats = new CoreIOStatistics("core");

		this.blockMap = new HashMap<Integer, CoreSubOperation>();
		this.pendingBlocks = new LinkedList<RpcFuture<RpcGetBlock>>();
	}

	final CoreDataOperation dataOperation(CrailBuffer dataBuf) throws Exception {
		blockMap.clear();
		pendingBlocks.clear();
		CoreDataOperation multiOperation = new CoreDataOperation(this, dataBuf);

		//compute off, len for the fragments, start transfer or start RPC if block info is missing
		while(multiOperation.remaining() > 0){
			long blockRemaining = blockRemaining();
			int opLen = CrailUtils.minFileBuf(blockRemaining, multiOperation.remaining());
			CoreSubOperation subOperation = new CoreSubOperation(fileInfo.getFd(), position, multiOperation.getCurrentBufferPosition(), opLen);
//			LOG.info("OpDesc: " + opDesc.toString());
			ioStats.incTotalOps((long) opLen);

			if (blockCache.containsKey(subOperation.key())){
				BlockInfo block = blockCache.get(subOperation.key());
				StorageFuture subFuture = this.prepareAndTrigger(subOperation, dataBuf, block);
				multiOperation.add(subFuture);
				this.ioStats.incCachedOps();
			} else if (nextBlockCache.containsKey(subOperation.key())){
				RpcFuture<RpcGetBlock> rpcFuture = nextBlockCache.get(subOperation.key());
				blockMap.put(rpcFuture.getTicket(), subOperation);
				pendingBlocks.add(rpcFuture);
			} else {
				this.syncedCapacity = fileInfo.getCapacity();
				RpcFuture<RpcGetBlock> rpcFuture = namenodeClientRpc.getBlock(fileInfo.getFd(), fileInfo.getToken(), position, syncedCapacity);
				blockMap.put(rpcFuture.getTicket(), subOperation);
				pendingBlocks.add(rpcFuture);
			}

			position += opLen;
			multiOperation.incProcessedLen(opLen);
		}

		//wait for RPC results and start reads for those blocks as well
		for (RpcFuture<RpcGetBlock> rpcFuture = pendingBlocks.poll(); rpcFuture != null; rpcFuture = pendingBlocks.poll()){
			if (!rpcFuture.isDone()){
				this.ioStats.incBlockingOps();
				if (rpcFuture.isPrefetched()){
					this.ioStats.incPrefetchedBlockingOps();
				}
			} else {
				this.ioStats.incNonblockingOps();
				if (rpcFuture.isPrefetched()){
					this.ioStats.incPrefetchedNonblockingOps();
				}
			}

			RpcGetBlock getBlockRes = rpcFuture.get(CrailConstants.RPC_TIMEOUT, TimeUnit.MILLISECONDS);
			if (!rpcFuture.isDone()){
				throw new IOException("rpc timeout ");
			}
			if (getBlockRes.getError() != RpcErrors.ERR_OK) {
				LOG.info("inputStream: " + RpcErrors.messages[getBlockRes.getError()]);
				throw new IOException(RpcErrors.messages[getBlockRes.getError()]);
			}
			BlockInfo block = getBlockRes.getBlockInfo();
			CoreSubOperation subOperation = blockMap.get(rpcFuture.getTicket());
			StorageFuture subFuture = prepareAndTrigger(subOperation, dataBuf, block);
			multiOperation.add(subFuture);
			blockCache.put(subOperation.key(), block);
		}

		if (!multiOperation.isProcessed()){
			throw new IOException("Internal error, processed data != operation length");
		}

		dataBuf.limit(multiOperation.getBufferLimit());
		dataBuf.position(multiOperation.getCurrentBufferPosition());
		return multiOperation;
	}

	final void prefetchMetadata() throws Exception {
		long key = CoreSubOperation.createKey(fileInfo.getFd(), position);
		if (blockCache.containsKey(key)){
			return;
		}
		if (nextBlockCache.containsKey(key)){
			return;
		}
		this.syncedCapacity = fileInfo.getCapacity();
		RpcFuture<RpcGetBlock> nextBlock = namenodeClientRpc.getBlock(fileInfo.getFd(), fileInfo.getToken(), position, syncedCapacity);
		nextBlock.setPrefetched(true);
		nextBlockCache.put(key, nextBlock);
		this.ioStats.incPrefetchedOps();
	}

	void seek(long pos) throws IOException {
		long newOffset = Math.min(fileInfo.getCapacity(), Math.max(0, pos));
		if (newOffset == pos){
			this.position = newOffset;
		} else {
			throw new IOException("seek position out of range, pos " + pos + ", fileCapacity " + fileInfo.getCapacity());
		}
	}

	Future<Void> sync() throws IOException {
		Future<Void> future = null;
		if (fileInfo.getToken() > 0 && syncedCapacity < fileInfo.getCapacity()){
			syncedCapacity = fileInfo.getCapacity();
			future = new SyncNodeFuture(namenodeClientRpc.setFile(fileInfo, false));
		} else {
			future = new NoOperation();
		}

		return future;
	}

	void updateIOStats() {
		ioStats.setCapacity(fileInfo.getCapacity());
	}

	long getStreamId() {
		return streamId;
	}

	public long position() {
		return position;
	}

	CoreIOStatistics getCoreStatistics(){
		return ioStats;
	}

	public CoreNode getFile(){
		return node;
	}

	BufferCheckpoint getBufferCheckpoint(){
		return bufferCheckpoint;
	}

	//-----------------

	void setCapacity(long currentCapacity) {
		fileInfo.setCapacity(currentCapacity);
	}

	private long blockRemaining(){
		long blockOffset = position % CrailConstants.BLOCK_SIZE;
		long blockRemaining = CrailConstants.BLOCK_SIZE - blockOffset;
		return blockRemaining;
	}

	private StorageFuture prepareAndTrigger(CoreSubOperation opDesc, CrailBuffer dataBuf, BlockInfo block) throws Exception {
		try {
			StorageEndpoint endpoint = endpointCache.getDataEndpoint(block.getDnInfo());
			dataBuf.clear();
			dataBuf.position(opDesc.getBufferPosition());
			dataBuf.limit(dataBuf.position() + opDesc.getLen());
			StorageFuture subFuture = trigger(endpoint, opDesc, dataBuf, block);
			incStats(endpoint.isLocal());
			return subFuture;
		} catch(IOException e){
			LOG.info("ERROR: failed data operation");
			e.printStackTrace();
			throw e;
		}
	}

	private void incStats(boolean isLocal){
		if (CrailConstants.STATISTICS){
			if (isLocal){
				ioStats.incLocalOps();
				if (fileInfo.getType().isDirectory()){
					ioStats.incLocalDirOps();
				}
			} else {
				ioStats.incRemoteOps();
				if (fileInfo.getType().isDirectory()){
					ioStats.incRemoteDirOps();
				}
			}
		}
	}
}
