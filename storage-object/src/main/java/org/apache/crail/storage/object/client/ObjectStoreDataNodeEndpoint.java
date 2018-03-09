/*
 * Copyright (C) 2015-2018, IBM Corporation
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

package org.apache.crail.storage.object.client;

import org.apache.crail.CrailBuffer;
import org.apache.crail.conf.CrailConstants;
import org.apache.crail.metadata.BlockInfo;
import org.apache.crail.storage.StorageEndpoint;
import org.apache.crail.storage.StorageFuture;
import org.apache.crail.storage.object.ObjectStoreConstants;
import org.apache.crail.storage.object.ObjectStoreUtils;
import org.apache.crail.storage.object.rpc.MappingEntry;
import org.apache.crail.storage.object.rpc.ObjectStoreRPC;
import org.apache.crail.storage.object.rpc.RPCCall;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

public class ObjectStoreDataNodeEndpoint implements StorageEndpoint {
	private static final Logger LOG = ObjectStoreUtils.getLogger();

	private final static AtomicInteger objectSequenceNumber = new AtomicInteger(0);
	private final ObjectStoreMetadataClient metadataClient;
	private final S3ObjectStoreClient objectStoreClient;
	private final String localObjectKeyPrefix;
	private final byte[] stagingBuffer = new byte[64 * 1024 * 1024];
	private final int blockSize = Long.valueOf(CrailConstants.BLOCK_SIZE).intValue();

	public ObjectStoreDataNodeEndpoint(ObjectStoreMetadataClient metadataClient) throws IOException {
		LOG.debug("TID {} : Creating a new ObjectStore client endpoint", Thread.currentThread().getId());
		this.metadataClient = metadataClient;
		this.objectStoreClient = new S3ObjectStoreClient();
		Random rand = new Random();
		localObjectKeyPrefix = ObjectStoreConstants.OBJECT_PREFIX + "-"
				+ InetAddress.getLocalHost().getHostName() + "-"
				+ Integer.toString(Math.abs(rand.nextInt())) + "-";
	}

	public StorageFuture write(CrailBuffer buffer, BlockInfo blockInfo, long remoteOffset)
			throws IOException {
		long startTime = 0, endTime;
		if (ObjectStoreConstants.PROFILE) {
			startTime = System.nanoTime();
		}
		String key = makeUniqueKey(blockInfo);
		int length = buffer.remaining();
		LOG.debug("Block write: addr = {}, start offset = {}, end offset = {}, key = {}",
				blockInfo.getAddr(), remoteOffset, (remoteOffset + length), key);
		objectStoreClient.putObject(key, buffer);
		if (remoteOffset == 0 && length == ObjectStoreConstants.ALLOCATION_SIZE) {
			metadataClient.writeBlock(blockInfo, key);
		} else {
			metadataClient.writeBlockRange(blockInfo, remoteOffset, length, key);
		}
		if (ObjectStoreConstants.PROFILE) {
			endTime = System.nanoTime();
			LOG.info("Wrote {} bytes in {} (us)\n", length, (endTime - startTime) / 1000.);
		}
		return new ObjectStoreDataFuture(length);
	}

	private String makeUniqueKey(BlockInfo blockInfo) {
		return localObjectKeyPrefix + Long.toString(blockInfo.getAddr() / CrailConstants.BLOCK_SIZE)
				+ "-" + objectSequenceNumber.getAndIncrement();
	}

	public StorageFuture read(CrailBuffer buffer, BlockInfo blockInfo, long remoteOffset)
			throws IOException {
		long startTime = 0, endTime;
		if (ObjectStoreConstants.PROFILE) {
			startTime = System.nanoTime();
		}
		int startPos = buffer.position();
		final long startOffset = remoteOffset;
		long curOffset = startOffset;
		final long endOffset = startOffset + buffer.limit();
		LOG.debug("Block read request: Block address = {}, range ({} - {})", blockInfo.getAddr(), startOffset,
				endOffset);
		ObjectStoreRPC.TranslateBlock rpc;
		Future<RPCCall> future = metadataClient.translateBlock(blockInfo);
		try {
			rpc = (ObjectStoreRPC.TranslateBlock) future.get();
		} catch (Exception e) {
			LOG.error("RPC exception: " + e);
			throw new IOException("Got exception while performing RPC call ", e);
		}
		List<MappingEntry> mapping = rpc.getResponse();
		if (mapping != null) {
			// A read of a non-written block is theoretically possible
			for (MappingEntry entry : mapping) {
				if (startOffset < entry.getEndOffset() && endOffset > entry.getStartOffset()) {
					// the current mapping entry overlaps with what we are interested in
					String key = entry.getKey();
					long objStartOffset = entry.getObjectOffset();
					long curLength = entry.getSize();
					if (curOffset > entry.getStartOffset()) {
						// unaligned object read, we need to skip the beginning of the object
						long shift = curOffset - entry.getStartOffset();
						objStartOffset += shift;
						curLength -= shift;
					} else if (curOffset < entry.getStartOffset()) {
						// we have a read hole (the range was never written)
						LOG.warn("Reading non-initialized block range ({} - {})",
								curOffset, entry.getStartOffset());
						ObjectStoreUtils.putZeroes(buffer, (int) (entry.getStartOffset() - curOffset));
						curOffset = entry.getStartOffset();
					}
					if (entry.getEndOffset() >= endOffset) {
						// do not read end of block
						curLength -= (entry.getEndOffset() - endOffset);
					}
					assert curLength > 0;
					LOG.debug("Block range ({} - {}) maps to object key {} range ({} - {})",
							curOffset, curOffset + curLength, key, objStartOffset, objStartOffset + curLength);
					InputStream input =
							this.objectStoreClient.getObject(key, objStartOffset, objStartOffset + curLength);
					long st = 0, et;
					if (ObjectStoreConstants.PROFILE) {
						st = System.nanoTime();
					}
					//if (buffer.()) {
					//	ObjectStoreUtils.readStreamIntoHeapByteBuffer(input, buffer);
					//} else {
					ObjectStoreUtils.readStreamIntoDirectByteBuffer(input, stagingBuffer, buffer);
					//}
					if (ObjectStoreConstants.PROFILE) {
						et = System.nanoTime();
						LOG.debug("{} (us) for reading object into ByteBuffer", (et - st) / 1000.);
					}
					curOffset += curLength;
				}
			}
		}
		int endPos = buffer.position();
		int readLength = endPos - startPos;
		if (endPos < buffer.limit()) {
			/* NOTE: Not clear what to do if the buffer is not filled up to limit().
			 * Padding with zeroes might not be the correct behavior.
			 */
			LOG.warn("Trying to read non-initialized block range ({} - {})", endPos, buffer.limit());
		}
		if (ObjectStoreConstants.PROFILE) {
			endTime = System.nanoTime();
			LOG.info("Read {} bytes in {} (us)\n", readLength, (endTime - startTime) / 1000.);
		}
		return new ObjectStoreDataFuture(readLength);
	}

	public void close() throws IOException, InterruptedException {
		if (this.metadataClient != null) {
			this.metadataClient.close();
		}
		if (this.objectStoreClient != null) {
			this.objectStoreClient.close();
		}
	}

	public boolean isLocal() {
		return false;
	}

	protected void finalize() {
		LOG.info("Closing ObjectStore DataNode Endpoint");
		try {
			close();
		} catch (Exception e) {
			LOG.error("Could not close ObjectStoreEndpoint. Reason: {}", e);
		}
	}
}
