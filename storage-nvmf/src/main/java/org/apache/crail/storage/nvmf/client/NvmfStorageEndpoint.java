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

package org.apache.crail.storage.nvmf.client;

import com.ibm.jnvmf.*;
import org.apache.crail.CrailBuffer;
import org.apache.crail.CrailBufferCache;
import org.apache.crail.CrailStatistics;
import org.apache.crail.conf.CrailConstants;
import org.apache.crail.metadata.BlockInfo;
import org.apache.crail.metadata.DataNodeInfo;
import org.apache.crail.storage.StorageEndpoint;
import org.apache.crail.storage.StorageFuture;
import org.apache.crail.storage.nvmf.NvmfStorageConstants;
import org.apache.crail.utils.CrailUtils;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

public class NvmfStorageEndpoint implements StorageEndpoint {
	private static final Logger LOG = CrailUtils.getLogger();

	private final Controller controller;
	private final IoQueuePair queuePair;
	private final int lbaDataSize;
	private final long namespaceCapacity;
	private final NvmfRegisteredBufferCache registeredBufferCache;
	private final NvmfStagingBufferCache stagingBufferCache;
	private final CrailStatistics statistics;

	private final Queue<NvmWriteCommand> writeCommands;
	private final Queue<NvmReadCommand> readCommands;

	private final AtomicInteger outstandingOperations;

	public NvmfStorageEndpoint(Nvme nvme, DataNodeInfo info, CrailStatistics statistics,
							   CrailBufferCache bufferCache) throws IOException {
		InetSocketAddress inetSocketAddress = new InetSocketAddress(
				InetAddress.getByAddress(info.getIpAddress()), info.getPort());
		// XXX FIXME: nsid from datanodeinfo
		NvmfTransportId transportId = new NvmfTransportId(inetSocketAddress,
				new NvmeQualifiedName(NvmfStorageConstants.NQN.toString()));
		LOG.info("Connecting to NVMf target at " + transportId.toString());
		controller = nvme.connect(transportId);
		controller.getControllerConfiguration().setEnable(true);
		controller.syncConfiguration();
		try {
			controller.waitUntilReady();
		} catch (TimeoutException e) {
			throw new IOException(e);
		}
		IdentifyControllerData identifyControllerData = controller.getIdentifyControllerData();
		if (CrailConstants.SLICE_SIZE > identifyControllerData.getMaximumDataTransferSize().toInt()) {
			throw new IllegalArgumentException(CrailConstants.SLICE_SIZE_KEY + " > max transfer size (" +
					identifyControllerData.getMaximumDataTransferSize() + ")");
		}
		List<Namespace> namespaces = controller.getActiveNamespaces();
		//TODO: poll nsid in datanodeinfo
		NamespaceIdentifier namespaceIdentifier = new NamespaceIdentifier(1);
		Namespace namespace = null;
		for (Namespace n : namespaces) {
			if (n.getIdentifier().equals(namespaceIdentifier)) {
				namespace = n;
				break;
			}
		}
		if (namespace == null) {
			throw new IllegalArgumentException("No namespace with id " + namespaceIdentifier +
					" at controller " + transportId.toString());
		}
		IdentifyNamespaceData identifyNamespaceData = namespace.getIdentifyNamespaceData();
		lbaDataSize = identifyNamespaceData.getFormattedLbaSize().getLbaDataSize().toInt();
		if (CrailConstants.SLICE_SIZE % lbaDataSize != 0) {
			throw new IllegalArgumentException(CrailConstants.SLICE_SIZE_KEY +
					" is not a multiple of LBA data size (" + lbaDataSize + ")");
		}
		namespaceCapacity = identifyNamespaceData.getNamespaceCapacity() * lbaDataSize;
		this.queuePair = controller.createIoQueuePair(NvmfStorageConstants.QUEUE_SIZE, 0, 0,
				SubmissionQueueEntry.SIZE);

		this.writeCommands = new ArrayBlockingQueue<>(NvmfStorageConstants.QUEUE_SIZE);
		this.readCommands = new ArrayBlockingQueue<>(NvmfStorageConstants.QUEUE_SIZE);
		for(int i = 0; i < NvmfStorageConstants.QUEUE_SIZE; i++) {
			NvmWriteCommand writeCommand = new NvmWriteCommand(queuePair);
			writeCommand.setSendInline(true);
			writeCommand.getCommandCapsule().getSubmissionQueueEntry().setNamespaceIdentifier(namespaceIdentifier);
			writeCommands.add(writeCommand);
			NvmReadCommand readCommand = new NvmReadCommand(queuePair);
			readCommand.setSendInline(true);
			readCommand.getCommandCapsule().getSubmissionQueueEntry().setNamespaceIdentifier(namespaceIdentifier);
			readCommands.add(readCommand);
		}
		this.registeredBufferCache = new NvmfRegisteredBufferCache(queuePair);
		this.outstandingOperations = new AtomicInteger(0);
		this.stagingBufferCache = new NvmfStagingBufferCache(bufferCache,
				NvmfStorageConstants.STAGING_CACHE_SIZE, getLBADataSize());
		this.statistics = statistics;
	}

	public void keepAlive() throws IOException {
		controller.keepAlive();
	}

	public int getLBADataSize() {
		return lbaDataSize;
	}

	public long getNamespaceCapacity() {
		return namespaceCapacity;
	}

	enum Operation {
		WRITE,
		READ
	}

	void putOperation() {
		outstandingOperations.decrementAndGet();
	}

	private boolean tryGetOperation() {
		int outstandingOperationsOld = outstandingOperations.get();
		if (outstandingOperationsOld < NvmfStorageConstants.QUEUE_SIZE) {
			return outstandingOperations.compareAndSet(outstandingOperationsOld, outstandingOperationsOld + 1);
		}
		return false;
	}

	private static int divCeil(int a, int b) {
		return (a + b - 1) / b;
	}

	private int getNumLogicalBlocks(CrailBuffer buffer) {
		return divCeil(buffer.remaining(), getLBADataSize());
	}

	StorageFuture Op(Operation op, CrailBuffer buffer, BlockInfo blockInfo, long remoteOffset) throws InterruptedException, IOException {
		assert blockInfo.getAddr() + remoteOffset + buffer.remaining() <= getNamespaceCapacity();
		assert remoteOffset >= 0;
		assert buffer.remaining() <= CrailConstants.BLOCK_SIZE;

		long startingAddress = blockInfo.getAddr() + remoteOffset;
		if (startingAddress % getLBADataSize() != 0 ||
				((startingAddress + buffer.remaining()) % getLBADataSize() != 0 && op == Operation.WRITE)) {
			if (op == Operation.READ) {
				throw new IOException("Unaligned read access is not supported. Address (" + startingAddress +
						") needs to be multiple of LBA data size " + getLBADataSize());
			}
			try {
				return new NvmfUnalignedWriteFuture(this, buffer, blockInfo, remoteOffset);
			} catch (Exception e) {
				throw new IOException(e);
			}
		}

		if (!tryGetOperation()) {
			do {
				poll();
			} while (!tryGetOperation());
		}

		NvmIoCommand<? extends NvmIoCommandCapsule> command;
		NvmfFuture<?> future;
		Response<NvmResponseCapsule> response;
		if (op == Operation.READ) {
			NvmReadCommand readCommand = readCommands.remove();
			response = readCommand.newResponse();
			future = new NvmfFuture<>(this, readCommand, response, readCommands, buffer.remaining());
			command = readCommand;
		} else {
			NvmWriteCommand writeCommand = writeCommands.remove();
			response = writeCommand.newResponse();
			future = new NvmfFuture<>(this, writeCommand, response, writeCommands, buffer.remaining());
			command = writeCommand;
		}
		command.setCallback(future);
		response.setCallback(future);

		NvmIoCommandSqe sqe = command.getCommandCapsule().getSubmissionQueueEntry();
		long startingLBA = startingAddress / getLBADataSize();
		sqe.setStartingLba(startingLBA);
		/* TODO: on read this potentially overwrites data beyond the set limit */
		int numLogicalBlocks = getNumLogicalBlocks(buffer);
		buffer.limit(buffer.position() + numLogicalBlocks * getLBADataSize());
		sqe.setNumberOfLogicalBlocks(numLogicalBlocks);
		int remoteKey = registeredBufferCache.getRemoteKey(buffer);
		KeyedSglDataBlockDescriptor dataBlockDescriptor = sqe.getKeyedSglDataBlockDescriptor();
		dataBlockDescriptor.setAddress(buffer.address() + buffer.position());
		dataBlockDescriptor.setLength(buffer.remaining());
		dataBlockDescriptor.setKey(remoteKey);

		command.execute(response);

		return future;
	}

	public StorageFuture write(CrailBuffer buffer, BlockInfo blockInfo, long remoteOffset) throws InterruptedException, IOException {
		return Op(Operation.WRITE, buffer, blockInfo, remoteOffset);
	}

	public StorageFuture read(CrailBuffer buffer, BlockInfo blockInfo, long remoteOffset) throws InterruptedException, IOException {
		return Op(Operation.READ, buffer, blockInfo, remoteOffset);
	}

	void poll() throws IOException {
		queuePair.poll();
	}

	public void close() throws IOException, InterruptedException {
		registeredBufferCache.free();
		controller.free();

	}

	public boolean isLocal() {
		return false;
	}

	NvmfStagingBufferCache getStagingBufferCache() {
		return stagingBufferCache;
	}

}
