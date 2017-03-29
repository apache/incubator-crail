/*
 * Crail: A Multi-tiered Distributed Direct Access File System
 *
 * Author:
 * Jonas Pfefferle <jpf@zurich.ibm.com>
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

package com.ibm.crail.datanode.nvmf.client;

import com.ibm.crail.conf.CrailConstants;
import com.ibm.crail.storage.StorageEndpoint;
import com.ibm.crail.storage.DataResult;
import com.ibm.crail.datanode.nvmf.NvmfStorageTier;
import com.ibm.crail.datanode.nvmf.NvmfStorageConstants;
import com.ibm.crail.namenode.protocol.BlockInfo;
import com.ibm.crail.utils.CrailUtils;
import com.ibm.crail.utils.DirectBufferCache;
import com.ibm.disni.nvmef.NvmeCommand;
import com.ibm.disni.nvmef.NvmeEndpoint;
import com.ibm.disni.nvmef.NvmeEndpointGroup;
import com.ibm.disni.nvmef.spdk.IOCompletion;

import org.slf4j.Logger;

import com.ibm.crail.storage.StorageEndpoint;
import com.ibm.crail.storage.DataResult;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.concurrent.*;

public class NvmfStorageEndpoint implements StorageEndpoint { 
	private static final Logger LOG = CrailUtils.getLogger();

	private final InetSocketAddress inetSocketAddress;
	private final NvmeEndpoint endpoint;
	private final int sectorSize;
	private final DirectBufferCache cache;
	private final BlockingQueue<NvmeCommand> freeCommands;
	private final NvmeCommand[] commands;
	private final NvmfStorageFuture[] futures;
	private final ThreadLocal<long[]> completed;
	private final int ioQeueueSize;

	public NvmfStorageEndpoint(NvmeEndpointGroup group, InetSocketAddress inetSocketAddress) throws IOException {
		this.inetSocketAddress = inetSocketAddress;
		endpoint = group.createEndpoint();
		try {
			URI url = new URI("nvmef://" + inetSocketAddress.getHostString() + ":" + inetSocketAddress.getPort() +
					"/0/" + NvmfStorageConstants.NAMESPACE + "?subsystem=nqn.2016-06.io.spdk:cnode1");
			LOG.info("Connecting to " + url.toString());
			endpoint.connect(url);
		} catch (URISyntaxException e) {
			//FIXME
			e.printStackTrace();
		}
		sectorSize = endpoint.getSectorSize();
		cache = new DirectBufferCache();
		ioQeueueSize = endpoint.getIOQueueSize();
		freeCommands = new ArrayBlockingQueue<NvmeCommand>(ioQeueueSize);
		commands = new NvmeCommand[ioQeueueSize];
		for (int i = 0; i < ioQeueueSize; i++) {
			NvmeCommand command = endpoint.newCommand();
			command.setId(i);
			commands[i] = command;
			freeCommands.add(command);
		}
		futures = new NvmfStorageFuture[ioQeueueSize];
		completed = new ThreadLocal<long[]>() {
			public long[] initialValue() {
				return new long[ioQeueueSize];
			}
		};
	}

	public int getSectorSize() {
		return sectorSize;
	}

	enum Operation {
		WRITE,
		READ;
	}

	public Future<DataResult> Op(Operation op, ByteBuffer buffer, BlockInfo remoteMr, long remoteOffset)
			throws IOException, InterruptedException {
		int length = buffer.remaining();
		if (length > CrailConstants.BLOCK_SIZE){
			throw new IOException("write size too large " + length);
		}
		if (length <= 0){
			throw new IOException("write size too small, len " + length);
		}
		if (buffer.position() < 0){
			throw new IOException("local offset too small " + buffer.position());
		}
		if (remoteOffset < 0){
			throw new IOException("remote offset too small " + remoteOffset);
		}

		if (remoteMr.getAddr() + remoteOffset + length > endpoint.getNamespaceSize()){
			long tmpAddr = remoteMr.getAddr() + remoteOffset + length;
			throw new IOException("remote fileOffset + remoteOffset + len = " + tmpAddr + " - size = " +
					endpoint.getNamespaceSize());
		}

//		LOG.info("op = " + op.name() +
//				", position = " + buffer.position() +
//				", localOffset = " + buffer.position() +
//				", remoteOffset = " + remoteOffset +
//				", remoteAddr = " + remoteMr.getAddr() +
//				", length = " + length);

		NvmeCommand command = freeCommands.poll();
		while (command == null) {
			poll();
			command = freeCommands.poll();
		}

		boolean aligned = NvmfStorageUtils.namespaceSectorOffset(sectorSize, remoteOffset) == 0
				&& NvmfStorageUtils.namespaceSectorOffset(sectorSize, length) == 0;
		long lba = NvmfStorageUtils.linearBlockAddress(remoteMr, remoteOffset, sectorSize);
		Future<DataResult> future = null;
		if (aligned) {
//			LOG.debug("aligned");
			command.setBuffer(buffer).setLinearBlockAddress(lba);
			switch(op) {
				case READ:
					command.read();
					break;
				case WRITE:
					command.write();
					break;
			}
			future = futures[(int)command.getId()] = new NvmfStorageFuture(this, length);
			command.execute();
		} else {
//			LOG.info("unaligned");
			long alignedLength = NvmfStorageUtils.alignLength(sectorSize, remoteOffset, length);

			ByteBuffer stagingBuffer = cache.getBuffer();
			stagingBuffer.clear();
			stagingBuffer.limit((int)alignedLength);
			try {
				switch(op) {
					case READ: {
						NvmfStorageFuture f = futures[(int)command.getId()] = new NvmfStorageFuture(this, (int)alignedLength);
						command.setBuffer(stagingBuffer).setLinearBlockAddress(lba).read().execute();
						future = new NvmfStorageUnalignedReadFuture(f, this, buffer, remoteMr, remoteOffset, stagingBuffer);
						break;
					}
					case WRITE: {
						if (NvmfStorageUtils.namespaceSectorOffset(sectorSize, remoteOffset) == 0) {
							// Do not read if the offset is aligned to sector size
							int sizeToWrite = length;
							stagingBuffer.put(buffer);
							stagingBuffer.position(0);
							command.setBuffer(stagingBuffer).setLinearBlockAddress(lba).write().execute();
							future = futures[(int)command.getId()] = new NvmfStorageFuture(this, sizeToWrite);
						} else {
							// RMW but append only file system allows only reading last sector
							// and dir entries are sector aligned
							stagingBuffer.limit(sectorSize);
							NvmfStorageFuture f = futures[(int)command.getId()] = new NvmfStorageFuture(this, sectorSize);
							command.setBuffer(stagingBuffer).setLinearBlockAddress(lba).read().execute();
							future = new NvmfStorageUnalignedRMWFuture(f, this, buffer, remoteMr, remoteOffset, stagingBuffer);
						}
						break;
					}
				}
			} catch (NoSuchFieldException e) {
				throw new IOException(e);
			} catch (IllegalAccessException e) {
				throw new IOException(e);
			}
		}

		return future;
	}

	public Future<DataResult> write(ByteBuffer buffer, ByteBuffer region, BlockInfo blockInfo, long remoteOffset)
			throws IOException, InterruptedException {
		return Op(Operation.WRITE, buffer, blockInfo, remoteOffset);
	}

	public Future<DataResult> read(ByteBuffer buffer, ByteBuffer region, BlockInfo blockInfo, long remoteOffset)
			throws IOException, InterruptedException {
		return Op(Operation.READ, buffer, blockInfo, remoteOffset);
	}

	void poll() throws IOException {
		long[] ca = completed.get();
		int numberCompletions = endpoint.processCompletions(ca);
		for (int i = 0; i < numberCompletions; i++) {
			int idx = (int)ca[i];
			NvmeCommand command = commands[idx];
			IOCompletion completion = command.getCompletion();
			completion.done();
			futures[idx].signal(completion.getStatusCodeType(), completion.getStatusCode());
			freeCommands.add(command);
		}
	}

	void putBuffer(ByteBuffer buffer) throws IOException {
		cache.putBuffer(buffer);
	}

	public void close() throws IOException, InterruptedException {
		endpoint.close();
	}

	public boolean isLocal() {
		return false;
	}
}
