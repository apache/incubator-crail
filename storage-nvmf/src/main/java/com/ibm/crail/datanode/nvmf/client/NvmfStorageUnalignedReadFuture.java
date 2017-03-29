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

import com.ibm.crail.namenode.protocol.BlockInfo;
import com.ibm.crail.storage.StorageEndpoint;
import com.ibm.crail.storage.DataResult;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class NvmfStorageUnalignedReadFuture extends NvmfStorageUnalignedFuture {

	public NvmfStorageUnalignedReadFuture(NvmfStorageFuture future, NvmfStorageEndpoint endpoint, ByteBuffer buffer, BlockInfo remoteMr,
									   long remoteOffset, ByteBuffer stagingBuffer)
			throws NoSuchFieldException, IllegalAccessException {
		super(future, endpoint, buffer, remoteMr, remoteOffset, stagingBuffer);
	}

	public DataResult get(long l, TimeUnit timeUnit) throws InterruptedException, ExecutionException, TimeoutException {
		if (exception != null) {
			throw new ExecutionException(exception);
		}
		if (!done) {
			initFuture.get(l, timeUnit);
			long srcAddr = NvmfStorageUtils.getAddress(stagingBuffer) +
					NvmfStorageUtils.namespaceSectorOffset(endpoint.getSectorSize(), remoteOffset);
			long dstAddr = NvmfStorageUtils.getAddress(buffer) + localOffset;
			unsafe.copyMemory(srcAddr, dstAddr, len);
			done = true;
			try {
				endpoint.putBuffer(stagingBuffer);
			} catch (IOException e) {
				throw new ExecutionException(e);
			}
		}
		return this;
	}
}
