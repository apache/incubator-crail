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

import com.ibm.crail.datanode.nvmf.NvmfStorageConstants;
import com.ibm.crail.namenode.protocol.BlockInfo;
import sun.misc.Unsafe;
import com.ibm.crail.storage.DataResult;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public abstract class NvmfStorageUnalignedFuture implements Future<DataResult>, DataResult  {
	protected final NvmfStorageFuture initFuture;
	protected final NvmfStorageEndpoint endpoint;
	protected final ByteBuffer buffer;
	protected final long localOffset;
	protected final BlockInfo remoteMr;
	protected final long remoteOffset;
	protected final int len;
	protected final ByteBuffer stagingBuffer;
	protected boolean done;
	protected Exception exception;
	protected static Unsafe unsafe;

	public NvmfStorageUnalignedFuture(NvmfStorageFuture future, NvmfStorageEndpoint endpoint, ByteBuffer buffer,
								   BlockInfo remoteMr, long remoteOffset, ByteBuffer stagingBuffer)
			throws NoSuchFieldException, IllegalAccessException {
		this.initFuture = future;
		this.endpoint = endpoint;
		this.buffer = buffer;
		this.localOffset = buffer.position();
		this.remoteMr = remoteMr;
		this.remoteOffset = remoteOffset;
		this.len = buffer.remaining();
		this.stagingBuffer = stagingBuffer;
		initUnsafe();
		done = false;
	}

	public boolean isDone() {
		if (!done) {
			try {
				get(0, TimeUnit.NANOSECONDS);
			} catch (InterruptedException e) {
				exception = e;
			} catch (ExecutionException e) {
				exception = e;
			} catch (TimeoutException e) {
				// i.e. operation is not finished
			}
		}
		return done;
	}

	public int getLen() {
		return len;
	}

	public boolean cancel(boolean b) {
		return false;
	}

	public boolean isCancelled() {
		return false;
	}

	public DataResult get() throws InterruptedException, ExecutionException {
		try {
			return get(NvmfStorageConstants.TIME_OUT, NvmfStorageConstants.TIME_UNIT);
		} catch (TimeoutException e) {
			throw new ExecutionException(e);
		}
	}

	private static void initUnsafe() throws NoSuchFieldException, IllegalAccessException {
		if (unsafe == null) {
			Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
			theUnsafe.setAccessible(true);
			unsafe = (Unsafe) theUnsafe.get(null);
		}
	}
}
