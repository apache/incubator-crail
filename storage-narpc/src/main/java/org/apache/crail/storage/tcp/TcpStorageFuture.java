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

package org.apache.crail.storage.tcp;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.crail.storage.StorageFuture;
import org.apache.crail.storage.StorageResult;

import com.ibm.narpc.NaRPCFuture;

public class TcpStorageFuture implements StorageFuture, StorageResult {
	private NaRPCFuture<TcpStorageRequest, TcpStorageResponse> future;
	private int len;
	
	public TcpStorageFuture(NaRPCFuture<TcpStorageRequest, TcpStorageResponse> future, int len) {
		this.future = future;
		this.len = len;
	}

	@Override
	public boolean cancel(boolean mayInterruptIfRunning) {
		return false;
	}

	@Override
	public boolean isCancelled() {
		return false;
	}

	@Override
	public boolean isDone() {
		return future.isDone();
	}

	@Override
	public StorageResult get() throws InterruptedException, ExecutionException {
		future.get();
		return this;
	}

	@Override
	public StorageResult get(long timeout, TimeUnit unit)
			throws InterruptedException, ExecutionException, TimeoutException {
		future.get(timeout, unit);
		return this;
	}

	@Override
	public boolean isSynchronous() {
		return false;
	}

	@Override
	public int getLen() {
		return len;
	}

}
