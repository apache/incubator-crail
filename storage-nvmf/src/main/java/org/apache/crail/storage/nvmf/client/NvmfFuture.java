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
import org.apache.crail.storage.StorageFuture;
import org.apache.crail.storage.StorageResult;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

public class NvmfFuture<Command extends NvmIoCommand<? extends NvmIoCommandCapsule>> implements StorageFuture, OperationCallback {
	private final NvmfStorageEndpoint endpoint;
	private final Command command;
	private final Queue<Command> operations;
	private volatile boolean done;
	private RdmaException exception;
	private final StorageResult storageResult;
	private final Response<NvmResponseCapsule> response;
	private final AtomicInteger completed;

	NvmfFuture(NvmfStorageEndpoint endpoint, Command command, Response<NvmResponseCapsule> response,
			   Queue<Command> operations, int length) {
		this.endpoint = endpoint;
		this.command = command;
		this.operations = operations;
		this.done = false;
		this.storageResult = () -> length;
		this.response = response;
		this.completed = new AtomicInteger(0);
	}

	@Override
	public boolean isSynchronous() {
		return false;
	}

	@Override
	public boolean cancel(boolean b) {
		return false;
	}

	@Override
	public boolean isCancelled() {
		return false;
	}

	@Override
	public boolean isDone() {
		if (!done) {
			try {
				endpoint.poll();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
		return done;
	}

	private final void checkStatus() throws ExecutionException {
		if (exception != null) {
			throw new ExecutionException(exception);
		}
		NvmCompletionQueueEntry cqe = response.getResponseCapsule().getCompletionQueueEntry();
		StatusCode.Value statusCode = cqe.getStatusCode();
		if (statusCode != null) {
			if (!statusCode.equals(GenericStatusCode.getInstance().SUCCESS)) {
				throw new ExecutionException(new UnsuccessfulComandException(cqe));
			}
		}
	}

	@Override
	public StorageResult get() throws InterruptedException, ExecutionException {
		try {
			return get(2, TimeUnit.MINUTES);
		} catch (TimeoutException e) {
			throw new ExecutionException(e);
		}
	}

	@Override
	public StorageResult get(long timeout, TimeUnit timeUnit) throws InterruptedException, ExecutionException, TimeoutException {
		if (!done) {
			long start = System.nanoTime();
			long end = start + TimeUnit.NANOSECONDS.convert(timeout, timeUnit);
			boolean waitTimeOut;
			do {
				try {
					endpoint.poll();
				} catch (IOException e) {
					throw new ExecutionException(e);
				}
				waitTimeOut = System.nanoTime() > end;
			} while (!done && !waitTimeOut);
			if (!done && waitTimeOut) {
				throw new TimeoutException("poll wait time out!");
			}
		}
		checkStatus();
		return storageResult;
	}

	@Override
	public void onStart() {

	}

	@Override
	public void onComplete() {
		assert !done;
		assert completed.get() < 2;
		if (completed.incrementAndGet() == 2) {
			/* we need to complete command and response */
			operations.add(command);
			this.done = true;
			endpoint.putOperation();
		}
	}

	@Override
	public void onFailure(RdmaException e) {
		assert !done;
		this.operations.add(command);
		this.exception = e;
		this.done = true;
		endpoint.putOperation();
	}
}
