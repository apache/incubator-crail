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

package org.apache.crail.namenode.rpc.darpc;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.crail.rpc.RpcFuture;

import com.ibm.darpc.DaRPCFuture;

public class DaRPCNameNodeFuture<T> implements RpcFuture<T> {
	private DaRPCFuture<DaRPCNameNodeRequest, DaRPCNameNodeResponse> future;
	private boolean prefetched;
	private T response;
	
	public DaRPCNameNodeFuture(DaRPCFuture<DaRPCNameNodeRequest, DaRPCNameNodeResponse> future, T response) {
		this.future = future;
		this.response = response;
		this.prefetched = false;
	}

	@Override
	public T get() throws InterruptedException, ExecutionException {
		future.get();
		return response;
	}

	@Override
	public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
		future.get(timeout, unit);
		return response;
	}
	
	@Override
	public boolean isDone() {
		return future.isDone();
	}	

	@Override
	public int getTicket() {
		return future.getTicket();
	}
	
	@Override
	public boolean isPrefetched() {
		return prefetched;
	}

	@Override
	public void setPrefetched(boolean prefetched) {
		this.prefetched = prefetched;
	}	

	@Override
	public boolean cancel(boolean mayInterruptIfRunning) {
		return future.cancel(mayInterruptIfRunning);
	}

	@Override
	public boolean isCancelled() {
		return future.isCancelled();
	}
}
