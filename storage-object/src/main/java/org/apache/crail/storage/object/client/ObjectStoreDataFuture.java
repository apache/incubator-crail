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

import org.apache.crail.storage.StorageFuture;
import org.apache.crail.storage.StorageResult;
import org.apache.crail.storage.object.ObjectStoreUtils;
import org.slf4j.Logger;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

public class ObjectStoreDataFuture implements StorageFuture, StorageResult {

	private static final Logger LOG = ObjectStoreUtils.getLogger();
	private static final AtomicInteger hash = new AtomicInteger(0);
	private final int hashCode;
	private final int length;

	public ObjectStoreDataFuture(int length) {
		this.hashCode = hash.getAndIncrement();
		this.length = length;
	}

	public int getLen() {
		return this.length;
	}

	public boolean cancel(boolean mayInterruptIfRunning) {
		return false;
	}

	public boolean isCancelled() {
		return false;
	}

	public boolean isDone() {
		return true;
	}

	public final StorageResult get() throws InterruptedException, ExecutionException {
		return this;
	}

	public StorageResult get(long timeout, TimeUnit unit)
			throws InterruptedException, ExecutionException, TimeoutException {
		return this;
	}

	@Override
	public int hashCode() {
		return hashCode;
	}

	@Override
	public boolean equals(Object o) {
		return hashCode == o.hashCode();
	}

	@Override
	public boolean isSynchronous() {
		return true;
	}
}
