/*
 * Crail: A Multi-tiered Distributed Direct Access File System
 *
 * Author: Patrick Stuedi <stu@zurich.ibm.com>
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

package org.apache.crail.storage;

import java.io.IOException;

import org.apache.crail.CrailBuffer;
import org.apache.crail.metadata.BlockInfo;

public interface StorageEndpoint{

	public abstract StorageFuture write(CrailBuffer buffer, BlockInfo remoteMr, long remoteOffset) throws IOException,
			InterruptedException;

	public abstract StorageFuture read(CrailBuffer buffer, BlockInfo remoteMr, long remoteOffset) throws IOException,
			InterruptedException;

	public abstract void close() throws IOException, InterruptedException;
	
	public abstract boolean isLocal();
}
