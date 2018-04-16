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

import com.ibm.jnvmf.Freeable;
import com.ibm.jnvmf.KeyedNativeBuffer;
import com.ibm.jnvmf.QueuePair;
import org.apache.crail.CrailBuffer;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

class NvmfRegisteredBufferCache implements Freeable {
	private final QueuePair queuePair;
	private final Map<Long, KeyedNativeBuffer> regionMap;
	private boolean valid;

	public NvmfRegisteredBufferCache(QueuePair queuePair) {
		this.queuePair = queuePair;
		this.regionMap = new ConcurrentHashMap<>();
		this.valid = true;
	}

	int getRemoteKey(CrailBuffer buffer) throws IOException {
		CrailBuffer regionBuffer = buffer.getRegion();
		KeyedNativeBuffer keyedNativeBuffer = regionMap.get(regionBuffer.address());
		if (keyedNativeBuffer == null) {
			/* region has not been registered yet */
			keyedNativeBuffer = queuePair.registerMemory(regionBuffer.getByteBuffer());
			KeyedNativeBuffer prevKeyedNativeBuffer =
					regionMap.putIfAbsent(keyedNativeBuffer.getAddress(), keyedNativeBuffer);
			if (prevKeyedNativeBuffer != null) {
				/* someone registered the same region in parallel */
				keyedNativeBuffer.free();
				keyedNativeBuffer = prevKeyedNativeBuffer;
			}
		}
		return keyedNativeBuffer.getRemoteKey();
	}


	@Override
	public void free() throws IOException {
		for (KeyedNativeBuffer buffer : regionMap.values()) {
			buffer.free();
		}
		valid = false;
	}

	@Override
	public boolean isValid() {
		return valid;
	}
}
