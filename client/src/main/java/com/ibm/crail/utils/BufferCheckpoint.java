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

package com.ibm.crail.utils;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import com.ibm.disni.util.*;

public class BufferCheckpoint {
	private static final Logger LOG = CrailUtils.getLogger();
	private ConcurrentHashMap<Long, ByteBuffer> checkpoint;
	
	public BufferCheckpoint(){
		this.checkpoint = new ConcurrentHashMap<Long, ByteBuffer>();
	}
	
	public void checkIn(ByteBuffer buffer) throws Exception {
		long address = MemoryUtils.getAddress(buffer);
		ByteBuffer oldBuf = checkpoint.putIfAbsent(address, buffer);
		if (oldBuf != null){
			LOG.info("ERROR Buffer already in use!");
			StackTraceElement[] stack = Thread.currentThread().getStackTrace();
			for (StackTraceElement element : stack){
				LOG.info(element.toString());
			}
			throw new Exception("ERROR Buffer already in use!");
		}
	}
	
	public void checkOut(ByteBuffer buffer){
		long address = MemoryUtils.getAddress(buffer);
		checkpoint.remove(address);
	}
}
