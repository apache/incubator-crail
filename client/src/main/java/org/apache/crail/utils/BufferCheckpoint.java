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

package org.apache.crail.utils;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.crail.CrailBuffer;
import org.slf4j.Logger;

public class BufferCheckpoint {
	private static final Logger LOG = CrailUtils.getLogger();
	private ConcurrentHashMap<Long, CrailBuffer> checkpoint;
	
	public BufferCheckpoint(){
		this.checkpoint = new ConcurrentHashMap<Long, CrailBuffer>();
	}
	
	public void checkIn(CrailBuffer buffer) throws Exception {
		long address = buffer.address();
		CrailBuffer oldBuf = checkpoint.putIfAbsent(address, buffer);
		if (oldBuf != null){
			LOG.info("ERROR Buffer already in use!");
			StackTraceElement[] stack = Thread.currentThread().getStackTrace();
			for (StackTraceElement element : stack){
				LOG.info(element.toString());
			}
			throw new Exception("ERROR Buffer already in use!");
		}
	}
	
	public void checkOut(CrailBuffer buffer){
		long address = buffer.address();
		checkpoint.remove(address);
	}
}
