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

package org.apache.crail.metadata;

import java.net.UnknownHostException;
import java.nio.ByteBuffer;

public class DataNodeStatistics {
	public static final int CSIZE = 12;
	
	private long serviceId;
	private int freeBlockCount;
	
	public DataNodeStatistics(){
		this.serviceId = 0;
		this.freeBlockCount = 0;
	}
	
	public int write(ByteBuffer buffer){
		buffer.putLong(serviceId);
		buffer.putInt(freeBlockCount);
		return CSIZE;
	}
	
	public void update(ByteBuffer buffer) throws UnknownHostException {
		this.serviceId = buffer.getLong();
		this.freeBlockCount = buffer.getInt();
	}

	public int getFreeBlockCount() {
		return freeBlockCount;
	}

	public void setFreeBlockCount(int blockCount) {
		this.freeBlockCount = blockCount;
	}

	public void setStatistics(DataNodeStatistics statistics) {
		this.serviceId = statistics.getServiceId();
		this.freeBlockCount = statistics.getFreeBlockCount();
	}

	public void setServiceId(long serviceId) {
		this.serviceId = serviceId;
	}	
	
	public long getServiceId(){
		return serviceId;
	}
}
