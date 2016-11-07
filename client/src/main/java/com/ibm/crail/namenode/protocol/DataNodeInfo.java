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

package com.ibm.crail.namenode.protocol;

import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import org.slf4j.Logger;
import com.ibm.crail.utils.CrailUtils;

public class DataNodeInfo {
	private static final Logger LOG = CrailUtils.getLogger();
	public static final int CSIZE = 16;
	
	private int storageTier;
	private int locationAffinity;
	private byte[] ipAddress;
	private int port;	
	
	private long key;
	
	public DataNodeInfo(){
		this.storageTier = 0;
		this.locationAffinity = 0;
		this.ipAddress = new byte[4];
		this.port = 0;		
		this.key = 0;
	}
	
	public DataNodeInfo(int storageTier, int locationAffinity, byte[] ipAddress, int port){
		this();
		
		this.storageTier = storageTier;
		this.locationAffinity = locationAffinity;
		for (int i = 0; i < ipAddress.length; i++){
			this.ipAddress[i] = ipAddress[i];
		}
		this.port = port;
	}	
	
	void setDataNodeInfo(DataNodeInfo info) {
		this.storageTier = info.getStorageTier();
		this.locationAffinity = info.getLocationAffinity();
		for (int i = 0; i < ipAddress.length; i++){
			this.ipAddress[i] = info.getIpAddress()[i];
		}		
		this.port = info.getPort();
	}

	public int write(ByteBuffer buffer){
		buffer.putInt(storageTier);
		buffer.putInt(locationAffinity);
		buffer.put(ipAddress);
		buffer.putInt(port);		
		return CSIZE;
	}
	
	public void update(ByteBuffer buffer) throws UnknownHostException {
		this.storageTier = buffer.getInt();
		this.locationAffinity = buffer.getInt();
		buffer.get(ipAddress);
		this.port = buffer.getInt();
	}	
	
	public byte[] getIpAddress() {
		return ipAddress;
	}

	public int getPort() {
		return port;
	}

	public int getLocationAffinity() {
		return locationAffinity;
	}

	public int getStorageTier() {
		return storageTier;
	}
	
	public long key(){
		if (key == 0){
			int a = java.util.Arrays.hashCode(ipAddress);
			key = (((long)a) << 32) | (port & 0xffffffffL);
		}
		return key;
	}
}
