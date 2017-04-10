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

package com.ibm.crail.storage;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.slf4j.Logger;

import com.ibm.crail.conf.CrailConfiguration;

public interface StorageClient {
	public abstract void init(CrailConfiguration conf, String[] args) throws IOException;
	public abstract StorageEndpoint createEndpoint(InetSocketAddress inetAddress) throws IOException;	
	public abstract void close() throws Exception;
	public abstract void printConf(Logger log);
	
	@SuppressWarnings("unchecked")
	public static StorageClient createInstance(String name) throws Exception {
		Class<?> nodeClass = Class.forName(name);
		if (StorageClient.class.isAssignableFrom(nodeClass)){
			Class<? extends StorageClient> storageClientClass = (Class<? extends StorageClient>) nodeClass;
			StorageClient client = storageClientClass.newInstance();
			return client;
		} else {
			throw new Exception("Cannot instantiate storage client of type " + name);
		}
		
	}	
}
