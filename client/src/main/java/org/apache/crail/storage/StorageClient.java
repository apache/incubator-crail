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

package org.apache.crail.storage;

import java.io.IOException;

import org.apache.crail.CrailBufferCache;
import org.apache.crail.CrailStatistics;
import org.apache.crail.conf.CrailConfiguration;
import org.apache.crail.metadata.DataNodeInfo;
import org.slf4j.Logger;

public interface StorageClient {
	StorageEndpoint createEndpoint(DataNodeInfo info) throws IOException;
	void close() throws Exception;
	void init(CrailStatistics statistics, CrailBufferCache bufferCache, CrailConfiguration configuration,
			  String[] args) throws IOException;
	void printConf(Logger log);


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
