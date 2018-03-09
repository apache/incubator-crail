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

package org.apache.crail.storage.object;

import org.apache.crail.conf.CrailConfiguration;
import org.apache.crail.metadata.DataNodeInfo;
import org.apache.crail.storage.StorageEndpoint;
import org.apache.crail.storage.StorageServer;
import org.apache.crail.storage.StorageTier;
import org.apache.crail.storage.object.client.ObjectStoreDataNodeEndpoint;
import org.apache.crail.storage.object.client.ObjectStoreMetadataClientGroup;
import org.apache.crail.storage.object.server.ObjectStoreServer;
import org.apache.crail.utils.CrailUtils;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;


public class ObjectStorageTier implements StorageTier {
	private static final Logger LOG = ObjectStoreUtils.getLogger();

	private ObjectStoreMetadataClientGroup metadataClientGroup = null;
	private ObjectStoreServer storageServer = null;

	@Override
	public void init(CrailConfiguration conf, String[] args) {
		LOG.debug("Initializing ObjectStorageTier");
		ObjectStoreConstants.updateConstants(conf);
		metadataClientGroup = new ObjectStoreMetadataClientGroup();
	}

	@Override
	public StorageServer launchServer() {
		LOG.info("Initializing ObjectStore Tier");
		return new ObjectStoreServer();
	}

	@Override
	public void printConf(Logger logger) {
		ObjectStoreConstants.printConf(logger);
	}

	@Override
	public void close() throws Exception {
		LOG.info("Closing ObjectStorageTier");
		if (metadataClientGroup != null) {
			// stop ObjectStore metadata service
			LOG.debug("Closing metadata client group");
			metadataClientGroup.closeClientGroup();
		}
		if (storageServer != null && storageServer.isAlive()) {
			// stop ObjectStore metadata service
			LOG.debug("Closing metadata server");
			storageServer.close();
		}
	}

	@Override
	public StorageEndpoint createEndpoint(DataNodeInfo dataNodeInfo) throws IOException {
		InetSocketAddress addr = CrailUtils.datanodeInfo2SocketAddr(dataNodeInfo);
		LOG.debug("Opening a connection to StorageNode: " + addr.toString());
		return new ObjectStoreDataNodeEndpoint(metadataClientGroup.getClient());
	}
}
