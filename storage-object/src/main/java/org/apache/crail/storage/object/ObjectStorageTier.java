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

import org.apache.crail.utils.CrailUtils;
import org.apache.crail.conf.CrailConfiguration;
import org.apache.crail.metadata.DataNodeInfo;
import org.apache.crail.storage.StorageEndpoint;
import org.apache.crail.storage.StorageServer;
import org.apache.crail.storage.StorageTier;
import org.apache.crail.storage.object.client.ObjectStoreDataNodeEndpoint;
import org.apache.crail.storage.object.client.ObjectStoreMetadataClientGroup;
import org.apache.crail.storage.object.object.S3ObjectStoreClient;
import org.apache.crail.storage.object.server.ObjectStoreMetadataServer;
import org.apache.crail.storage.object.server.ObjectStoreServer;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;


public class ObjectStorageTier implements StorageTier {
	private static final Logger LOG = ObjectStoreUtils.getLogger();

	ObjectStoreMetadataClientGroup metadataClientGroup;
	private InetSocketAddress datanodeAddr;
	private long blockID = 0;
	private ObjectStoreMetadataServer metadataServer = null;
	private ObjectStoreServer storageServer = null;
	private S3ObjectStoreClient objectStoreClient = null;

	public ObjectStorageTier() {
		super();
		metadataClientGroup = null;
		metadataServer = null;
	}

	@Override
	public void init(CrailConfiguration crailConfiguration, String[] args) throws IOException {
		if (args != null) {
			for (int i = 0; i < args.length; i++) {
				char flag;
				try {
					if (args[i].charAt(0) == '-') {
						flag = args[i].charAt(1);
					} else {
						LOG.warn("Invalid flag {}", args[i]);
						continue;
					}
					switch (flag) {
						case 't':
							break;
						case 'o':
							String opt;
							if (args[i].length() > 2) {
								opt = args[i].substring(2);
							} else {
								i++;
								opt = args[i];
							}
							String[] split = opt.split("=");
							String key = split[0];
							String val = split[1];
							crailConfiguration.set(key, val);
							LOG.info("Set custom option {} = {} ", key, val);
							break;
						default:
							LOG.warn("Unknown flag {}", flag);
							continue;
					}
				} catch (Exception e) {
					LOG.warn("Error processing input {}", args[i]);
				}
			}
		}
		ObjectStoreConstants.updateConstants(crailConfiguration);
		ObjectStoreConstants.verify();
		metadataClientGroup = new ObjectStoreMetadataClientGroup();
	}

	@Override
	public void printConf(Logger logger) {
		ObjectStoreConstants.printConf(logger);
	}

	@Override
	public StorageEndpoint createEndpoint(DataNodeInfo dataNodeInfo) throws IOException {
		InetSocketAddress addr = CrailUtils.datanodeInfo2SocketAddr(dataNodeInfo);
		LOG.debug("Opening a connection to StorageNode: " + addr.toString());
		return new ObjectStoreDataNodeEndpoint(metadataClientGroup.getClient());
	}

	@Override
	public void close() throws Exception {
		LOG.info("Closing ObjectStore tier");
		if (metadataServer != null && metadataServer.isAlive()) {
			// stop ObjectStore metadata service
			LOG.debug("Closing metadata server");
			metadataServer.close();
		}
		if (storageServer != null && storageServer.isAlive()) {
			// stop ObjectStore metadata service
			LOG.debug("Closing metadata server");
			storageServer.close();
		}
		if (metadataClientGroup != null) {
			// stop ObjectStore metadata service
			LOG.debug("Closing metadata client group");
			metadataClientGroup.closeClientGroup();
		}
		if (objectStoreClient != null) {
			//objectStoreClient.deleteBucket(ObjectStoreConstants.S3_BUCKET_NAME);;
		}
	}

	@Override
	protected void finalize() {
		LOG.info("Datanode finalize");
		try {
			close();
		} catch (Exception e) {
			LOG.error("Could not close ObjectStoreDataNode. Reason: {}", e);
		}
	}

	@Override
	public StorageServer launchServer() throws Exception {
		LOG.info("Initializing ObjectStore Tier");
		objectStoreClient = new S3ObjectStoreClient();
		//objectStoreClient.createBucket(ObjectStoreConstants.S3_BUCKET_NAME);
		storageServer = new ObjectStoreServer();
		//Thread server = new Thread(storageServer);
		//server.start();
		metadataServer = new ObjectStoreMetadataServer();
		metadataServer.start();
		return storageServer;
	}
}
