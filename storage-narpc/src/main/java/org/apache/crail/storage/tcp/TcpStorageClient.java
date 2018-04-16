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

package org.apache.crail.storage.tcp;

import java.io.IOException;

import org.apache.crail.CrailBufferCache;
import org.apache.crail.CrailStatistics;
import org.apache.crail.conf.CrailConfiguration;
import org.apache.crail.conf.CrailConstants;
import org.apache.crail.metadata.DataNodeInfo;
import org.apache.crail.storage.StorageClient;
import org.apache.crail.storage.StorageEndpoint;
import org.apache.crail.utils.CrailUtils;
import org.slf4j.Logger;

import com.ibm.narpc.NaRPCClientGroup;
import com.ibm.narpc.NaRPCEndpoint;

public class TcpStorageClient implements StorageClient {
	private NaRPCClientGroup<TcpStorageRequest, TcpStorageResponse> clientGroup;

	@Override
	public void init(CrailStatistics statistics, CrailBufferCache bufferCache, CrailConfiguration conf, String[] args)
			throws IOException {
		TcpStorageConstants.updateConstants(conf);

		this.clientGroup = new NaRPCClientGroup<TcpStorageRequest, TcpStorageResponse>(TcpStorageConstants.STORAGE_TCP_QUEUE_DEPTH, (int) CrailConstants.BLOCK_SIZE*2, false);
	}

	@Override
	public void printConf(Logger logger) {
		TcpStorageConstants.printConf(logger);
	}

	@Override
	public void close() throws Exception {
	}

	@Override
	public StorageEndpoint createEndpoint(DataNodeInfo info) throws IOException {
		try {
			NaRPCEndpoint<TcpStorageRequest, TcpStorageResponse> narpcEndpoint = clientGroup.createEndpoint();
			TcpStorageEndpoint endpoint = new TcpStorageEndpoint(narpcEndpoint);
			endpoint.connect(CrailUtils.datanodeInfo2SocketAddr(info));
			return endpoint;
		} catch(Exception e){
			throw new IOException(e);
		}
	}

}
