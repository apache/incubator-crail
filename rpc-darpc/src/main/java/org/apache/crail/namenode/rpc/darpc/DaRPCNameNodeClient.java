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

package org.apache.crail.namenode.rpc.darpc;

import java.net.InetSocketAddress;
import org.apache.crail.conf.CrailConfiguration;
import org.apache.crail.rpc.RpcClient;
import org.apache.crail.rpc.RpcConnection;
import org.apache.crail.utils.CrailUtils;
import org.slf4j.Logger;

import com.ibm.darpc.DaRPCClientEndpoint;
import com.ibm.darpc.DaRPCClientGroup;

public class DaRPCNameNodeClient implements RpcClient {
	private static final Logger LOG = CrailUtils.getLogger();
	
	private DaRPCNameNodeProtocol namenodeProtocol;
	private DaRPCClientGroup<DaRPCNameNodeRequest, DaRPCNameNodeResponse> namenodeClientGroup;
	
	public DaRPCNameNodeClient(){
		this.namenodeProtocol = null;
		this.namenodeClientGroup = null;
	}
	
	public void init(CrailConfiguration conf, String[] args) throws Exception{
		DaRPCConstants.updateConstants(conf);
		DaRPCConstants.verify();
		this.namenodeProtocol = new DaRPCNameNodeProtocol();
		this.namenodeClientGroup = DaRPCClientGroup.createClientGroup(namenodeProtocol, 100, DaRPCConstants.NAMENODE_DARPC_MAXINLINE, DaRPCConstants.NAMENODE_DARPC_RECVQUEUE, DaRPCConstants.NAMENODE_DARPC_SENDQUEUE);
		LOG.info("rpc group started, recvQueue " + namenodeClientGroup.recvQueueSize());
	}
	
	public void printConf(Logger logger){
		DaRPCConstants.printConf(logger);
	}

	@Override
	public RpcConnection connect(InetSocketAddress address) throws Exception {
		DaRPCClientEndpoint<DaRPCNameNodeRequest, DaRPCNameNodeResponse> namenodeEndopoint = namenodeClientGroup.createEndpoint();
		namenodeEndopoint.connect(address, DaRPCConstants.NAMENODE_DARPC_CONNECTTIMEOUT);
		DaRPCNameNodeConnection connection = new DaRPCNameNodeConnection(namenodeEndopoint);
		return connection;
		
	}

	@Override
	public void close() {
		try {
			if (namenodeClientGroup != null){
				namenodeClientGroup.close();
				namenodeClientGroup = null;
			}
		} catch(Exception e){
			e.printStackTrace();
			LOG.info("Error while closing " + e.getMessage());
		}
	}

}
