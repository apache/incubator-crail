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
import org.apache.crail.rpc.RpcNameNodeService;
import org.apache.crail.rpc.RpcServer;
import org.apache.crail.utils.CrailUtils;
import org.slf4j.Logger;

import com.ibm.darpc.DaRPCServerEndpoint;
import com.ibm.darpc.DaRPCServerGroup;
import com.ibm.disni.RdmaServerEndpoint;

public class DaRPCNameNodeServer extends RpcServer {
	private static final Logger LOG = CrailUtils.getLogger();
	
	private RpcNameNodeService service;
	private DaRPCServerGroup<DaRPCNameNodeRequest, DaRPCNameNodeResponse> namenodeServerGroup;
	private RdmaServerEndpoint<DaRPCServerEndpoint<DaRPCNameNodeRequest, DaRPCNameNodeResponse>> namenodeServerEp;	
	
	public DaRPCNameNodeServer(RpcNameNodeService service){
		this.service = service;
		this.namenodeServerEp = null;
		this.namenodeServerGroup = null;
	}	

	public void init(CrailConfiguration conf, String[] args) throws Exception{
		DaRPCConstants.updateConstants(conf);
		DaRPCConstants.verify();
		
		String _clusterAffinities[] = DaRPCConstants.NAMENODE_DARPC_AFFINITY.split(",");
		long clusterAffinities[] = new long[_clusterAffinities.length];
		for (int i = 0; i < clusterAffinities.length; i++){
			int affinity = Integer.decode(_clusterAffinities[i]).intValue();
			clusterAffinities[i] = 1L << affinity;
		}
		DaRPCServiceDispatcher darpcService = new DaRPCServiceDispatcher(service);
		this.namenodeServerGroup = DaRPCServerGroup.createServerGroup(darpcService, clusterAffinities, -1, DaRPCConstants.NAMENODE_DARPC_MAXINLINE, DaRPCConstants.NAMENODE_DARPC_POLLING, DaRPCConstants.NAMENODE_DARPC_RECVQUEUE, DaRPCConstants.NAMENODE_DARPC_SENDQUEUE, DaRPCConstants.NAMENODE_DARPC_POLLSIZE, DaRPCConstants.NAMENODE_DARPC_CLUSTERSIZE);
		LOG.info("rpc group started, recvQueue " + namenodeServerGroup.recvQueueSize());
		this.namenodeServerEp = namenodeServerGroup.createServerEndpoint();		
	}
	
	public void printConf(Logger logger){
		DaRPCConstants.printConf(logger);
	}

	@Override
	public void run() {
		try {
			InetSocketAddress addr = CrailUtils.getNameNodeAddress();
			namenodeServerEp.bind(addr, DaRPCConstants.NAMENODE_DARPC_BACKLOG);
			LOG.info("opened server at " + addr);
			while (true) {
				DaRPCServerEndpoint<DaRPCNameNodeRequest, DaRPCNameNodeResponse> clientEndpoint = namenodeServerEp.accept();
				LOG.info("accepting RPC connection, qpnum " + clientEndpoint.getQp().getQp_num());
			}
		} catch(Exception e){
			e.printStackTrace();
			LOG.error(e.getMessage());
		}
	}

}
