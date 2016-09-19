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

package com.ibm.crail.namenode.rpc.darpc;

import java.net.InetSocketAddress;

import org.slf4j.Logger;

import com.ibm.crail.conf.CrailConstants;
import com.ibm.crail.namenode.rpc.*;
import com.ibm.crail.utils.CrailUtils;
import com.ibm.darpc.RpcActiveEndpointGroup;
import com.ibm.darpc.RpcClientEndpoint;
import com.ibm.darpc.RpcEndpointGroup;
import com.ibm.darpc.RpcPassiveEndpointGroup;
import com.ibm.disni.endpoints.RdmaServerEndpoint;

public class DaRPCNameNode implements RpcNameNode {
	private static final Logger LOG = CrailUtils.getLogger();
	
	private RpcEndpointGroup<DaRPCNameNodeRequest, DaRPCNameNodeResponse> namenodeClientGroup;
	private RpcClientEndpoint<DaRPCNameNodeRequest, DaRPCNameNodeResponse> namenodeClientEp;
	
	private RpcEndpointGroup<DaRPCNameNodeRequest, DaRPCNameNodeResponse> namenodeServerGroup;
	private RdmaServerEndpoint<RpcClientEndpoint<DaRPCNameNodeRequest, DaRPCNameNodeResponse>> namenodeServerEp;
	
	public DaRPCNameNode(){
		this.namenodeClientEp = null;
		this.namenodeClientGroup = null;
		this.namenodeServerEp = null;
		this.namenodeServerGroup = null;
	}

	@Override
	public RpcNameNodeClient getRpcClient(InetSocketAddress address) throws Exception {
		DaRPCNameNodeProtocol namenodeProtocol = new DaRPCNameNodeProtocol();
		long clusterAffinities[] = { 1L << 1 | 1L << 17};
		if (CrailConstants.NAMENODE_DARPC_TYPE.equalsIgnoreCase("passive")){
			LOG.info("passive RPC");
			this.namenodeClientGroup = RpcPassiveEndpointGroup.createDefault(namenodeProtocol, clusterAffinities, 100, CrailConstants.NAMENODE_DARPC_MAXINLINE, false, CrailConstants.NAMENODE_DARPC_QUEUESIZE, 4, CrailConstants.NAMENODE_DARPC_QUEUESIZE*2);
		} else {
			LOG.info("active RPC");		
			this.namenodeClientGroup = RpcActiveEndpointGroup.createDefault(namenodeProtocol, clusterAffinities, 100, CrailConstants.NAMENODE_DARPC_MAXINLINE, false, CrailConstants.NAMENODE_DARPC_QUEUESIZE, 4, CrailConstants.NAMENODE_DARPC_QUEUESIZE*2);
		}
		LOG.info("rpc group started, maxWR " + namenodeClientGroup.getRpcpipeline() + ", maxSge " + namenodeClientGroup.getMaxSge() + ", cqSize " + namenodeClientGroup.getCqSize());
		this.namenodeClientEp = namenodeClientGroup.createClientEndpoint();
		
		InetSocketAddress nnAddr = CrailUtils.getNameNodeAddress();
		LOG.info("connecting to namenode at " + nnAddr);
		namenodeClientEp.connect(nnAddr, 1000);
		DaRPCNameNodeClient namenodeClientRpc = new DaRPCNameNodeClient(namenodeClientEp);
		return namenodeClientRpc;
		
	}

	@Override
	public void run(RpcNameNodeService service) {
		try {
			String _clusterAffinities[] = CrailConstants.NAMENODE_DARPC_AFFINITY.split(",");
			long clusterAffinities[] = new long[_clusterAffinities.length];
			for (int i = 0; i < clusterAffinities.length; i++){
				int affinity = Integer.decode(_clusterAffinities[i]).intValue();
				clusterAffinities[i] = 1L << affinity;
			}
			DaRPCServiceDispatcher darpcService = new DaRPCServiceDispatcher(service);
			this.namenodeServerGroup = RpcActiveEndpointGroup.createDefault(darpcService, clusterAffinities, -1, CrailConstants.NAMENODE_DARPC_MAXINLINE, CrailConstants.NAMENODE_DARPC_POLLING, CrailConstants.NAMENODE_DARPC_QUEUESIZE, 4, CrailConstants.NAMENODE_DARPC_QUEUESIZE*2*100);
			LOG.info("rpc group started, maxWR " + namenodeServerGroup.getRpcpipeline() + ", maxSge " + namenodeServerGroup.getMaxSge() + ", cqSize " + namenodeServerGroup.getCqSize());
			this.namenodeServerEp = namenodeServerGroup.createServerEndpoint();
			
			InetSocketAddress addr = CrailUtils.getNameNodeAddress();
			namenodeServerEp.bind(addr, 100);
			LOG.info("opened server at " + addr);
			while (true) {
				RpcClientEndpoint<DaRPCNameNodeRequest, DaRPCNameNodeResponse> clientEndpoint = namenodeServerEp.accept();
//				LOG.info("accepting RPC connection, qpnum " + clientEndpoint.getQp().getQp_num() + ", total " + allEndpoints.size());
			}
		} catch(Exception e){
			e.printStackTrace();
			LOG.error(e.getMessage());
		}
		
	
	}

	@Override
	public void close() {
		try {
			if (namenodeClientEp != null){
				namenodeClientEp.close();
			}
			if (namenodeClientGroup != null){
				namenodeClientGroup.close();
			}
			if (namenodeServerEp != null){
				namenodeServerEp.close();
			}
			if (namenodeServerGroup != null){
				namenodeServerGroup.close();
			}			
		} catch(Exception e){
			LOG.info("Error while closing ");
		}
	}

}
