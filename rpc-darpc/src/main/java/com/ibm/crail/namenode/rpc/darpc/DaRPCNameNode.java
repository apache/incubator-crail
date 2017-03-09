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
import java.net.URI;

import org.slf4j.Logger;

import com.ibm.crail.conf.CrailConstants;
import com.ibm.crail.namenode.rpc.*;
import com.ibm.crail.utils.CrailUtils;
import com.ibm.darpc.RpcClientEndpoint;
import com.ibm.darpc.RpcClientGroup;
import com.ibm.darpc.RpcServerEndpoint;
import com.ibm.darpc.RpcServerGroup;
import com.ibm.disni.rdma.RdmaServerEndpoint;

public class DaRPCNameNode implements RpcNameNode {
	private static final Logger LOG = CrailUtils.getLogger();
	
	private RpcClientGroup<DaRPCNameNodeRequest, DaRPCNameNodeResponse> namenodeClientGroup;
	private RpcClientEndpoint<DaRPCNameNodeRequest, DaRPCNameNodeResponse> namenodeClientEp;
	
	private RpcServerGroup<DaRPCNameNodeRequest, DaRPCNameNodeResponse> namenodeServerGroup;
	private RdmaServerEndpoint<RpcServerEndpoint<DaRPCNameNodeRequest, DaRPCNameNodeResponse>> namenodeServerEp;
	
	public DaRPCNameNode(){
		this.namenodeClientEp = null;
		this.namenodeClientGroup = null;
		this.namenodeServerEp = null;
		this.namenodeServerGroup = null;
	}

	@Override
	public RpcNameNodeClient getRpcClient(InetSocketAddress address) throws Exception {
		DaRPCNameNodeProtocol namenodeProtocol = new DaRPCNameNodeProtocol();
		this.namenodeClientGroup = RpcClientGroup.createClientGroup(namenodeProtocol, 100, CrailConstants.NAMENODE_DARPC_MAXINLINE, CrailConstants.NAMENODE_DARPC_RECVQUEUE, CrailConstants.NAMENODE_DARPC_SENDQUEUE);
		LOG.info("rpc group started, recvQueue " + namenodeClientGroup.recvQueueSize());
		this.namenodeClientEp = namenodeClientGroup.createEndpoint();
		InetSocketAddress nnAddr = CrailUtils.getNameNodeAddress();
		LOG.info("connecting to namenode at " + nnAddr);
		URI uri = URI.create("rdma://" + nnAddr.getAddress().getHostAddress() + ":" + nnAddr.getPort());
		namenodeClientEp.connect(uri);
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
			this.namenodeServerGroup = RpcServerGroup.createServerGroup(darpcService, clusterAffinities, -1, CrailConstants.NAMENODE_DARPC_MAXINLINE, CrailConstants.NAMENODE_DARPC_POLLING, CrailConstants.NAMENODE_DARPC_RECVQUEUE, CrailConstants.NAMENODE_DARPC_SENDQUEUE, CrailConstants.NAMENODE_DARPC_POLLSIZE, CrailConstants.NAMENODE_DARPC_CLUSTERSIZE);
			LOG.info("rpc group started, recvQueue " + namenodeServerGroup.recvQueueSize());
			this.namenodeServerEp = namenodeServerGroup.createServerEndpoint();
			
			InetSocketAddress addr = CrailUtils.getNameNodeAddress();
			URI uri = URI.create("rdma://" + addr.getAddress().getHostAddress() + ":" + addr.getPort());
			namenodeServerEp.bind(uri);
			LOG.info("opened server at " + addr);
			while (true) {
				RpcServerEndpoint<DaRPCNameNodeRequest, DaRPCNameNodeResponse> clientEndpoint = namenodeServerEp.accept();
				LOG.info("accepting RPC connection, qpnum " + clientEndpoint.getQp().getQp_num());
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
				namenodeClientEp = null;
			}
			if (namenodeClientGroup != null){
				namenodeClientGroup.close();
				namenodeClientGroup = null;
			}
			if (namenodeServerEp != null){
				namenodeServerEp.close();
				namenodeServerEp = null;
			}
			if (namenodeServerGroup != null){
				namenodeServerGroup.close();
				namenodeServerGroup = null;
			}			
		} catch(Exception e){
			e.printStackTrace();
			LOG.info("Error while closing " + e.getMessage());
		}
	}

}
