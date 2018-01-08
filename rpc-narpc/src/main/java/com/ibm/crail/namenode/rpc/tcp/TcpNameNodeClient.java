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

package com.ibm.crail.namenode.rpc.tcp;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.slf4j.Logger;

import com.ibm.crail.conf.CrailConfiguration;
import com.ibm.crail.rpc.RpcClient;
import com.ibm.crail.rpc.RpcConnection;
import com.ibm.narpc.NaRPCClientGroup;
import com.ibm.narpc.NaRPCEndpoint;

public class TcpNameNodeClient implements RpcClient {
	private NaRPCClientGroup<TcpNameNodeRequest, TcpNameNodeResponse> clientGroup;
	private NaRPCEndpoint<TcpNameNodeRequest, TcpNameNodeResponse> endpoint;
	
    public void init(CrailConfiguration conf, String[] strings) throws IOException {
    	try {
    		TcpRpcConstants.updateConstants(conf);
    		TcpRpcConstants.verify();   		
    		this.clientGroup = new NaRPCClientGroup<TcpNameNodeRequest, TcpNameNodeResponse>(TcpRpcConstants.NAMENODE_TCP_QUEUEDEPTH, TcpRpcConstants.NAMENODE_TCP_MESSAGESIZE, true);
    		this.endpoint = clientGroup.createEndpoint();
    	} catch(Exception e){
    		throw new IOException(e);
    	}
    }

    public void printConf(Logger logger) {
    	TcpRpcConstants.printConf(logger);
    }

    /* This function comes from RPCClient interface */
    public RpcConnection connect(InetSocketAddress address) throws IOException {
    	endpoint.connect(address);
    	return new TcpRpcConnection(endpoint);
    }

	@Override
	public void close() {
		try {
			endpoint.close();
		} catch(Exception e){
		}
	}

}
