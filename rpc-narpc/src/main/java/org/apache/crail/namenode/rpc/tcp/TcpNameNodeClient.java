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

package org.apache.crail.namenode.rpc.tcp;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.LinkedList;

import org.apache.crail.conf.CrailConfiguration;
import org.apache.crail.rpc.RpcClient;
import org.apache.crail.rpc.RpcConnection;
import org.slf4j.Logger;

import com.ibm.narpc.NaRPCClientGroup;
import com.ibm.narpc.NaRPCEndpoint;

public class TcpNameNodeClient implements RpcClient {
	private NaRPCClientGroup<TcpNameNodeRequest, TcpNameNodeResponse> clientGroup;
	private LinkedList<TcpRpcConnection> allConnections;
	
    public void init(CrailConfiguration conf, String[] strings) throws IOException {
    	try {
    		TcpRpcConstants.updateConstants(conf);
    		TcpRpcConstants.verify();   		
    		this.clientGroup = new NaRPCClientGroup<TcpNameNodeRequest, TcpNameNodeResponse>(TcpRpcConstants.NAMENODE_TCP_QUEUEDEPTH, TcpRpcConstants.NAMENODE_TCP_MESSAGESIZE, true);
    		this.allConnections = new LinkedList<TcpRpcConnection>();
    	} catch(Exception e){
    		throw new IOException(e);
    	}
    }

    public void printConf(Logger logger) {
    	TcpRpcConstants.printConf(logger);
    }

    /* This function comes from RPCClient interface */
    public RpcConnection connect(InetSocketAddress address) throws IOException {
    	try {
    		NaRPCEndpoint<TcpNameNodeRequest, TcpNameNodeResponse> endpoint = clientGroup.createEndpoint();
    		endpoint.connect(address);
    		TcpRpcConnection connection = new TcpRpcConnection(endpoint);
    		allConnections.add(connection);
    		return connection;
    	} catch(Exception e){
    		throw new IOException(e);
    	}
    }

	@Override
	public void close() {
		try {
			for (TcpRpcConnection connection : allConnections){
				connection.close();
			}
		} catch(Exception e){
		}
	}

}
