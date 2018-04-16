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

import org.apache.crail.conf.CrailConfiguration;
import org.apache.crail.rpc.RpcNameNodeService;
import org.apache.crail.rpc.RpcServer;
import org.apache.crail.utils.CrailUtils;
import org.slf4j.Logger;

import com.ibm.narpc.NaRPCServerChannel;
import com.ibm.narpc.NaRPCServerEndpoint;
import com.ibm.narpc.NaRPCServerGroup;

public class TcpNameNodeServer extends RpcServer {
	private static final Logger LOG = CrailUtils.getLogger();

	private TcpRpcDispatcher dispatcher;
	private NaRPCServerGroup<TcpNameNodeRequest, TcpNameNodeResponse> serverGroup;
	private NaRPCServerEndpoint<TcpNameNodeRequest, TcpNameNodeResponse> serverEndpoint;

	public TcpNameNodeServer(RpcNameNodeService service) throws IOException {
		this.dispatcher = new TcpRpcDispatcher(service);
	}

	@Override
	public void init(CrailConfiguration conf, String[] arg1) throws Exception {
		TcpRpcConstants.updateConstants(conf);
		TcpRpcConstants.verify();
		this.serverGroup = new NaRPCServerGroup<TcpNameNodeRequest, TcpNameNodeResponse>(
				dispatcher, TcpRpcConstants.NAMENODE_TCP_QUEUEDEPTH,
				TcpRpcConstants.NAMENODE_TCP_MESSAGESIZE, true, TcpRpcConstants.NAMENODE_TCP_CORES);
		this.serverEndpoint = serverGroup.createServerEndpoint();
		InetSocketAddress inetSocketAddress = CrailUtils.getNameNodeAddress();
		serverEndpoint.bind(inetSocketAddress);
	}

	@Override
	public void printConf(Logger logger) {
		TcpRpcConstants.printConf(logger);
	}

	public void run() {
		try {
			while (true) {
				NaRPCServerChannel endpoint = serverEndpoint.accept();
				LOG.info("new connection from " + endpoint.address());
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
