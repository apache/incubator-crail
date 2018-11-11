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

import com.ibm.narpc.NaRPCService;
import com.ibm.narpc.NaRPCServerChannel;

import org.apache.crail.rpc.RpcErrors;
import org.apache.crail.rpc.RpcNameNodeService;
import org.apache.crail.rpc.RpcProtocol;
import org.apache.crail.utils.CrailUtils;
import org.slf4j.Logger;

public class TcpRpcDispatcher implements NaRPCService<TcpNameNodeRequest, TcpNameNodeResponse> {
	public static final Logger LOG = CrailUtils.getLogger();
	private RpcNameNodeService service;
	
	public TcpRpcDispatcher(RpcNameNodeService service) {
		this.service = service;
	}

	@Override
	public TcpNameNodeRequest createRequest() {
		return new TcpNameNodeRequest();
	}

	@Override
	public TcpNameNodeResponse processRequest(TcpNameNodeRequest request) {
		TcpNameNodeResponse response = new TcpNameNodeResponse();
		short error = RpcErrors.ERR_OK;
		try {
			short type = RpcProtocol.responseTypes[request.getCmd()];
			response.setType(type);
			switch(request.getCmd()) {
			case RpcProtocol.CMD_CREATE_FILE:
				error = service.createFile(request.createFile(), response.createFile(), response);
				break;			
			case RpcProtocol.CMD_GET_FILE:
				error = service.getFile(request.getFile(), response.getFile(), response);
				break;
			case RpcProtocol.CMD_SET_FILE:
				error = service.setFile(request.setFile(), response.getVoid(), response);
				break;
			case RpcProtocol.CMD_REMOVE_FILE:
				error = service.removeFile(request.removeFile(), response.removeFile(), response);
				break;				
			case RpcProtocol.CMD_RENAME_FILE:
				error = service.renameFile(request.renameFile(), response.renameFile(), response);
				break;		
			case RpcProtocol.CMD_GET_BLOCK:
				error = service.getBlock(request.getBlock(), response.getBlock(), response);
				break;
			case RpcProtocol.CMD_GET_LOCATION:
				error = service.getLocation(request.getLocation(), response.getLocation(), response);
				break;				
			case RpcProtocol.CMD_SET_BLOCK:
				error = service.setBlock(request.setBlock(), response.getVoid(), response);
				break;
			case RpcProtocol.CMD_GET_DATANODE:
				error = service.getDataNode(request.getDataNode(), response.getDataNode(), response);
				break;					
			case RpcProtocol.CMD_DUMP_NAMENODE:
				error = service.dump(request.dumpNameNode(), response.getVoid(), response);
				break;			
			case RpcProtocol.CMD_PING_NAMENODE:
				error = service.ping(request.pingNameNode(), response.pingNameNode(), response);
				break;
			default:
				error = RpcErrors.ERR_INVALID_RPC_CMD;
				LOG.info("Rpc command not valid, opcode " + request.getCmd());
			}
		} catch(Exception e){
			error = RpcErrors.ERR_UNKNOWN;
			LOG.info(RpcErrors.messages[RpcErrors.ERR_UNKNOWN] + e.getMessage());
			e.printStackTrace();
		}
		
		try {
			response.setError(error);
		} catch(Exception e){
			LOG.info("ERROR: RPC failed, messagesSend ");
			e.printStackTrace();
		}
		
		return response;		
	}

	public void removeEndpoint(NaRPCServerChannel channel){
	}

	public void addEndpoint(NaRPCServerChannel channel){
	}
}
