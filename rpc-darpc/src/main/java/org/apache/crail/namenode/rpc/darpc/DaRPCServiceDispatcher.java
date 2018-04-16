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

import java.util.concurrent.atomic.AtomicLong;
import java.io.IOException;

import org.apache.crail.rpc.RpcErrors;
import org.apache.crail.rpc.RpcNameNodeService;
import org.apache.crail.rpc.RpcNameNodeState;
import org.apache.crail.rpc.RpcProtocol;
import org.apache.crail.rpc.RpcRequestMessage;
import org.apache.crail.rpc.RpcResponseMessage;
import org.apache.crail.utils.CrailUtils;
import org.slf4j.Logger;

import com.ibm.darpc.DaRPCServerEndpoint;
import com.ibm.darpc.DaRPCServerEvent;
import com.ibm.darpc.DaRPCService;

public class DaRPCServiceDispatcher extends DaRPCNameNodeProtocol implements DaRPCService<DaRPCNameNodeRequest, DaRPCNameNodeResponse> {
	private static final Logger LOG = CrailUtils.getLogger();
	
	private RpcNameNodeService service;
	
	private AtomicLong totalOps;
	private AtomicLong createOps;
	private AtomicLong lookupOps;
	private AtomicLong setOps;
	private AtomicLong removeOps;
	private AtomicLong renameOps;
	private AtomicLong getOps;
	private AtomicLong locationOps;
	private AtomicLong errorOps;
	
	public DaRPCServiceDispatcher(RpcNameNodeService service){
		this.service = service;
		
		this.totalOps = new AtomicLong(0);
		this.createOps = new AtomicLong(0);
		this.lookupOps = new AtomicLong(0);
		this.setOps = new AtomicLong(0);
		this.removeOps = new AtomicLong(0);
		this.renameOps = new AtomicLong(0);
		this.getOps = new AtomicLong(0);
		this.locationOps = new AtomicLong(0);
		this.errorOps = new AtomicLong(0);
	}
	
	public void processServerEvent(DaRPCServerEvent<DaRPCNameNodeRequest, DaRPCNameNodeResponse> event) {
		DaRPCNameNodeRequest request = event.getReceiveMessage();
		DaRPCNameNodeResponse response = event.getSendMessage();
		short error = RpcErrors.ERR_OK;
		try {
			response.setType(RpcProtocol.responseTypes[request.getCmd()]);
			response.setError((short) 0);
			switch(request.getCmd()) {
			case RpcProtocol.CMD_CREATE_FILE:
				this.totalOps.incrementAndGet();
				this.createOps.incrementAndGet();
				error = service.createFile(request.createFile(), response.createFile(), response);
				break;			
			case RpcProtocol.CMD_GET_FILE:
				this.totalOps.incrementAndGet();
				this.lookupOps.incrementAndGet();
				error = service.getFile(request.getFile(), response.getFile(), response);
				break;
			case RpcProtocol.CMD_SET_FILE:
				this.totalOps.incrementAndGet();
				this.setOps.incrementAndGet();
				error = service.setFile(request.setFile(), response.getVoid(), response);
				break;
			case RpcProtocol.CMD_REMOVE_FILE:
				this.totalOps.incrementAndGet();
				this.removeOps.incrementAndGet();
				error = service.removeFile(request.removeFile(), response.delFile(), response);
				break;				
			case RpcProtocol.CMD_RENAME_FILE:
				this.totalOps.incrementAndGet();
				this.renameOps.incrementAndGet();
				error = service.renameFile(request.renameFile(), response.getRename(), response);
				break;		
			case RpcProtocol.CMD_GET_BLOCK:
				this.totalOps.incrementAndGet();
				this.getOps.incrementAndGet();
				error = service.getBlock(request.getBlock(), response.getBlock(), response);
				break;
			case RpcProtocol.CMD_GET_LOCATION:
				this.totalOps.incrementAndGet();
				this.locationOps.incrementAndGet();
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
				error = this.stats(request.pingNameNode(), response.pingNameNode(), response);
				error = service.ping(request.pingNameNode(), response.pingNameNode(), response);
				break;
			default:
				error = RpcErrors.ERR_INVALID_RPC_CMD;
				LOG.info("Rpc command not valid, opcode " + request.getCmd());
			}
		} catch(Exception e){
			error = RpcErrors.ERR_UNKNOWN;
			this.errorOps.incrementAndGet();
			LOG.info(RpcErrors.messages[RpcErrors.ERR_UNKNOWN] + e.getMessage());
			e.printStackTrace();
		}
		
		try {
			response.setError(error);
			event.triggerResponse();
		} catch(Exception e){
			LOG.info("ERROR: RPC failed, messagesSend ");
			e.printStackTrace();
		}
	}
	
	public short stats(RpcRequestMessage.PingNameNodeReq request, RpcResponseMessage.PingNameNodeRes response, RpcNameNodeState errorState) throws Exception {
		if (!RpcProtocol.verifyProtocol(RpcProtocol.CMD_PING_NAMENODE, request, response)){
			return RpcErrors.ERR_PROTOCOL_MISMATCH;
		}			
		
		LOG.info("totalOps " + totalOps.get());
		LOG.info("errorOps " + errorOps.get());
		LOG.info("createOps " + createOps.get());
		LOG.info("lookupOps " + lookupOps.get());
		LOG.info("setOps " + setOps.get());
		LOG.info("removeOps " + removeOps.get());
		LOG.info("renameOps " + renameOps.get());
		LOG.info("getOps " + getOps.get());
		LOG.info("locationOps " + locationOps.get());
		
		return RpcErrors.ERR_OK;
	}	
	
	@Override
	public void open(DaRPCServerEndpoint<DaRPCNameNodeRequest, DaRPCNameNodeResponse> endpoint) {
		try {
			LOG.info("RPC connection, qpnum " + endpoint.getQp().getQp_num());
		} catch(IOException e) {
			LOG.info("RPC connection, cannot get qpnum, because QP is not open.\n");
		}
	}	

	@Override
	public void close(DaRPCServerEndpoint<DaRPCNameNodeRequest, DaRPCNameNodeResponse> endpoint) {
		try {
			LOG.info("disconnecting RPC connection, qpnum " + endpoint.getQp().getQp_num());
			endpoint.close();
		} catch(Exception e){
		}
	}	
}
