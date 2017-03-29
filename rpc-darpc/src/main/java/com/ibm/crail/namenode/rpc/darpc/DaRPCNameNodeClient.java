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

import java.io.IOException;

import org.slf4j.Logger;

import com.ibm.crail.CrailNodeType;
import com.ibm.crail.conf.CrailConstants;
import com.ibm.crail.namenode.protocol.BlockInfo;
import com.ibm.crail.namenode.protocol.DataNodeInfo;
import com.ibm.crail.namenode.protocol.FileInfo;
import com.ibm.crail.namenode.protocol.FileName;
import com.ibm.crail.namenode.rpc.NameNodeProtocol;
import com.ibm.crail.namenode.rpc.RpcNameNodeClient;
import com.ibm.crail.namenode.rpc.RpcNameNodeFuture;
import com.ibm.crail.namenode.rpc.RpcRequestMessage;
import com.ibm.crail.namenode.rpc.RpcResponseMessage;
import com.ibm.crail.utils.CrailUtils;
import com.ibm.darpc.RpcClientEndpoint;
import com.ibm.darpc.RpcFuture;
import com.ibm.darpc.RpcStream;

public class DaRPCNameNodeClient implements RpcNameNodeClient {
	private static final Logger LOG = CrailUtils.getLogger();
	
	private RpcClientEndpoint<DaRPCNameNodeRequest, DaRPCNameNodeResponse> rpcEndpoint;
	private RpcStream<DaRPCNameNodeRequest, DaRPCNameNodeResponse> stream;
	
	public DaRPCNameNodeClient(RpcClientEndpoint<DaRPCNameNodeRequest, DaRPCNameNodeResponse> endpoint) throws IOException {
		this.rpcEndpoint = endpoint;
		this.stream = endpoint.createStream();
	}	
	
	@Override
	public RpcNameNodeFuture<RpcResponseMessage.CreateFileRes> createFile(FileName filename, CrailNodeType type, int storageAffinity, int locationAffinity) throws IOException {
		if (CrailConstants.DEBUG){
			LOG.debug("RPC: createFile, fileType " + type + ", affinity " + locationAffinity);
		}
		
		RpcRequestMessage.CreateFileReq createFileReq = new RpcRequestMessage.CreateFileReq(filename, type, storageAffinity, locationAffinity);
		DaRPCNameNodeRequest request = new DaRPCNameNodeRequest(createFileReq);
		request.setCommand(NameNodeProtocol.CMD_CREATE_FILE);
		
		RpcResponseMessage.CreateFileRes fileRes = new RpcResponseMessage.CreateFileRes();
		DaRPCNameNodeResponse response = new DaRPCNameNodeResponse(fileRes);
		
		RpcFuture<DaRPCNameNodeRequest, DaRPCNameNodeResponse> future = issueRPC(request, response);
		
		DaRPCNameNodeFuture<RpcResponseMessage.CreateFileRes> nameNodeFuture = new DaRPCNameNodeFuture<RpcResponseMessage.CreateFileRes>(future, fileRes);
		
		return nameNodeFuture;
	}
	
	@Override
	public RpcNameNodeFuture<RpcResponseMessage.GetFileRes> getFile(FileName filename, boolean writeable) throws IOException {
		if (CrailConstants.DEBUG){
			LOG.debug("RPC: getFile, writeable " + writeable);
		}
		
		RpcRequestMessage.GetFileReq getFileReq = new RpcRequestMessage.GetFileReq(filename, writeable);
		DaRPCNameNodeRequest request = new DaRPCNameNodeRequest(getFileReq);
		request.setCommand(NameNodeProtocol.CMD_GET_FILE);

		RpcResponseMessage.GetFileRes fileRes = new RpcResponseMessage.GetFileRes();
		DaRPCNameNodeResponse response = new DaRPCNameNodeResponse(fileRes);
		
		RpcFuture<DaRPCNameNodeRequest, DaRPCNameNodeResponse> future = issueRPC(request, response);
		
		DaRPCNameNodeFuture<RpcResponseMessage.GetFileRes> nameNodeFuture = new DaRPCNameNodeFuture<RpcResponseMessage.GetFileRes>(future, fileRes);
		
		return nameNodeFuture;
	}
	
	@Override
	public DaRPCNameNodeFuture<RpcResponseMessage.VoidRes> setFile(FileInfo fileInfo, boolean close) throws IOException {
		if (CrailConstants.DEBUG){
			LOG.debug("RPC: setFile, id " + fileInfo.getFd() + ", close " + close);
		}
		
		RpcRequestMessage.SetFileReq setFileReq = new RpcRequestMessage.SetFileReq(fileInfo, close);
		DaRPCNameNodeRequest request = new DaRPCNameNodeRequest(setFileReq);
		request.setCommand(NameNodeProtocol.CMD_SET_FILE);
		
		RpcResponseMessage.VoidRes voidRes = new RpcResponseMessage.VoidRes();
		DaRPCNameNodeResponse response = new DaRPCNameNodeResponse(voidRes);
		
		RpcFuture<DaRPCNameNodeRequest, DaRPCNameNodeResponse> future = issueRPC(request, response);
		
		DaRPCNameNodeFuture<RpcResponseMessage.VoidRes> nameNodeFuture = new DaRPCNameNodeFuture<RpcResponseMessage.VoidRes>(future, voidRes);
		
		return nameNodeFuture;		
	}
	
	@Override
	public DaRPCNameNodeFuture<RpcResponseMessage.DeleteFileRes> removeFile(FileName filename, boolean recursive) throws IOException {
		if (CrailConstants.DEBUG){
			LOG.debug("RPC: removeFile");
		}
		
		RpcRequestMessage.RemoveFileReq removeReq = new RpcRequestMessage.RemoveFileReq(filename, recursive);
		DaRPCNameNodeRequest request = new DaRPCNameNodeRequest(removeReq);
		request.setCommand(NameNodeProtocol.CMD_REMOVE_FILE);
		
		RpcResponseMessage.DeleteFileRes fileRes = new RpcResponseMessage.DeleteFileRes();
		DaRPCNameNodeResponse response = new DaRPCNameNodeResponse(fileRes);
		
		RpcFuture<DaRPCNameNodeRequest, DaRPCNameNodeResponse> future = issueRPC(request, response);
		
		DaRPCNameNodeFuture<RpcResponseMessage.DeleteFileRes> nameNodeFuture = new DaRPCNameNodeFuture<RpcResponseMessage.DeleteFileRes>(future, fileRes);
		
		return nameNodeFuture;			
	}
	
	@Override
	public DaRPCNameNodeFuture<RpcResponseMessage.RenameRes> renameFile(FileName srcHash, FileName dstHash) throws IOException {
		if (CrailConstants.DEBUG){
			LOG.debug("RPC: renameFile");
		}
		
		RpcRequestMessage.RenameFileReq renameReq = new RpcRequestMessage.RenameFileReq(srcHash, dstHash);
		DaRPCNameNodeRequest request = new DaRPCNameNodeRequest(renameReq);
		request.setCommand(NameNodeProtocol.CMD_RENAME_FILE);
		
		RpcResponseMessage.RenameRes renameRes = new RpcResponseMessage.RenameRes();
		DaRPCNameNodeResponse response = new DaRPCNameNodeResponse(renameRes);
		
		RpcFuture<DaRPCNameNodeRequest, DaRPCNameNodeResponse> future = issueRPC(request, response);
		
		DaRPCNameNodeFuture<RpcResponseMessage.RenameRes> nameNodeFuture = new DaRPCNameNodeFuture<RpcResponseMessage.RenameRes>(future, renameRes);
		
		return nameNodeFuture;	
	}
	
	@Override
	public DaRPCNameNodeFuture<RpcResponseMessage.GetBlockRes> getBlock(long fd, long token, long position, int storageAffinity, int locationAffinity, long capacity) throws IOException {
		if (CrailConstants.DEBUG){
			LOG.debug("RPC: getBlock, fd " + fd + ", token " + token + ", position " + position + ", capacity " + capacity);
		}
		
		RpcRequestMessage.GetBlockReq getBlockReq = new RpcRequestMessage.GetBlockReq(fd, token, position, storageAffinity, locationAffinity, capacity);
		DaRPCNameNodeRequest request = new DaRPCNameNodeRequest(getBlockReq);
		request.setCommand(NameNodeProtocol.CMD_GET_BLOCK);
		
		RpcResponseMessage.GetBlockRes getBlockRes = new RpcResponseMessage.GetBlockRes();
		DaRPCNameNodeResponse response = new DaRPCNameNodeResponse(getBlockRes);
		
		RpcFuture<DaRPCNameNodeRequest, DaRPCNameNodeResponse> future = issueRPC(request, response);
		
		DaRPCNameNodeFuture<RpcResponseMessage.GetBlockRes> nameNodeFuture = new DaRPCNameNodeFuture<RpcResponseMessage.GetBlockRes>(future, getBlockRes);
		
		return nameNodeFuture;	
	}
	
	@Override
	public DaRPCNameNodeFuture<RpcResponseMessage.GetLocationRes> getLocation(FileName fileName, long position) throws IOException {
		if (CrailConstants.DEBUG){
			LOG.debug("RPC: getLocation, position " + position);
		}		
		
		RpcRequestMessage.GetLocationReq getLocationReq = new RpcRequestMessage.GetLocationReq(fileName, position);
		DaRPCNameNodeRequest request = new DaRPCNameNodeRequest(getLocationReq);
		request.setCommand(NameNodeProtocol.CMD_GET_LOCATION);

		RpcResponseMessage.GetLocationRes getLocationRes = new RpcResponseMessage.GetLocationRes();
		DaRPCNameNodeResponse response = new DaRPCNameNodeResponse(getLocationRes);
		
		RpcFuture<DaRPCNameNodeRequest, DaRPCNameNodeResponse> future = issueRPC(request, response);
		
		DaRPCNameNodeFuture<RpcResponseMessage.GetLocationRes> nameNodeFuture = new DaRPCNameNodeFuture<RpcResponseMessage.GetLocationRes>(future, getLocationRes);
		
		return nameNodeFuture;			
	}	
	
	@Override
	public DaRPCNameNodeFuture<RpcResponseMessage.VoidRes> setBlock(BlockInfo blockInfo) throws Exception {
		if (CrailConstants.DEBUG){
			LOG.debug("RPC: setBlock, ");
		}		
		
		RpcRequestMessage.SetBlockReq setBlockReq = new RpcRequestMessage.SetBlockReq(blockInfo);
		DaRPCNameNodeRequest request = new DaRPCNameNodeRequest(setBlockReq);
		request.setCommand(NameNodeProtocol.CMD_SET_BLOCK);
		
		RpcResponseMessage.VoidRes voidRes = new RpcResponseMessage.VoidRes();
		DaRPCNameNodeResponse response = new DaRPCNameNodeResponse(voidRes);
		
		RpcFuture<DaRPCNameNodeRequest, DaRPCNameNodeResponse> future = issueRPC(request, response);
		
		DaRPCNameNodeFuture<RpcResponseMessage.VoidRes> nameNodeFuture = new DaRPCNameNodeFuture<RpcResponseMessage.VoidRes>(future, voidRes);
		
		return nameNodeFuture;	
	}
	
	@Override
	public DaRPCNameNodeFuture<RpcResponseMessage.GetDataNodeRes> getDataNode(DataNodeInfo dnInfo) throws Exception {
		RpcRequestMessage.GetDataNodeReq getDataNodeReq = new RpcRequestMessage.GetDataNodeReq(dnInfo);
		DaRPCNameNodeRequest request = new DaRPCNameNodeRequest(getDataNodeReq);
		request.setCommand(NameNodeProtocol.CMD_GET_DATANODE);
		
		RpcResponseMessage.GetDataNodeRes getDataNodeRes = new RpcResponseMessage.GetDataNodeRes();
		DaRPCNameNodeResponse response = new DaRPCNameNodeResponse(getDataNodeRes);
		
		RpcFuture<DaRPCNameNodeRequest, DaRPCNameNodeResponse> future = issueRPC(request, response);
		
		DaRPCNameNodeFuture<RpcResponseMessage.GetDataNodeRes> nameNodeFuture = new DaRPCNameNodeFuture<RpcResponseMessage.GetDataNodeRes>(future, getDataNodeRes);
		
		return nameNodeFuture;	
	}	
	
	@Override
	public DaRPCNameNodeFuture<RpcResponseMessage.VoidRes> dumpNameNode() throws Exception {
		
		
		RpcRequestMessage.DumpNameNodeReq dumpNameNodeReq = new RpcRequestMessage.DumpNameNodeReq();
		DaRPCNameNodeRequest request = new DaRPCNameNodeRequest(dumpNameNodeReq);
		request.setCommand(NameNodeProtocol.CMD_DUMP_NAMENODE);

		RpcResponseMessage.VoidRes voidRes = new RpcResponseMessage.VoidRes();
		DaRPCNameNodeResponse response = new DaRPCNameNodeResponse(voidRes);	
		
		RpcFuture<DaRPCNameNodeRequest, DaRPCNameNodeResponse> future = issueRPC(request, response);
		
		DaRPCNameNodeFuture<RpcResponseMessage.VoidRes> nameNodeFuture = new DaRPCNameNodeFuture<RpcResponseMessage.VoidRes>(future, voidRes);
		
		return nameNodeFuture;	
	}	
	
	@Override
	public DaRPCNameNodeFuture<RpcResponseMessage.PingNameNodeRes> pingNameNode() throws Exception {
		
		RpcRequestMessage.PingNameNodeReq pingReq = new RpcRequestMessage.PingNameNodeReq();
		DaRPCNameNodeRequest request = new DaRPCNameNodeRequest(pingReq);
		request.setCommand(NameNodeProtocol.CMD_PING_NAMENODE);

		RpcResponseMessage.PingNameNodeRes pingRes = new RpcResponseMessage.PingNameNodeRes();
		DaRPCNameNodeResponse response = new DaRPCNameNodeResponse(pingRes);
		
		RpcFuture<DaRPCNameNodeRequest, DaRPCNameNodeResponse> future = issueRPC(request, response);
		
		DaRPCNameNodeFuture<RpcResponseMessage.PingNameNodeRes> nameNodeFuture = new DaRPCNameNodeFuture<RpcResponseMessage.PingNameNodeRes>(future, pingRes);
		
		return nameNodeFuture;	
	}
	
	private RpcFuture<DaRPCNameNodeRequest, DaRPCNameNodeResponse> issueRPC(DaRPCNameNodeRequest request, DaRPCNameNodeResponse response) throws IOException{
		try {
			RpcFuture<DaRPCNameNodeRequest, DaRPCNameNodeResponse> future = stream.request(request, response, false);
			return future;
		} catch(IOException e){
			LOG.info("ERROR: RPC failed, messagesSend " + rpcEndpoint.getMessagesSent() + ", messagesReceived " + rpcEndpoint.getMessagesReceived() + ", isConnected " + rpcEndpoint.isConnected() + ", qpNum " + rpcEndpoint.getQp().getQp_num());
			throw e;
		}
	}
}
