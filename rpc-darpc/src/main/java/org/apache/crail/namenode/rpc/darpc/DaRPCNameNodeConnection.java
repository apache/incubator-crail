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

import java.io.IOException;

import org.apache.crail.CrailNodeType;
import org.apache.crail.conf.CrailConstants;
import org.apache.crail.metadata.BlockInfo;
import org.apache.crail.metadata.DataNodeInfo;
import org.apache.crail.metadata.FileInfo;
import org.apache.crail.metadata.FileName;
import org.apache.crail.rpc.RpcConnection;
import org.apache.crail.rpc.RpcCreateFile;
import org.apache.crail.rpc.RpcDeleteFile;
import org.apache.crail.rpc.RpcFuture;
import org.apache.crail.rpc.RpcGetBlock;
import org.apache.crail.rpc.RpcGetDataNode;
import org.apache.crail.rpc.RpcGetFile;
import org.apache.crail.rpc.RpcGetLocation;
import org.apache.crail.rpc.RpcPing;
import org.apache.crail.rpc.RpcProtocol;
import org.apache.crail.rpc.RpcRenameFile;
import org.apache.crail.rpc.RpcRequestMessage;
import org.apache.crail.rpc.RpcResponseMessage;
import org.apache.crail.rpc.RpcVoid;
import org.apache.crail.utils.CrailUtils;
import org.slf4j.Logger;

import com.ibm.darpc.DaRPCClientEndpoint;
import com.ibm.darpc.DaRPCFuture;
import com.ibm.darpc.DaRPCStream;

public class DaRPCNameNodeConnection implements RpcConnection {
	private static final Logger LOG = CrailUtils.getLogger();
	
	private DaRPCClientEndpoint<DaRPCNameNodeRequest, DaRPCNameNodeResponse> rpcEndpoint;
	private DaRPCStream<DaRPCNameNodeRequest, DaRPCNameNodeResponse> stream;
	
	public DaRPCNameNodeConnection(DaRPCClientEndpoint<DaRPCNameNodeRequest, DaRPCNameNodeResponse> endpoint) throws IOException {
		this.rpcEndpoint = endpoint;
		this.stream = endpoint.createStream();
	}	
	
	@Override
	public RpcFuture<RpcCreateFile> createFile(FileName filename, CrailNodeType type, int storageClass, int locationClass, boolean enumerable) throws IOException {
		if (CrailConstants.DEBUG){
			LOG.debug("RPC: createFile, fileType " + type + ", storageClass " + storageClass + ", locationClass " + locationClass);
		}
		
		RpcRequestMessage.CreateFileReq createFileReq = new RpcRequestMessage.CreateFileReq(filename, type, storageClass, locationClass, enumerable);
		DaRPCNameNodeRequest request = new DaRPCNameNodeRequest(createFileReq);
		request.setCommand(RpcProtocol.CMD_CREATE_FILE);
		
		RpcResponseMessage.CreateFileRes fileRes = new RpcResponseMessage.CreateFileRes();
		DaRPCNameNodeResponse response = new DaRPCNameNodeResponse(fileRes);
		
		DaRPCFuture<DaRPCNameNodeRequest, DaRPCNameNodeResponse> future = issueRPC(request, response);
		
		DaRPCNameNodeFuture<RpcCreateFile> nameNodeFuture = new DaRPCNameNodeFuture<RpcCreateFile>(future, fileRes);
		
		return nameNodeFuture;
	}
	
	@Override
	public RpcFuture<RpcGetFile> getFile(FileName filename, boolean writeable) throws IOException {
		if (CrailConstants.DEBUG){
			LOG.debug("RPC: getFile, writeable " + writeable);
		}
		
		RpcRequestMessage.GetFileReq getFileReq = new RpcRequestMessage.GetFileReq(filename, writeable);
		DaRPCNameNodeRequest request = new DaRPCNameNodeRequest(getFileReq);
		request.setCommand(RpcProtocol.CMD_GET_FILE);

		RpcResponseMessage.GetFileRes fileRes = new RpcResponseMessage.GetFileRes();
		DaRPCNameNodeResponse response = new DaRPCNameNodeResponse(fileRes);
		
		DaRPCFuture<DaRPCNameNodeRequest, DaRPCNameNodeResponse> future = issueRPC(request, response);
		
		DaRPCNameNodeFuture<RpcGetFile> nameNodeFuture = new DaRPCNameNodeFuture<RpcGetFile>(future, fileRes);
		
		return nameNodeFuture;
	}
	
	@Override
	public DaRPCNameNodeFuture<RpcVoid> setFile(FileInfo fileInfo, boolean close) throws IOException {
		if (CrailConstants.DEBUG){
			LOG.debug("RPC: setFile, id " + fileInfo.getFd() + ", close " + close);
		}
		
		RpcRequestMessage.SetFileReq setFileReq = new RpcRequestMessage.SetFileReq(fileInfo, close);
		DaRPCNameNodeRequest request = new DaRPCNameNodeRequest(setFileReq);
		request.setCommand(RpcProtocol.CMD_SET_FILE);
		
		RpcResponseMessage.VoidRes voidRes = new RpcResponseMessage.VoidRes();
		DaRPCNameNodeResponse response = new DaRPCNameNodeResponse(voidRes);
		
		DaRPCFuture<DaRPCNameNodeRequest, DaRPCNameNodeResponse> future = issueRPC(request, response);
		
		DaRPCNameNodeFuture<RpcVoid> nameNodeFuture = new DaRPCNameNodeFuture<RpcVoid>(future, voidRes);
		
		return nameNodeFuture;		
	}
	
	@Override
	public DaRPCNameNodeFuture<RpcDeleteFile> removeFile(FileName filename, boolean recursive) throws IOException {
		if (CrailConstants.DEBUG){
			LOG.debug("RPC: removeFile");
		}
		
		RpcRequestMessage.RemoveFileReq removeReq = new RpcRequestMessage.RemoveFileReq(filename, recursive);
		DaRPCNameNodeRequest request = new DaRPCNameNodeRequest(removeReq);
		request.setCommand(RpcProtocol.CMD_REMOVE_FILE);
		
		RpcResponseMessage.DeleteFileRes fileRes = new RpcResponseMessage.DeleteFileRes();
		DaRPCNameNodeResponse response = new DaRPCNameNodeResponse(fileRes);
		
		DaRPCFuture<DaRPCNameNodeRequest, DaRPCNameNodeResponse> future = issueRPC(request, response);
		
		DaRPCNameNodeFuture<RpcDeleteFile> nameNodeFuture = new DaRPCNameNodeFuture<RpcDeleteFile>(future, fileRes);
		
		return nameNodeFuture;			
	}
	
	@Override
	public DaRPCNameNodeFuture<RpcRenameFile> renameFile(FileName srcHash, FileName dstHash) throws IOException {
		if (CrailConstants.DEBUG){
			LOG.debug("RPC: renameFile");
		}
		
		RpcRequestMessage.RenameFileReq renameReq = new RpcRequestMessage.RenameFileReq(srcHash, dstHash);
		DaRPCNameNodeRequest request = new DaRPCNameNodeRequest(renameReq);
		request.setCommand(RpcProtocol.CMD_RENAME_FILE);
		
		RpcResponseMessage.RenameRes renameRes = new RpcResponseMessage.RenameRes();
		DaRPCNameNodeResponse response = new DaRPCNameNodeResponse(renameRes);
		
		DaRPCFuture<DaRPCNameNodeRequest, DaRPCNameNodeResponse> future = issueRPC(request, response);
		
		DaRPCNameNodeFuture<RpcRenameFile> nameNodeFuture = new DaRPCNameNodeFuture<RpcRenameFile>(future, renameRes);
		
		return nameNodeFuture;	
	}
	
	@Override
	public DaRPCNameNodeFuture<RpcGetBlock> getBlock(long fd, long token, long position, long capacity) throws IOException {
		if (CrailConstants.DEBUG){
			LOG.debug("RPC: getBlock, fd " + fd + ", token " + token + ", position " + position + ", capacity " + capacity);
		}
		
		RpcRequestMessage.GetBlockReq getBlockReq = new RpcRequestMessage.GetBlockReq(fd, token, position, capacity);
		DaRPCNameNodeRequest request = new DaRPCNameNodeRequest(getBlockReq);
		request.setCommand(RpcProtocol.CMD_GET_BLOCK);
		
		RpcResponseMessage.GetBlockRes getBlockRes = new RpcResponseMessage.GetBlockRes();
		DaRPCNameNodeResponse response = new DaRPCNameNodeResponse(getBlockRes);
		
		DaRPCFuture<DaRPCNameNodeRequest, DaRPCNameNodeResponse> future = issueRPC(request, response);
		
		DaRPCNameNodeFuture<RpcGetBlock> nameNodeFuture = new DaRPCNameNodeFuture<RpcGetBlock>(future, getBlockRes);
		
		return nameNodeFuture;	
	}
	
	@Override
	public DaRPCNameNodeFuture<RpcGetLocation> getLocation(FileName fileName, long position) throws IOException {
		if (CrailConstants.DEBUG){
			LOG.debug("RPC: getLocation, position " + position);
		}		
		
		RpcRequestMessage.GetLocationReq getLocationReq = new RpcRequestMessage.GetLocationReq(fileName, position);
		DaRPCNameNodeRequest request = new DaRPCNameNodeRequest(getLocationReq);
		request.setCommand(RpcProtocol.CMD_GET_LOCATION);

		RpcResponseMessage.GetLocationRes getLocationRes = new RpcResponseMessage.GetLocationRes();
		DaRPCNameNodeResponse response = new DaRPCNameNodeResponse(getLocationRes);
		
		DaRPCFuture<DaRPCNameNodeRequest, DaRPCNameNodeResponse> future = issueRPC(request, response);
		
		DaRPCNameNodeFuture<RpcGetLocation> nameNodeFuture = new DaRPCNameNodeFuture<RpcGetLocation>(future, getLocationRes);
		
		return nameNodeFuture;			
	}	
	
	@Override
	public DaRPCNameNodeFuture<RpcVoid> setBlock(BlockInfo blockInfo) throws Exception {
		if (CrailConstants.DEBUG){
			LOG.debug("RPC: setBlock, ");
		}		
		
		RpcRequestMessage.SetBlockReq setBlockReq = new RpcRequestMessage.SetBlockReq(blockInfo);
		DaRPCNameNodeRequest request = new DaRPCNameNodeRequest(setBlockReq);
		request.setCommand(RpcProtocol.CMD_SET_BLOCK);
		
		RpcResponseMessage.VoidRes voidRes = new RpcResponseMessage.VoidRes();
		DaRPCNameNodeResponse response = new DaRPCNameNodeResponse(voidRes);
		
		DaRPCFuture<DaRPCNameNodeRequest, DaRPCNameNodeResponse> future = issueRPC(request, response);
		
		DaRPCNameNodeFuture<RpcVoid> nameNodeFuture = new DaRPCNameNodeFuture<RpcVoid>(future, voidRes);
		
		return nameNodeFuture;	
	}
	
	@Override
	public DaRPCNameNodeFuture<RpcGetDataNode> getDataNode(DataNodeInfo dnInfo) throws Exception {
		RpcRequestMessage.GetDataNodeReq getDataNodeReq = new RpcRequestMessage.GetDataNodeReq(dnInfo);
		DaRPCNameNodeRequest request = new DaRPCNameNodeRequest(getDataNodeReq);
		request.setCommand(RpcProtocol.CMD_GET_DATANODE);
		
		RpcResponseMessage.GetDataNodeRes getDataNodeRes = new RpcResponseMessage.GetDataNodeRes();
		DaRPCNameNodeResponse response = new DaRPCNameNodeResponse(getDataNodeRes);
		
		DaRPCFuture<DaRPCNameNodeRequest, DaRPCNameNodeResponse> future = issueRPC(request, response);
		
		DaRPCNameNodeFuture<RpcGetDataNode> nameNodeFuture = new DaRPCNameNodeFuture<RpcGetDataNode>(future, getDataNodeRes);
		
		return nameNodeFuture;	
	}	
	
	@Override
	public DaRPCNameNodeFuture<RpcVoid> dumpNameNode() throws Exception {
		
		
		RpcRequestMessage.DumpNameNodeReq dumpNameNodeReq = new RpcRequestMessage.DumpNameNodeReq();
		DaRPCNameNodeRequest request = new DaRPCNameNodeRequest(dumpNameNodeReq);
		request.setCommand(RpcProtocol.CMD_DUMP_NAMENODE);

		RpcResponseMessage.VoidRes voidRes = new RpcResponseMessage.VoidRes();
		DaRPCNameNodeResponse response = new DaRPCNameNodeResponse(voidRes);	
		
		DaRPCFuture<DaRPCNameNodeRequest, DaRPCNameNodeResponse> future = issueRPC(request, response);
		
		DaRPCNameNodeFuture<RpcVoid> nameNodeFuture = new DaRPCNameNodeFuture<RpcVoid>(future, voidRes);
		
		return nameNodeFuture;	
	}	
	
	@Override
	public DaRPCNameNodeFuture<RpcPing> pingNameNode() throws Exception {
		
		RpcRequestMessage.PingNameNodeReq pingReq = new RpcRequestMessage.PingNameNodeReq();
		DaRPCNameNodeRequest request = new DaRPCNameNodeRequest(pingReq);
		request.setCommand(RpcProtocol.CMD_PING_NAMENODE);

		RpcResponseMessage.PingNameNodeRes pingRes = new RpcResponseMessage.PingNameNodeRes();
		DaRPCNameNodeResponse response = new DaRPCNameNodeResponse(pingRes);
		
		DaRPCFuture<DaRPCNameNodeRequest, DaRPCNameNodeResponse> future = issueRPC(request, response);
		
		DaRPCNameNodeFuture<RpcPing> nameNodeFuture = new DaRPCNameNodeFuture<RpcPing>(future, pingRes);
		
		return nameNodeFuture;	
	}
	
	@Override
	public void close() throws Exception {
		if (rpcEndpoint != null){
			rpcEndpoint.close();
			rpcEndpoint = null;
		}		
	}

	private DaRPCFuture<DaRPCNameNodeRequest, DaRPCNameNodeResponse> issueRPC(DaRPCNameNodeRequest request, DaRPCNameNodeResponse response) throws IOException{
		try {
			DaRPCFuture<DaRPCNameNodeRequest, DaRPCNameNodeResponse> future = stream.request(request, response, false);
			return future;
		} catch(IOException e){
			LOG.info("ERROR: RPC failed, messagesSend " + rpcEndpoint.getMessagesSent() + ", messagesReceived " + rpcEndpoint.getMessagesReceived() + ", isConnected " + rpcEndpoint.isConnected() + ", qpNum " + rpcEndpoint.getQp().getQp_num());
			throw e;
		}
	}

	@Override
	public String toString() {
		try {
			return rpcEndpoint.getDstAddr().toString();
		} catch(Exception e){
			return "Unknown";
		}
	}
}
