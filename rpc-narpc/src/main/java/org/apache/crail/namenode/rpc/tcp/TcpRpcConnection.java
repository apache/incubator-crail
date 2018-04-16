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

import com.ibm.narpc.NaRPCEndpoint;
import com.ibm.narpc.NaRPCFuture;

import org.apache.crail.CrailNodeType;
import org.apache.crail.metadata.BlockInfo;
import org.apache.crail.metadata.DataNodeInfo;
import org.apache.crail.metadata.FileInfo;
import org.apache.crail.metadata.FileName;
import org.apache.crail.rpc.*;
import org.apache.crail.utils.CrailUtils;
import org.slf4j.Logger;

import java.io.IOException;

public class TcpRpcConnection implements RpcConnection {
	static private final Logger LOG = CrailUtils.getLogger();
	private NaRPCEndpoint<TcpNameNodeRequest, TcpNameNodeResponse> endpoint;

	public TcpRpcConnection(
			NaRPCEndpoint<TcpNameNodeRequest, TcpNameNodeResponse> endpoint) {
		this.endpoint = endpoint;
	}

	public String toString() {
		String address = "";
		try {
			address = endpoint.address();
		} catch (IOException e) {
		}
		return address;
	}

	public void close() throws IOException {
		this.endpoint.close();
	}

	public RpcFuture<RpcCreateFile> createFile(FileName fileName,
			CrailNodeType type, int storageAffinity, int locationAffinity, boolean enumerable)
			throws IOException {
		RpcRequestMessage.CreateFileReq req = new RpcRequestMessage.CreateFileReq(fileName, type, storageAffinity, locationAffinity, enumerable);
		RpcResponseMessage.CreateFileRes resp = new RpcResponseMessage.CreateFileRes();

		TcpNameNodeRequest request = new TcpNameNodeRequest(req);
		TcpNameNodeResponse response = new TcpNameNodeResponse(resp);
		request.setCommand(RpcProtocol.CMD_CREATE_FILE);
		NaRPCFuture<TcpNameNodeRequest, TcpNameNodeResponse> future = endpoint.issueRequest(request, response);
		return new TcpFuture<RpcCreateFile>(future, resp);
	}

	public RpcFuture<RpcGetFile> getFile(FileName fileName, boolean b)
			throws IOException {
		RpcRequestMessage.GetFileReq req = new RpcRequestMessage.GetFileReq(fileName, b);
		RpcResponseMessage.GetFileRes resp = new RpcResponseMessage.GetFileRes();

		TcpNameNodeRequest request = new TcpNameNodeRequest(req);
		TcpNameNodeResponse response = new TcpNameNodeResponse(resp);
		request.setCommand(RpcProtocol.CMD_GET_FILE);
		NaRPCFuture<TcpNameNodeRequest, TcpNameNodeResponse> future = endpoint.issueRequest(request, response);
		return new TcpFuture<RpcGetFile>(future, resp);
	}

	public RpcFuture<RpcVoid> setFile(FileInfo fileInfo, boolean b)
			throws IOException {
		RpcRequestMessage.SetFileReq req = new RpcRequestMessage.SetFileReq(fileInfo, b);
		RpcResponseMessage.VoidRes resp = new RpcResponseMessage.VoidRes();

		TcpNameNodeRequest request = new TcpNameNodeRequest(req);
		TcpNameNodeResponse response = new TcpNameNodeResponse(resp);
		request.setCommand(RpcProtocol.CMD_SET_FILE);
		NaRPCFuture<TcpNameNodeRequest, TcpNameNodeResponse> future = endpoint.issueRequest(request, response);
		return new TcpFuture<RpcVoid>(future, resp);
	}

	public RpcFuture<RpcDeleteFile> removeFile(FileName fileName, boolean b)
			throws IOException {
		RpcRequestMessage.RemoveFileReq req = new RpcRequestMessage.RemoveFileReq(fileName, b);
		RpcResponseMessage.DeleteFileRes resp = new RpcResponseMessage.DeleteFileRes();

		TcpNameNodeRequest request = new TcpNameNodeRequest(req);
		TcpNameNodeResponse response = new TcpNameNodeResponse(resp);
		request.setCommand(RpcProtocol.CMD_REMOVE_FILE);
		NaRPCFuture<TcpNameNodeRequest, TcpNameNodeResponse> future = endpoint.issueRequest(request, response);
		return new TcpFuture<RpcDeleteFile>(future, resp);
	}

	public RpcFuture<RpcRenameFile> renameFile(FileName fileName,
			FileName fileName1) throws IOException {
		RpcRequestMessage.RenameFileReq req = new RpcRequestMessage.RenameFileReq(fileName, fileName1);
		RpcResponseMessage.RenameRes resp = new RpcResponseMessage.RenameRes();

		TcpNameNodeRequest request = new TcpNameNodeRequest(req);
		TcpNameNodeResponse response = new TcpNameNodeResponse(resp);
		request.setCommand(RpcProtocol.CMD_RENAME_FILE);
		NaRPCFuture<TcpNameNodeRequest, TcpNameNodeResponse> future = endpoint.issueRequest(request, response);
		return new TcpFuture<RpcRenameFile>(future, resp);
	}

	public RpcFuture<RpcGetBlock> getBlock(long fd, long token, long position,
			long capacity) throws IOException {
		RpcRequestMessage.GetBlockReq req = new RpcRequestMessage.GetBlockReq(fd, token, position, capacity);
		RpcResponseMessage.GetBlockRes resp = new RpcResponseMessage.GetBlockRes();

		TcpNameNodeRequest request = new TcpNameNodeRequest(req);
		TcpNameNodeResponse response = new TcpNameNodeResponse(resp);
		request.setCommand(RpcProtocol.CMD_GET_BLOCK);
		NaRPCFuture<TcpNameNodeRequest, TcpNameNodeResponse> future = endpoint.issueRequest(request, response);
		return new TcpFuture<RpcGetBlock>(future, resp);
	}

	public RpcFuture<RpcGetLocation> getLocation(FileName fileName, long l)
			throws IOException {
		RpcRequestMessage.GetLocationReq req = new RpcRequestMessage.GetLocationReq(fileName, l);
		RpcResponseMessage.GetLocationRes resp = new RpcResponseMessage.GetLocationRes();

		TcpNameNodeRequest request = new TcpNameNodeRequest(req);
		TcpNameNodeResponse response = new TcpNameNodeResponse(resp);
		request.setCommand(RpcProtocol.CMD_GET_LOCATION);
		NaRPCFuture<TcpNameNodeRequest, TcpNameNodeResponse> future = endpoint.issueRequest(request, response);
		return new TcpFuture<RpcGetLocation>(future, resp);
	}

	public RpcFuture<RpcVoid> setBlock(BlockInfo blockInfo) throws Exception {
		RpcRequestMessage.SetBlockReq req = new RpcRequestMessage.SetBlockReq(blockInfo);
		RpcResponseMessage.VoidRes resp = new RpcResponseMessage.VoidRes();

		TcpNameNodeRequest request = new TcpNameNodeRequest(req);
		TcpNameNodeResponse response = new TcpNameNodeResponse(resp);
		request.setCommand(RpcProtocol.CMD_SET_BLOCK);
		NaRPCFuture<TcpNameNodeRequest, TcpNameNodeResponse> future = endpoint.issueRequest(request, response);
		return new TcpFuture<RpcVoid>(future, resp);
	}

	public RpcFuture<RpcGetDataNode> getDataNode(DataNodeInfo dataNodeInfo)
			throws Exception {
		RpcRequestMessage.GetDataNodeReq req = new RpcRequestMessage.GetDataNodeReq(dataNodeInfo);
		RpcResponseMessage.GetDataNodeRes resp = new RpcResponseMessage.GetDataNodeRes();

		TcpNameNodeRequest request = new TcpNameNodeRequest(req);
		TcpNameNodeResponse response = new TcpNameNodeResponse(resp);
		request.setCommand(RpcProtocol.CMD_GET_DATANODE);
		NaRPCFuture<TcpNameNodeRequest, TcpNameNodeResponse> future = endpoint.issueRequest(request, response);
		return new TcpFuture<RpcGetDataNode>(future, resp);
	}

	public RpcFuture<RpcVoid> dumpNameNode() throws Exception {
		RpcRequestMessage.DumpNameNodeReq req = new RpcRequestMessage.DumpNameNodeReq();
		RpcResponseMessage.VoidRes resp = new RpcResponseMessage.VoidRes();

		TcpNameNodeRequest request = new TcpNameNodeRequest(req);
		TcpNameNodeResponse response = new TcpNameNodeResponse(resp);
		request.setCommand(RpcProtocol.CMD_DUMP_NAMENODE);
		NaRPCFuture<TcpNameNodeRequest, TcpNameNodeResponse> future = endpoint.issueRequest(request, response);
		return new TcpFuture<RpcVoid>(future, resp);
	}

	public RpcFuture<RpcPing> pingNameNode() throws Exception {
		RpcRequestMessage.PingNameNodeReq req = new RpcRequestMessage.PingNameNodeReq();
		RpcResponseMessage.PingNameNodeRes resp = new RpcResponseMessage.PingNameNodeRes();

		TcpNameNodeRequest request = new TcpNameNodeRequest(req);
		TcpNameNodeResponse response = new TcpNameNodeResponse(resp);
		request.setCommand(RpcProtocol.CMD_PING_NAMENODE);
		NaRPCFuture<TcpNameNodeRequest, TcpNameNodeResponse> future = endpoint.issueRequest(request, response);
		return new TcpFuture<RpcPing>(future, resp);
	}

}