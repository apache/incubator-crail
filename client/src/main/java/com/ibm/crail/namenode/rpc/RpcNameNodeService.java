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

package com.ibm.crail.namenode.rpc;

public interface RpcNameNodeService {
	public abstract short createFile(RpcRequestMessage.CreateFileReq request,
			RpcResponseMessage.CreateFileRes response, RpcNameNodeState errorState)
			throws Exception;

	public abstract short getFile(RpcRequestMessage.GetFileReq request,
			RpcResponseMessage.GetFileRes response, RpcNameNodeState errorState)
			throws Exception;

	public abstract short setFile(RpcRequestMessage.SetFileReq request,
			RpcResponseMessage.VoidRes response, RpcNameNodeState errorState)
			throws Exception;

	public abstract short removeFile(RpcRequestMessage.RemoveFileReq request,
			RpcResponseMessage.DeleteFileRes response, RpcNameNodeState errorState)
			throws Exception;

	public abstract short renameFile(
			RpcRequestMessage.RenameFileReq request,
			RpcResponseMessage.RenameRes response, RpcNameNodeState errorState)
			throws Exception;

	public abstract short getDataNode(
			RpcRequestMessage.GetDataNodeReq request,
			RpcResponseMessage.GetDataNodeRes response, RpcNameNodeState errorState)
			throws Exception;

	public abstract short setBlock(RpcRequestMessage.SetBlockReq request,
			RpcResponseMessage.VoidRes response, RpcNameNodeState errorState)
			throws Exception;

	public abstract short getBlock(RpcRequestMessage.GetBlockReq request,
			RpcResponseMessage.GetBlockRes response, RpcNameNodeState errorState)
			throws Exception;

	public abstract short getLocation(RpcRequestMessage.GetLocationReq request,
			RpcResponseMessage.GetLocationRes response, RpcNameNodeState errorState)
			throws Exception;

	public abstract short dump(RpcRequestMessage.DumpNameNodeReq request,
			RpcResponseMessage.VoidRes response, RpcNameNodeState errorState)
			throws Exception;

	public abstract short ping(RpcRequestMessage.PingNameNodeReq request,
			RpcResponseMessage.PingNameNodeRes response, RpcNameNodeState errorState)
			throws Exception;
}

