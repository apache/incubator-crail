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

package org.apache.crail.rpc;

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
	
	@SuppressWarnings("unchecked")
	public static RpcNameNodeService createInstance(String name) throws Exception {
		Class<?> serviceClass = Class.forName(name);
		if (RpcNameNodeService.class.isAssignableFrom(serviceClass)){
			Class<? extends RpcNameNodeService> rpcService = (Class<? extends RpcNameNodeService>) serviceClass;
			RpcNameNodeService service = rpcService.newInstance();
			return service;
		} else {
			throw new Exception("Cannot instantiate RPC service of type " + name);
		}
	}	
}
