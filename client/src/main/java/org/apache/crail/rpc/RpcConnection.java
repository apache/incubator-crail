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

import java.io.IOException;

import org.apache.crail.CrailNodeType;
import org.apache.crail.metadata.BlockInfo;
import org.apache.crail.metadata.DataNodeInfo;
import org.apache.crail.metadata.FileInfo;
import org.apache.crail.metadata.FileName;

public interface RpcConnection {
	public abstract RpcFuture<RpcCreateFile> createFile(
			FileName filename, CrailNodeType type, int storageClass, int locationClass, boolean enumerable) throws IOException;

	public abstract RpcFuture<RpcGetFile> getFile(FileName filename,
			boolean writeable) throws IOException;

	public abstract RpcFuture<RpcVoid> setFile(FileInfo fileInfo,
			boolean close) throws IOException;

	public abstract RpcFuture<RpcDeleteFile> removeFile(
			FileName filename, boolean recursive) throws IOException;

	public abstract RpcFuture<RpcRenameFile> renameFile(
			FileName srcHash, FileName dstHash) throws IOException;

	public abstract RpcFuture<RpcGetBlock> getBlock(long fd,
			long token, long position, long capacity) throws IOException;

	public abstract RpcFuture<RpcGetLocation> getLocation(
			FileName fileName, long position) throws IOException;

	public abstract RpcFuture<RpcVoid> setBlock(BlockInfo blockInfo)
			throws Exception;

	public abstract RpcFuture<RpcGetDataNode> getDataNode(
			DataNodeInfo dnInfo) throws Exception;

	public abstract RpcFuture<RpcVoid> dumpNameNode()
			throws Exception;

	public abstract RpcFuture<RpcPing> pingNameNode()
			throws Exception;
	
	public abstract void close() throws Exception;
	
	@SuppressWarnings("unchecked")
	public static RpcConnection createInstance(String name) throws Exception {
		Class<?> nodeClass = Class.forName(name);
		if (RpcConnection.class.isAssignableFrom(nodeClass)){
			Class<? extends RpcConnection> rpcClass = (Class<? extends RpcConnection>) nodeClass;
			RpcConnection rpcInstance = rpcClass.newInstance();
			return rpcInstance;
		} else {
			throw new Exception("Cannot instantiate RPC client of type " + name);
		}
	}		
}

