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

import java.io.IOException;

import com.ibm.crail.CrailNodeType;
import com.ibm.crail.namenode.protocol.BlockInfo;
import com.ibm.crail.namenode.protocol.DataNodeInfo;
import com.ibm.crail.namenode.protocol.FileInfo;
import com.ibm.crail.namenode.protocol.FileName;

public interface RpcNameNodeClient {
	public abstract RpcNameNodeFuture<RpcResponseMessage.CreateFileRes> createFile(
			FileName filename, CrailNodeType type, int storageAffinity, int locationAffinity) throws IOException;

	public abstract RpcNameNodeFuture<RpcResponseMessage.GetFileRes> getFile(FileName filename,
			boolean writeable) throws IOException;

	public abstract RpcNameNodeFuture<RpcResponseMessage.VoidRes> setFile(FileInfo fileInfo,
			boolean close) throws IOException;

	public abstract RpcNameNodeFuture<RpcResponseMessage.DeleteFileRes> removeFile(
			FileName filename, boolean recursive) throws IOException;

	public abstract RpcNameNodeFuture<RpcResponseMessage.RenameRes> renameFile(
			FileName srcHash, FileName dstHash) throws IOException;

	public abstract RpcNameNodeFuture<RpcResponseMessage.GetBlockRes> getBlock(long fd,
			long token, long position, int storageAffinity, int locationAffinity, long capacity) throws IOException;

	public abstract RpcNameNodeFuture<RpcResponseMessage.GetLocationRes> getLocation(
			FileName fileName, long position) throws IOException;

	public abstract RpcNameNodeFuture<RpcResponseMessage.VoidRes> setBlock(BlockInfo blockInfo)
			throws Exception;

	public abstract RpcNameNodeFuture<RpcResponseMessage.GetDataNodeRes> getDataNode(
			DataNodeInfo dnInfo) throws Exception;

	public abstract RpcNameNodeFuture<RpcResponseMessage.VoidRes> dumpNameNode()
			throws Exception;

	public abstract RpcNameNodeFuture<RpcResponseMessage.PingNameNodeRes> pingNameNode()
			throws Exception;
}

