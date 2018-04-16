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
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.crail.CrailNodeType;
import org.apache.crail.metadata.BlockInfo;
import org.apache.crail.metadata.DataNodeInfo;
import org.apache.crail.metadata.FileInfo;
import org.apache.crail.metadata.FileName;
import org.apache.crail.utils.CrailUtils;
import org.slf4j.Logger;

public class RpcDispatcher implements RpcConnection {
	private static final Logger LOG = CrailUtils.getLogger();
	
	private RpcConnection[] connections;
	private int setBlockIndex;
	private int getDataNodeIndex;

	public RpcDispatcher(ConcurrentLinkedQueue<RpcConnection> connectionList) {
		connections = new RpcConnection[connectionList.size()];
		for (int i = 0; i < connections.length; i++){
			connections[i] = connectionList.poll();
		}
		this.setBlockIndex = 0;
		this.getDataNodeIndex = 0;
	}

	@Override
	public RpcFuture<RpcCreateFile> createFile(FileName filename,
			CrailNodeType type, int storageClass, int locationClass, boolean enumerable)
			throws IOException {
		int index = computeIndex(filename.getComponent(0));
//		LOG.info("issuing create file for filename [" + filename.toString() + "], on index " + index);
		return connections[index].createFile(filename, type, storageClass, locationClass, enumerable);
	}

	@Override
	public RpcFuture<RpcGetFile> getFile(FileName filename, boolean writeable)
			throws IOException {
		int index = computeIndex(filename.getComponent(0));
//		LOG.info("issuing get file for filename [" + filename.toString() + "], on index " + index);
		return connections[index].getFile(filename, writeable);
	}

	@Override
	public RpcFuture<RpcVoid> setFile(FileInfo fileInfo, boolean close)
			throws IOException {
		int index = computeIndex(fileInfo.getFd());
//		LOG.info("issuing set file for fd [" + fileInfo.getFd() + "], on index " + index);
		return connections[index].setFile(fileInfo, close);
	}

	@Override
	public RpcFuture<RpcDeleteFile> removeFile(FileName filename,
			boolean recursive) throws IOException {
		int index = computeIndex(filename.getComponent(0));
//		LOG.info("issuing remove file for filename [" + filename.toString() + "], on index " + index);		
		return connections[index].removeFile(filename, recursive);
	}

	@Override
	public RpcFuture<RpcRenameFile> renameFile(FileName srcHash,
			FileName dstHash) throws IOException {
		int srcIndex = computeIndex(srcHash.getComponent(0));
		int dstIndex = computeIndex(srcHash.getComponent(0));
//		LOG.info("issuing remove file for src [" + srcHash.toString() + "," + dstHash.toString() + "], on index " + srcIndex);	
		if (srcIndex != dstIndex){
			throw new IOException("Rename not supported across namenode domains");
		} else {
			return connections[srcIndex].renameFile(srcHash, dstHash);
		}
	}

	@Override
	public RpcFuture<RpcGetBlock> getBlock(long fd, long token, long position,
			long capacity) throws IOException {
		int index = computeIndex(fd);
//		LOG.info("issuing get block for fd [" + fd + "], on index " + index);		
		return connections[index].getBlock(fd, token, position, capacity);
	}

	@Override
	public RpcFuture<RpcGetLocation> getLocation(FileName fileName,
			long position) throws IOException {
		int index = computeIndex(fileName.getComponent(0));
//		LOG.info("issuing get location for filename [" + fileName.toString() + "], on index " + index);			
		return connections[index].getLocation(fileName, position);
	}

	@Override
	public RpcFuture<RpcVoid> setBlock(BlockInfo blockInfo) throws Exception {
//		LOG.info("issuing set block on index " + setBlockIndex);
		RpcFuture<RpcVoid> res = connections[setBlockIndex].setBlock(blockInfo);
		setBlockIndex = (setBlockIndex + 1) % connections.length;
		return res;
	}

	@Override
	public RpcFuture<RpcGetDataNode> getDataNode(DataNodeInfo dnInfo)
			throws Exception {
//		LOG.info("issuing get datanode on index " + getDataNodeIndex);
		RpcFuture<RpcGetDataNode> res = connections[getDataNodeIndex].getDataNode(dnInfo);
		getDataNodeIndex = (getDataNodeIndex + 1) % connections.length;
		return res;
	}

	@Override
	public RpcFuture<RpcVoid> dumpNameNode() throws Exception {
		return connections[0].dumpNameNode();
	}

	@Override
	public RpcFuture<RpcPing> pingNameNode() throws Exception {
		return connections[0].pingNameNode();
	}

	@Override
	public void close() throws Exception {
		for (RpcConnection connection : connections){
			connection.close();
		}
	}

	@Override
	public String toString() {
		String address = "";
		for (RpcConnection connection : connections){
			address = address + ", " + connection.toString();
		}
		
		return address;
	}

	private int computeIndex(int component) {
		int index = ((component % connections.length) + connections.length) % connections.length;
		return index;
	}
	
	private int computeIndex(long component) {
		long connectionsLength = (long) connections.length;
		long _index = ((component % connectionsLength) + connectionsLength) % connectionsLength;
		int index = (int) _index;
		return index;
	}	
}
