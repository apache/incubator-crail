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

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.crail.rpc.RpcProtocol;
import org.apache.crail.rpc.RpcRequestMessage;
import org.apache.crail.utils.CrailUtils;
import org.slf4j.Logger;

import com.ibm.narpc.NaRPCMessage;

public class TcpNameNodeRequest extends RpcRequestMessage implements NaRPCMessage {
	public static final Logger LOG = CrailUtils.getLogger();
	public static final int CSIZE = 2*Short.BYTES + Math.max(RpcRequestMessage.SetFileReq.CSIZE, RpcRequestMessage.RenameFileReq.CSIZE);
	
	private short cmd;
	private short type;
	private RpcRequestMessage.CreateFileReq createFileReq;
	private RpcRequestMessage.GetFileReq fileReq;
	private RpcRequestMessage.SetFileReq setFileReq;
	private RpcRequestMessage.RemoveFileReq removeReq;
	private RpcRequestMessage.RenameFileReq renameFileReq;
	private RpcRequestMessage.GetBlockReq getBlockReq;
	private RpcRequestMessage.GetLocationReq getLocationReq;
	private RpcRequestMessage.SetBlockReq setBlockReq;
	private RpcRequestMessage.GetDataNodeReq getDataNodeReq;
	private RpcRequestMessage.DumpNameNodeReq dumpNameNodeReq;
	private RpcRequestMessage.PingNameNodeReq pingNameNodeReq;

	public TcpNameNodeRequest() {
		this.cmd = 0;
		this.type = 0;
		this.createFileReq = new RpcRequestMessage.CreateFileReq();
		this.fileReq = new RpcRequestMessage.GetFileReq();
		this.setFileReq = new RpcRequestMessage.SetFileReq();
		this.removeReq = new RpcRequestMessage.RemoveFileReq();
		this.renameFileReq = new RpcRequestMessage.RenameFileReq();
		this.getBlockReq = new RpcRequestMessage.GetBlockReq();
		this.getLocationReq = new RpcRequestMessage.GetLocationReq();
		this.setBlockReq = new RpcRequestMessage.SetBlockReq();
		this.dumpNameNodeReq = new RpcRequestMessage.DumpNameNodeReq();
		this.pingNameNodeReq = new RpcRequestMessage.PingNameNodeReq();
		this.getDataNodeReq = new RpcRequestMessage.GetDataNodeReq();
	}	
	
	public TcpNameNodeRequest(RpcRequestMessage.CreateFileReq message) {
		this.type = message.getType();
		this.createFileReq = message;
	}
	public TcpNameNodeRequest(RpcRequestMessage.GetFileReq message) {
		this.type = message.getType();
		this.fileReq = message;
	}
	
	public TcpNameNodeRequest(RpcRequestMessage.SetFileReq message) {
		this.type = message.getType();
		this.setFileReq = message;
	}
	
	public TcpNameNodeRequest(RpcRequestMessage.RemoveFileReq message) {
		this.type = message.getType();
		this.removeReq = message;
	}
	
	public TcpNameNodeRequest(RpcRequestMessage.RenameFileReq message) {
		this.type = message.getType();
		this.renameFileReq = message;
	}
	
	public TcpNameNodeRequest(RpcRequestMessage.GetBlockReq message) {
		this.type = message.getType();
		this.getBlockReq = message;
	}
	
	public TcpNameNodeRequest(RpcRequestMessage.GetLocationReq message) {
		this.type = message.getType();
		this.getLocationReq = message;
	}
	
	public TcpNameNodeRequest(RpcRequestMessage.SetBlockReq message) {
		this.type = message.getType();
		this.setBlockReq = message;
	}
	
	public TcpNameNodeRequest(RpcRequestMessage.GetDataNodeReq message) {
		this.type = message.getType();
		this.getDataNodeReq = message;
	}
	
	public TcpNameNodeRequest(RpcRequestMessage.DumpNameNodeReq message) {
		this.type = message.getType();
		this.dumpNameNodeReq = message;
	}
	
	public TcpNameNodeRequest(RpcRequestMessage.PingNameNodeReq message) {
		this.type = message.getType();
		this.pingNameNodeReq = message;
	}
	
	public void setCommand(short command) {
		this.cmd = command;
	}	

	public int size(){
		return CSIZE;
	}
	
	public int write(ByteBuffer buffer) throws IOException{
		buffer.putShort(cmd);
		buffer.putShort(type);
		
		int written = 4;
		switch(type){
		case RpcProtocol.REQ_CREATE_FILE:
			written += createFileReq.write(buffer);
			break;		
		case RpcProtocol.REQ_GET_FILE:
			written += fileReq.write(buffer);
			break;
		case RpcProtocol.REQ_SET_FILE:
			written += setFileReq.write(buffer);
			break;
		case RpcProtocol.REQ_REMOVE_FILE:
			written += removeReq.write(buffer);
			break;			
		case RpcProtocol.REQ_RENAME_FILE:
			written += renameFileReq.write(buffer);
			break;
		case RpcProtocol.REQ_GET_BLOCK:
			written += getBlockReq.write(buffer);
			break;
		case RpcProtocol.REQ_GET_LOCATION:
			written += getLocationReq.write(buffer);
			break;			
		case RpcProtocol.REQ_SET_BLOCK:
			written += setBlockReq.write(buffer);
			break;
		case RpcProtocol.REQ_GET_DATANODE:
			written += getDataNodeReq.write(buffer);
			break;				
		case RpcProtocol.REQ_DUMP_NAMENODE:
			written += dumpNameNodeReq.write(buffer);
			break;
		case RpcProtocol.REQ_PING_NAMENODE:
			written += pingNameNodeReq.write(buffer);
			break;
		}
		
		return written;
	}
	
	public void update(ByteBuffer buffer) throws IOException {
		this.cmd = buffer.getShort();
		this.type = buffer.getShort();
		
		switch(type){
		case RpcProtocol.REQ_CREATE_FILE:
			createFileReq.update(buffer);
			break;		
		case RpcProtocol.REQ_GET_FILE:
			fileReq.update(buffer);
			break;
		case RpcProtocol.REQ_SET_FILE:
			setFileReq.update(buffer);
			break;
		case RpcProtocol.REQ_REMOVE_FILE:
			removeReq.update(buffer);
			break;			
		case RpcProtocol.REQ_RENAME_FILE:
			renameFileReq.update(buffer);
			break;
		case RpcProtocol.REQ_GET_BLOCK:
			getBlockReq.update(buffer);
			break;
		case RpcProtocol.REQ_GET_LOCATION:
			getLocationReq.update(buffer);
			break;			
		case RpcProtocol.REQ_SET_BLOCK:
			setBlockReq.update(buffer);
			break;
		case RpcProtocol.REQ_GET_DATANODE:
			getDataNodeReq.update(buffer);
			break;				
		case RpcProtocol.REQ_DUMP_NAMENODE:
			dumpNameNodeReq.update(buffer);
			break;		
		case RpcProtocol.REQ_PING_NAMENODE:
			pingNameNodeReq.update(buffer);
			break;
		}
	}

	public short getCmd() {
		return cmd;
	}
	
	public short getType(){
		return type;
	}
	
	public RpcRequestMessage.CreateFileReq createFile(){
		return this.createFileReq;
	}
	
	public RpcRequestMessage.GetFileReq getFile(){
		return fileReq;
	}
	
	public RpcRequestMessage.SetFileReq setFile() {
		return setFileReq;
	}

	public RpcRequestMessage.RemoveFileReq removeFile(){
		return removeReq;
	}	

	public RpcRequestMessage.RenameFileReq renameFile(){
		return renameFileReq;
	}

	public RpcRequestMessage.GetBlockReq getBlock() {
		return getBlockReq;
	}
	
	public RpcRequestMessage.GetLocationReq getLocation() {
		return getLocationReq;
	}	

	public RpcRequestMessage.SetBlockReq setBlock() {
		return setBlockReq;
	}

	public RpcRequestMessage.GetDataNodeReq getDataNode() {
		return this.getDataNodeReq;
	}	
	
	public RpcRequestMessage.DumpNameNodeReq dumpNameNode() {
		return this.dumpNameNodeReq;
	}
	
	public RpcRequestMessage.PingNameNodeReq pingNameNode(){
		return this.pingNameNodeReq;
	}
}
