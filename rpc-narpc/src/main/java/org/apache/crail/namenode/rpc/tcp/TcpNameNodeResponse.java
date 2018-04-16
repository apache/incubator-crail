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

import java.nio.ByteBuffer;

import org.apache.crail.rpc.RpcNameNodeState;
import org.apache.crail.rpc.RpcProtocol;
import org.apache.crail.rpc.RpcResponseMessage;
import org.apache.crail.utils.CrailUtils;
import org.slf4j.Logger;

import com.ibm.narpc.NaRPCMessage;

public class TcpNameNodeResponse extends RpcResponseMessage implements RpcNameNodeState, NaRPCMessage {
	public static final Logger LOG = CrailUtils.getLogger();
	public static final int CSIZE = 2*Short.BYTES + Math.max(RpcResponseMessage.GetBlockRes.CSIZE, RpcResponseMessage.RenameRes.CSIZE);
	
	private short type;
	private short error;
	private RpcResponseMessage.VoidRes voidRes;
	private RpcResponseMessage.CreateFileRes createFileRes;
	private RpcResponseMessage.GetFileRes getFileRes;
	private RpcResponseMessage.DeleteFileRes delFileRes;
	private RpcResponseMessage.RenameRes renameRes;
	private RpcResponseMessage.GetBlockRes getBlockRes;
	private RpcResponseMessage.GetLocationRes getLocationRes;	
	private RpcResponseMessage.GetDataNodeRes getDataNodeRes;
	private RpcResponseMessage.PingNameNodeRes pingNameNodeRes;
	
	public TcpNameNodeResponse() {
		this.type = 0;
		this.error = 0;
		this.voidRes = new RpcResponseMessage.VoidRes();
		this.createFileRes = new RpcResponseMessage.CreateFileRes();
		this.getFileRes = new RpcResponseMessage.GetFileRes();
		this.delFileRes = new RpcResponseMessage.DeleteFileRes();
		this.renameRes = new RpcResponseMessage.RenameRes();
		this.getBlockRes = new RpcResponseMessage.GetBlockRes();
		this.getLocationRes = new RpcResponseMessage.GetLocationRes();
		this.getDataNodeRes = new RpcResponseMessage.GetDataNodeRes();
		this.pingNameNodeRes = new RpcResponseMessage.PingNameNodeRes();
	}
	
	public TcpNameNodeResponse(RpcResponseMessage.VoidRes message) {
		this.type = message.getType();
		this.voidRes = message;
	}
	
	public TcpNameNodeResponse(RpcResponseMessage.CreateFileRes message) {
		this.type = message.getType();
		this.createFileRes = message;
	}	
	
	public TcpNameNodeResponse(RpcResponseMessage.GetFileRes message) {
		this.type = message.getType();
		this.getFileRes = message;
	}
	
	public TcpNameNodeResponse(RpcResponseMessage.DeleteFileRes message) {
		this.type = message.getType();
		this.delFileRes = message;
	}	
	
	public TcpNameNodeResponse(RpcResponseMessage.RenameRes message) {
		this.type = message.getType();
		this.renameRes = message;
	}
	
	public TcpNameNodeResponse(RpcResponseMessage.GetBlockRes message) {
		this.type = message.getType();
		this.getBlockRes = message;
	}
	
	public TcpNameNodeResponse(RpcResponseMessage.GetLocationRes message) {
		this.type = message.getType();
		this.getLocationRes = message;
	}
	
	public TcpNameNodeResponse(RpcResponseMessage.GetDataNodeRes message) {
		this.type = message.getType();
		this.getDataNodeRes = message;
	}	
	
	public TcpNameNodeResponse(RpcResponseMessage.PingNameNodeRes message) {
		this.type = message.getType();
		this.pingNameNodeRes = message;
	}
	
	public void setType(short type) throws Exception {
		this.type = type;
	}	

	public int size(){
		return CSIZE;
	}
	
	public int write(ByteBuffer buffer){
		buffer.putShort(type);
		buffer.putShort(error);
		
		int written = 4;
		switch(type){
		case RpcProtocol.RES_VOID:
			written += voidRes.write(buffer);
			break;	
		case RpcProtocol.RES_CREATE_FILE:
			written += createFileRes.write(buffer);
			break;				
		case RpcProtocol.RES_GET_FILE:
			written += getFileRes.write(buffer);
			break;
		case RpcProtocol.RES_DELETE_FILE:
			written += delFileRes.write(buffer);
			break;				
		case RpcProtocol.RES_RENAME_FILE:
			written += renameRes.write(buffer);
			break;				
		case RpcProtocol.RES_GET_BLOCK:
			written += getBlockRes.write(buffer);
			break;
		case RpcProtocol.RES_GET_LOCATION:
			written += getLocationRes.write(buffer);
			break;			
		case RpcProtocol.RES_GET_DATANODE:
			written += getDataNodeRes.write(buffer);
			break;			
		case RpcProtocol.RES_PING_NAMENODE:
			written += pingNameNodeRes.write(buffer);
			break;			
		}
		
		return written;
	}
	
	public void update(ByteBuffer buffer){
		this.type = buffer.getShort();
		this.error = buffer.getShort();
		
		switch(type){
		case RpcProtocol.RES_VOID:
			voidRes.update(buffer);
			voidRes.setError(error);
			break;			
		case RpcProtocol.RES_CREATE_FILE:
			createFileRes.update(buffer);
			createFileRes.setError(error);
			break;				
		case RpcProtocol.RES_GET_FILE:
			getFileRes.update(buffer);
			getFileRes.setError(error);
			break;	
		case RpcProtocol.RES_DELETE_FILE:
			delFileRes.update(buffer);
			delFileRes.setError(error);
			break;				
		case RpcProtocol.RES_RENAME_FILE:
			renameRes.update(buffer);
			renameRes.setError(error);
			break;				
		case RpcProtocol.RES_GET_BLOCK:
			getBlockRes.update(buffer);
			getBlockRes.setError(error);
			break;
		case RpcProtocol.RES_GET_LOCATION:
			getLocationRes.update(buffer);
			getLocationRes.setError(error);
			break;			
		case RpcProtocol.RES_GET_DATANODE:
			getDataNodeRes.update(buffer);
			getDataNodeRes.setError(error);
			break;			
		case RpcProtocol.RES_PING_NAMENODE:
			pingNameNodeRes.update(buffer);
			pingNameNodeRes.setError(error);
			break;		
		}
	}
	
	public short getType(){
		return type;
	}
	
	public short getError() {
		return error;
	}

	public void setError(short error) {
		this.error = error;
	}	
	
	public RpcResponseMessage.VoidRes getVoid() {
		return voidRes;
	}	
	
	public RpcResponseMessage.CreateFileRes createFile() {
		return createFileRes;
	}	
	
	public RpcResponseMessage.GetFileRes getFile() {
		return getFileRes;
	}
	
	public RpcResponseMessage.DeleteFileRes removeFile() {
		return delFileRes;
	}	
	
	public RpcResponseMessage.RenameRes renameFile() {
		return renameRes;
	}	

	public RpcResponseMessage.GetBlockRes getBlock() {
		return getBlockRes;
	}	
	
	public RpcResponseMessage.GetLocationRes getLocation() {
		return getLocationRes;
	}	
	
	public RpcResponseMessage.GetDataNodeRes getDataNode() {
		return getDataNodeRes;
	}	
	
	public RpcResponseMessage.PingNameNodeRes pingNameNode(){
		return this.pingNameNodeRes;
	}
}
