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

import org.apache.crail.rpc.RpcErrors;
import org.apache.crail.utils.CrailUtils;
import org.slf4j.Logger;

public class RpcProtocol extends RpcErrors {
	private static final Logger LOG = CrailUtils.getLogger();
	
	public static short[] requestTypes = new short[16];
	public static short[] responseTypes = new short[16];
	
	//rpc calls
	public static final short CMD_CREATE_FILE = 1;	
	public static final short CMD_GET_FILE = 2;
	public static final short CMD_SET_FILE = 3;	
	public static final short CMD_REMOVE_FILE = 4;
	public static final short CMD_RENAME_FILE = 5;
	public static final short CMD_GET_BLOCK = 6;	
	public static final short CMD_GET_LOCATION = 7;	
	public static final short CMD_SET_BLOCK = 8;
	public static final short CMD_DUMP_NAMENODE = 10;
	public static final short CMD_PING_NAMENODE = 11;
	public static final short CMD_GET_DATANODE = 12;
	
	//request types
	public static final short REQ_CREATE_FILE = 1;	
	public static final short REQ_GET_FILE = 2;
	public static final short REQ_SET_FILE = 3;
	public static final short REQ_REMOVE_FILE = 4;
	public static final short REQ_RENAME_FILE = 5;
	public static final short REQ_GET_BLOCK = 6;
	public static final short REQ_GET_LOCATION = 7;	
	public static final short REQ_SET_BLOCK = 8;
	public static final short REQ_DUMP_NAMENODE = 10;
	public static final short REQ_PING_NAMENODE = 11;
	public static final short REQ_GET_DATANODE = 12;
	
	//response types
	public static final short RES_VOID = 1;
	public static final short RES_CREATE_FILE = 2;
	public static final short RES_GET_FILE = 3;
	public static final short RES_DELETE_FILE = 4;
	public static final short RES_RENAME_FILE = 5;
	public static final short RES_GET_BLOCK = 6;
	public static final short RES_GET_LOCATION = 7;
	public static final short RES_PING_NAMENODE = 9;
	public static final short RES_GET_DATANODE = 10;
	
	
	static {
		requestTypes[0] = 0;
		requestTypes[CMD_CREATE_FILE] = REQ_CREATE_FILE;
		requestTypes[CMD_GET_FILE] = REQ_GET_FILE;
		requestTypes[CMD_SET_FILE] = REQ_SET_FILE;
		requestTypes[CMD_REMOVE_FILE] = REQ_REMOVE_FILE;
		requestTypes[CMD_RENAME_FILE] = REQ_RENAME_FILE;
		requestTypes[CMD_GET_BLOCK] = REQ_GET_BLOCK;
		requestTypes[CMD_GET_LOCATION] = REQ_GET_LOCATION;
		requestTypes[CMD_SET_BLOCK] = REQ_SET_BLOCK;
		requestTypes[CMD_DUMP_NAMENODE] = REQ_DUMP_NAMENODE;
		requestTypes[CMD_PING_NAMENODE] = REQ_PING_NAMENODE;	
		requestTypes[CMD_GET_DATANODE] = REQ_GET_DATANODE;
		
		responseTypes[0] = 0;
		responseTypes[CMD_CREATE_FILE] = RES_CREATE_FILE;
		responseTypes[CMD_GET_FILE] = RES_GET_FILE;
		responseTypes[CMD_SET_FILE] = RES_VOID;
		responseTypes[CMD_REMOVE_FILE] = RES_DELETE_FILE;
		responseTypes[CMD_RENAME_FILE] = RES_RENAME_FILE;
		responseTypes[CMD_GET_BLOCK] = RES_GET_BLOCK;
		responseTypes[CMD_GET_LOCATION] = RES_GET_LOCATION;
		responseTypes[CMD_SET_BLOCK] = RES_VOID;
		responseTypes[CMD_DUMP_NAMENODE] = RES_VOID;
		responseTypes[CMD_PING_NAMENODE] = RES_PING_NAMENODE;	
		responseTypes[CMD_GET_DATANODE] = RES_GET_DATANODE;
	}
	

	public static boolean verifyProtocol(short cmd, NameNodeRpcMessage request, NameNodeRpcMessage response){
		if (request.getType() != RpcProtocol.requestTypes[cmd]){
			LOG.info("protocol mismatch, cmd " + cmd + ", request.type " + request.getType() + ", response.type " + response.getType());
			return false;
		}
		if (response.getType() != RpcProtocol.responseTypes[cmd]){
			LOG.info("protocol mismatch, cmd " + cmd + ", request.type " + request.getType() + ", response.type " + response.getType());
			return false;
		}
		return true;
	}
	
	public static interface NameNodeRpcMessage {
		short getType();
	}
}
