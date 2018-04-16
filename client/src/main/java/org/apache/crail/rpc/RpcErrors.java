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

import org.apache.crail.utils.CrailUtils;
import org.slf4j.Logger;

public class RpcErrors {
	private static final Logger LOG = CrailUtils.getLogger();
	public static String[] messages = new String[32];	
	
	//errors
	public static short ERR_OK = 0;
	public static short ERR_UNKNOWN = 1;
	public static short ERR_PROTOCOL_MISMATCH = 2;
	public static short ERR_CREATE_FILE_FAILED = 3;
	public static short ERR_GET_FILE_FAILED = 4;
	public static short ERR_TOKEN_TAKEN = 5;
	public static short ERR_FILE_NOT_OPEN = 6;
	public static short ERR_TOKEN_MISMATCH = 7;
	public static short ERR_CAPACITY_EXCEEDED = 8;
	public static short ERR_POSITION_NEGATIV = 9;
	public static short ERR_OFFSET_TOO_LARGE = 10;
	public static short ERR_NO_FREE_BLOCKS = 11;
	public static short ERR_NO_DATANODES = 12;
	public static short ERR_DATANODE_NOT_REGISTERED = 13;
	public static short ERR_INVALID_RPC_CMD = 14;
	public static short ERR_PARENT_MISSING = 15;
	public static short ERR_PARENT_NOT_DIR = 16;
	public static short ERR_FILE_EXISTS = 17;
	public static short ERR_FILE_NOT_FOUND = 18;
	public static short ERR_SRC_FILE_NOT_FOUND = 19;
	public static short ERR_DST_PARENT_NOT_FOUND = 20;
	public static short ERR_HAS_CHILDREN = 21;	
	public static short ERR_FILE_COMPONENTS_EXCEEDED = 22;
	public static short ERR_DIRECTORY_EXISTS = 23;
	public static short ERR_TREE_CORRUPT = 24;
	public static short ERR_FILE_IS_NOT_DIR = 25;
	public static short ERR_DIR_LOCATION_AFFINITY_MISMATCH = 26;
	public static short ERR_ADD_BLOCK_FAILED = 27;
	public static short ERR_CREATE_FILE_BUG = 28;
	
	static {
		messages[ERR_OK] = "ERROR: No error, all fine";
		messages[ERR_UNKNOWN] = "ERROR: Unknown error";
		messages[ERR_PROTOCOL_MISMATCH] = "ERROR: Protocol mismatch";
		messages[ERR_CREATE_FILE_FAILED] = "ERROR: Create file failed";
		messages[ERR_GET_FILE_FAILED] = "ERROR: Get file failed";
		messages[ERR_TOKEN_TAKEN] = "ERROR: Token already taken";
		messages[ERR_FILE_NOT_OPEN] = "ERROR: File not open";
		messages[ERR_TOKEN_MISMATCH] = "ERROR: Token not maching";
		messages[ERR_CAPACITY_EXCEEDED] = "ERROR: Capacity exceeded";
		messages[ERR_POSITION_NEGATIV] = "ERROR: Position < 0";
		messages[ERR_OFFSET_TOO_LARGE] = "ERROR: Offset too large";
		messages[ERR_NO_FREE_BLOCKS] = "ERROR: No free blocks";
		messages[ERR_NO_DATANODES] = "ERROR: No data node";
		messages[ERR_DATANODE_NOT_REGISTERED] = "ERROR: Data node not registered";
		messages[ERR_INVALID_RPC_CMD] = "ERROR: Invalid RPC command";
		messages[ERR_PARENT_MISSING] = "ERROR: Parent node missing";
		messages[ERR_PARENT_NOT_DIR] = "ERROR: Parent node is not a dirctory";
		messages[ERR_FILE_EXISTS] = "ERROR: File already exists";
		messages[ERR_FILE_NOT_FOUND] = "ERROR: File not found";
		messages[ERR_SRC_FILE_NOT_FOUND] = "ERROR: Source file not found";
		messages[ERR_DST_PARENT_NOT_FOUND] = "ERROR: Destination parent folder not found";
		messages[ERR_HAS_CHILDREN] = "ERROR: Cannot remove folder with subfolders";
		messages[ERR_FILE_COMPONENTS_EXCEEDED] = "ERROR: Too many subfolders in directory name";
		messages[ERR_DIRECTORY_EXISTS] = "Directory exists";
		messages[ERR_TREE_CORRUPT] = "ERROR: File tree corrupt";
		messages[ERR_FILE_IS_NOT_DIR] = "File is not a directory";
		messages[ERR_DIR_LOCATION_AFFINITY_MISMATCH] = "Directories cannot have local affinity";
		messages[ERR_ADD_BLOCK_FAILED] = "Could not add block";
		messages[ERR_CREATE_FILE_BUG] = "Could not retrieve parent block";
	}
}
