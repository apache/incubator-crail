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

package org.apache.crail.namenode;

import org.apache.crail.rpc.RpcNameNodeService;
import org.apache.crail.rpc.RpcNameNodeState;
import org.apache.crail.rpc.RpcProtocol;
import org.apache.crail.rpc.RpcRequestMessage.CreateFileReq;
import org.apache.crail.rpc.RpcRequestMessage.DumpNameNodeReq;
import org.apache.crail.rpc.RpcRequestMessage.GetBlockReq;
import org.apache.crail.rpc.RpcRequestMessage.GetDataNodeReq;
import org.apache.crail.rpc.RpcRequestMessage.GetFileReq;
import org.apache.crail.rpc.RpcRequestMessage.GetLocationReq;
import org.apache.crail.rpc.RpcRequestMessage.PingNameNodeReq;
import org.apache.crail.rpc.RpcRequestMessage.RemoveFileReq;
import org.apache.crail.rpc.RpcRequestMessage.RenameFileReq;
import org.apache.crail.rpc.RpcRequestMessage.SetBlockReq;
import org.apache.crail.rpc.RpcRequestMessage.SetFileReq;
import org.apache.crail.rpc.RpcResponseMessage.CreateFileRes;
import org.apache.crail.rpc.RpcResponseMessage.DeleteFileRes;
import org.apache.crail.rpc.RpcResponseMessage.GetBlockRes;
import org.apache.crail.rpc.RpcResponseMessage.GetDataNodeRes;
import org.apache.crail.rpc.RpcResponseMessage.GetFileRes;
import org.apache.crail.rpc.RpcResponseMessage.GetLocationRes;
import org.apache.crail.rpc.RpcResponseMessage.PingNameNodeRes;
import org.apache.crail.rpc.RpcResponseMessage.RenameRes;
import org.apache.crail.rpc.RpcResponseMessage.VoidRes;

public class LogDispatcher implements RpcNameNodeService {
	private RpcNameNodeService service;
	private LogService logService;
	
	public LogDispatcher(RpcNameNodeService service) throws Exception{
		this.service = service;
		this.logService = new LogService();
		this.logService.replay(service);
	}

	@Override
	public short createFile(CreateFileReq request, CreateFileRes response,
			RpcNameNodeState errorState) throws Exception {
		LogRecord record = new LogRecord(request);
		record.setCommand(RpcProtocol.CMD_CREATE_FILE);
		logService.writeRecord(record);
		return service.createFile(request, response, errorState);
	}

	@Override
	public short getFile(GetFileReq request, GetFileRes response,
			RpcNameNodeState errorState) throws Exception {
		return service.getFile(request, response, errorState);
	}

	@Override
	public short setFile(SetFileReq request, VoidRes response,
			RpcNameNodeState errorState) throws Exception {
		LogRecord record = new LogRecord(request);
		record.setCommand(RpcProtocol.CMD_SET_FILE);
		logService.writeRecord(record);		
		return service.setFile(request, response, errorState);
	}

	@Override
	public short removeFile(RemoveFileReq request, DeleteFileRes response,
			RpcNameNodeState errorState) throws Exception {
		LogRecord record = new LogRecord(request);
		record.setCommand(RpcProtocol.CMD_REMOVE_FILE);
		logService.writeRecord(record);		
		return service.removeFile(request, response, errorState);
	}

	@Override
	public short renameFile(RenameFileReq request, RenameRes response,
			RpcNameNodeState errorState) throws Exception {
		LogRecord record = new LogRecord(request);
		record.setCommand(RpcProtocol.CMD_RENAME_FILE);
		logService.writeRecord(record);		
		return service.renameFile(request, response, errorState);
	}

	@Override
	public short getDataNode(GetDataNodeReq request, GetDataNodeRes response,
			RpcNameNodeState errorState) throws Exception {
		return service.getDataNode(request, response, errorState);
	}

	@Override
	public short setBlock(SetBlockReq request, VoidRes response,
			RpcNameNodeState errorState) throws Exception {
		LogRecord record = new LogRecord(request);
		record.setCommand(RpcProtocol.CMD_SET_BLOCK);
		logService.writeRecord(record);		
		return service.setBlock(request, response, errorState);
	}

	@Override
	public short getBlock(GetBlockReq request, GetBlockRes response,
			RpcNameNodeState errorState) throws Exception {
		LogRecord record = new LogRecord(request);
		record.setCommand(RpcProtocol.CMD_GET_BLOCK);
		logService.writeRecord(record);		
		return service.getBlock(request, response, errorState);
	}

	@Override
	public short getLocation(GetLocationReq request, GetLocationRes response,
			RpcNameNodeState errorState) throws Exception {
		return service.getLocation(request, response, errorState);
	}

	@Override
	public short dump(DumpNameNodeReq request, VoidRes response,
			RpcNameNodeState errorState) throws Exception {
		return service.dump(request, response, errorState);
	}

	@Override
	public short ping(PingNameNodeReq request, PingNameNodeRes response,
			RpcNameNodeState errorState) throws Exception {
		return service.ping(request, response, errorState);
	}
	
}
