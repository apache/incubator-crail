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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.crail.conf.CrailConstants;
import org.apache.crail.rpc.RpcErrors;
import org.apache.crail.rpc.RpcNameNodeService;
import org.apache.crail.rpc.RpcProtocol;
import org.apache.crail.utils.CrailUtils;
import org.slf4j.Logger;

public class LogService {
	public static final Logger LOG = CrailUtils.getLogger();
	
	private ConcurrentHashMap<Long, Long> tokens;
	private FileOutputStream outStream;
	private FileChannel outChannel;
	private ByteBuffer header;
	private ByteBuffer payload;
	
	public LogService() throws IOException {
		File file = new File(CrailConstants.NAMENODE_LOG);
		if (!file.exists()){
			file.createNewFile();
		}
		outStream = new FileOutputStream(CrailConstants.NAMENODE_LOG, true);
		outChannel = outStream.getChannel();
		header = ByteBuffer.allocate(4);
		payload = ByteBuffer.allocate(512);
		tokens = new ConcurrentHashMap<Long, Long>();
	}
	
	public void writeRecord(LogRecord record) throws IOException{
		payload.clear();
		record.write(payload);
		payload.flip();
		
		header.clear();
		header.putInt(payload.remaining());
		header.flip();
		
		outChannel.write(header);
		while(payload.hasRemaining()){
			outChannel.write(payload);
		}
	}
	
	public void replay(RpcNameNodeService service) throws Exception {
		File file = new File(CrailConstants.NAMENODE_LOG);
		if (!file.exists()){
			return;
		}		
		
		FileInputStream inStream = new FileInputStream(CrailConstants.NAMENODE_LOG);
		FileChannel inChannel = inStream.getChannel();
		LogRecord record = new LogRecord();
		LogResponse response = new LogResponse();
		
		header.clear();
		int ret = inChannel.read(header);
		while(ret > 0){
			header.flip();
			int size = header.getInt();
			
			payload.clear().limit(size);
			while(payload.hasRemaining()){
				inChannel.read(payload);
			}
			payload.flip();
			record.update(payload);
			processServerEvent(service, record, response);
			
			header.clear();
			ret = inChannel.read(header);
		}
		
		inChannel.close();
		inStream.close();
	}
	
	public void close() throws IOException{
		outChannel.close();
		outStream.close();
	}
	
	private void processServerEvent(RpcNameNodeService service, LogRecord record, LogResponse response) {
		short error = RpcErrors.ERR_OK;
		try {
			switch(record.getCmd()) {
			case RpcProtocol.CMD_CREATE_FILE:
				error = service.createFile(record.createFile(), response.createFile(), response);
				long fd = response.createFile().getFile().getFd();
				long token = response.createFile().getFile().getToken();
				tokens.put(response.createFile().getFile().getFd(), response.createFile().getFile().getToken());
				tokens.put(response.createFile().getParent().getFd(), response.createFile().getParent().getToken());
				break;			
			case RpcProtocol.CMD_SET_FILE:
				record.setFile().getFileInfo().setToken(tokens.get(record.setFile().getFileInfo().getFd()));
				error = service.setFile(record.setFile(), response.getVoid(), response);
				break;
			case RpcProtocol.CMD_REMOVE_FILE:
				error = service.removeFile(record.removeFile(), response.delFile(), response);
				break;				
			case RpcProtocol.CMD_RENAME_FILE:
				error = service.renameFile(record.renameFile(), response.getRename(), response);
				break;		
			case RpcProtocol.CMD_GET_BLOCK:
				record.getBlock().setToken(tokens.get(record.getBlock().getFd()));
				error = service.getBlock(record.getBlock(), response.getBlock(), response);
				break;
			case RpcProtocol.CMD_SET_BLOCK:
				error = service.setBlock(record.setBlock(), response.getVoid(), response);
				break;
			default:
				error = RpcErrors.ERR_INVALID_RPC_CMD;
				LOG.info("Rpc command not valid, opcode " + record.getCmd());
			}
		} catch(Exception e){
			error = RpcErrors.ERR_UNKNOWN;
			LOG.info(RpcErrors.messages[RpcErrors.ERR_UNKNOWN] + e.getMessage());
			e.printStackTrace();
		}
	}	
}
