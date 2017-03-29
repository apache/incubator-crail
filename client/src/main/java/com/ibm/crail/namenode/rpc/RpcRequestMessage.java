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

import java.net.UnknownHostException;
import java.nio.ByteBuffer;

import com.ibm.crail.CrailNodeType;
import com.ibm.crail.namenode.protocol.BlockInfo;
import com.ibm.crail.namenode.protocol.DataNodeInfo;
import com.ibm.crail.namenode.protocol.FileInfo;
import com.ibm.crail.namenode.protocol.FileName;

public class RpcRequestMessage {
	public static class CreateFileReq implements NameNodeProtocol.NameNodeRpcMessage {
		public static int CSIZE = FileName.CSIZE + 12;
		
		protected FileName filename;
		protected CrailNodeType type;
		protected int storageAffinity;
		protected int locationAffinity;
		
		public CreateFileReq(){
			this.filename = new FileName();
			this.type = CrailNodeType.DATAFILE;
			this.storageAffinity = 0;
			this.locationAffinity = 0;
		}
		
		public CreateFileReq(FileName filename, CrailNodeType type, int storageAffinity, int locationAffinity) {
			this.filename = filename;
			this.type = type;
			this.storageAffinity = storageAffinity;
			this.locationAffinity = locationAffinity;
		}

		public FileName getFileName() {
			return filename;
		}

		public CrailNodeType getFileType(){
			return type;
		}
		
		public int getStorageAffinity() {
			return storageAffinity;
		}		
		
		public int getLocationAffinity() {
			return locationAffinity;
		}
		
		
		public int size() {
			return CSIZE;
		}
		
		public short getType(){
			return NameNodeProtocol.REQ_CREATE_FILE;
		}		
		
		public int write(ByteBuffer buffer) {
			int written = filename.write(buffer);
			buffer.putInt(type.getLabel());
			buffer.putInt(storageAffinity);
			buffer.putInt(locationAffinity);
			written += 12;
			
			return written;
		}		

		public void update(ByteBuffer buffer) {
			filename.update(buffer);
			int tmp = buffer.getInt();
			type = CrailNodeType.parse(tmp);
			storageAffinity = buffer.getInt();
			locationAffinity = buffer.getInt();
		}
	}
	
	public static class GetFileReq implements NameNodeProtocol.NameNodeRpcMessage {
		public static int CSIZE = FileName.CSIZE + 4;
		
		protected FileName filename;
		protected boolean writeable;
		
		public GetFileReq(){
			this.filename = new FileName();
			this.writeable = false;
		}
		
		public GetFileReq(FileName filename, boolean writeable) {
			this.filename = filename;
			this.writeable = writeable;
		}

		public FileName getFileName() {
			return filename;
		}

		public boolean isWriteable(){
			return writeable;
		}
		
		
		public int size() {
			return CSIZE;
		}
		
		public short getType(){
			return NameNodeProtocol.REQ_GET_FILE;
		}		
		
		public int write(ByteBuffer buffer) {
			int written = filename.write(buffer);
			buffer.putInt(writeable ? 1 : 0);
			written += 4;
			return written;
		}		

		public void update(ByteBuffer buffer) {
			filename.update(buffer);
			int tmp = buffer.getInt();
			writeable = (tmp == 1) ? true : false;
		}		
	}
	
	public static class SetFileReq implements NameNodeProtocol.NameNodeRpcMessage {
		public static int CSIZE = FileInfo.CSIZE + 4;
		
		protected FileInfo fileInfo;
		protected boolean close;
		
		public SetFileReq(){
			this.fileInfo = new FileInfo();
			this.close = false;
		}
		
		public SetFileReq(FileInfo fileInfo, boolean close) {
			this.fileInfo = fileInfo;
			this.close = close;
		}

		public FileInfo getFileInfo() {
			return fileInfo;
		}

		public boolean isClose(){
			return close;
		}
		
		public int size() {
			return CSIZE;
		}
		
		public short getType(){
			return NameNodeProtocol.REQ_SET_FILE;
		}		
		
		public int write(ByteBuffer buffer) {
			int written = fileInfo.write(buffer, true);
			buffer.putInt(close ? 1 : 0);
			written += 4;
			return written;
		}		
	
		public void update(ByteBuffer buffer) {
			try {
				fileInfo.update(buffer);
				int tmp = buffer.getInt();
				close = (tmp == 1) ? true : false;
			} catch (UnknownHostException e) {
				e.printStackTrace();
			}
		}		
	}
	
	public static class RemoveFileReq implements NameNodeProtocol.NameNodeRpcMessage {
		public static int CSIZE = FileName.CSIZE + 4;
		
		protected FileName filename;
		protected boolean recursive;
		
		public RemoveFileReq(){
			this.filename = new FileName();
			this.recursive = false;
		}
		
		public RemoveFileReq(FileName filename, boolean recursive) {
			this.filename = filename;
			this.recursive = recursive;
		}

		public FileName getFileName() {
			return filename;
		}

		public boolean isRecursive(){
			return recursive;
		}
		
		
		public int size() {
			return CSIZE;
		}
		
		public short getType(){
			return NameNodeProtocol.REQ_REMOVE_FILE;
		}		
		
		public int write(ByteBuffer buffer) {
			int written = filename.write(buffer);
			buffer.putInt(recursive ? 1 : 0);
			written += 4;
			return written;
		}		

		public void update(ByteBuffer buffer) {
			filename.update(buffer);
			int tmp = buffer.getInt();
			recursive = (tmp == 1) ? true : false;
		}		
	}	
	
	public static class RenameFileReq implements NameNodeProtocol.NameNodeRpcMessage {
		public static int CSIZE = FileName.CSIZE*2;
		
		protected FileName srcFileName;
		protected FileName dstFileName;

		public RenameFileReq(){
			this.srcFileName = new FileName();
			this.dstFileName = new FileName();
		}
		
		public RenameFileReq(FileName srcFileName, FileName dstFileName) {
			this.srcFileName = srcFileName;
			this.dstFileName = dstFileName;
		}

		public FileName getSrcFileName() {
			return srcFileName;
		}

		public FileName getDstFileName() {
			return dstFileName;
		}
		
		
		public int size() {
			return CSIZE;
		}
		
		public short getType(){
			return NameNodeProtocol.REQ_RENAME_FILE;
		}		
		
		public int write(ByteBuffer buffer) {
			int written = srcFileName.write(buffer);
			written += dstFileName.write(buffer);
			return written;
		}		

		public void update(ByteBuffer buffer) {
			srcFileName.update(buffer);
			dstFileName.update(buffer);
		}		
	}	
	
	public static class GetBlockReq implements NameNodeProtocol.NameNodeRpcMessage {
		public static int CSIZE = 40;
		
		protected long fd;
		protected long token;
		protected long position;
		protected int storageAffinity;
		protected int locationAffinity;
		protected long capacity;

		public GetBlockReq(){
			this.fd = 0;
			this.token = 0;
			this.position = 0;
			this.capacity = 0;	
		}
		
		public GetBlockReq(long fd, long token, long position, int storageAffinity, int locationAffinity, long capacity) {
			this.fd = fd;
			this.token = token;
			this.position = position;
			this.storageAffinity = storageAffinity;
			this.locationAffinity = locationAffinity;
			this.capacity = capacity;
		}

		public long getFd() {
			return fd;
		}

		public long getPosition(){
			return this.position;
		}

		public long getToken() {
			return token;
		}
		
		public int getStorageAffinity(){
			return storageAffinity;
		}
		
		public int getLocationAffinity(){
			return locationAffinity;
		}

		public long getCapacity(){
			return capacity;
		}
		
		
		public int size() {
			return CSIZE;
		}
		
		public short getType(){
			return NameNodeProtocol.REQ_GET_BLOCK;
		}		
		
		public int write(ByteBuffer buffer) {
			buffer.putLong(fd);
			buffer.putLong(token);
			buffer.putLong(position);
			buffer.putInt(storageAffinity);
			buffer.putInt(locationAffinity);
			buffer.putLong(capacity);
			return CSIZE;
		}		

		public void update(ByteBuffer buffer) {
			fd = buffer.getLong();
			token = buffer.getLong();
			position = buffer.getLong();
			storageAffinity = buffer.getInt();
			locationAffinity = buffer.getInt();
			capacity = buffer.getLong();
		}		
	}
	
	public static class GetLocationReq implements NameNodeProtocol.NameNodeRpcMessage {
		public static int CSIZE = FileName.CSIZE + 8;
		
		protected FileName fileName;
		protected long position;

		public GetLocationReq(){
			this.fileName = new FileName();
			this.position = 0;
		}
		
		public GetLocationReq(FileName fileName, long position) {
			this.fileName = fileName;
			this.position = position;
		}

		public long getPosition(){
			return this.position;
		}

		public FileName getFileName() {
			return fileName;
		}
		
		public int size() {
			return CSIZE;
		}
		
		public short getType(){
			return NameNodeProtocol.REQ_GET_LOCATION;
		}		
		
		public int write(ByteBuffer buffer) {
			fileName.write(buffer);
			buffer.putLong(position);
			return CSIZE;
		}		

		public void update(ByteBuffer buffer) {
			fileName.update(buffer);
			position = buffer.getLong();
		}		
	}
	
	public static class SetBlockReq implements NameNodeProtocol.NameNodeRpcMessage {
		public static int CSIZE = BlockInfo.CSIZE;
		
		protected BlockInfo blockInfo;
		
		public SetBlockReq() {
			this.blockInfo = new BlockInfo();
		}
		
		public SetBlockReq(BlockInfo blockInfo) {
			this.blockInfo = blockInfo;
		}

		public BlockInfo getBlockInfo() throws Exception {
			return blockInfo;
		}
		
		public int size() {
			return CSIZE;
		}	
		
		public short getType(){
			return NameNodeProtocol.REQ_SET_BLOCK;
		}		
		
		public int write(ByteBuffer buffer){
			int written = blockInfo.write(buffer);
			return written;
		}
		
		public void update(ByteBuffer buffer) {
			try {
				blockInfo.update(buffer);
			} catch (UnknownHostException e) {
				e.printStackTrace();
			}
		}		
	}	
	
	public static class GetDataNodeReq implements NameNodeProtocol.NameNodeRpcMessage {
		public static int CSIZE = DataNodeInfo.CSIZE;
		
		protected DataNodeInfo dnInfo;
		
		public GetDataNodeReq(){
			this.dnInfo = new DataNodeInfo();
		}
		
		public GetDataNodeReq(DataNodeInfo dnInfo) {
			this.dnInfo = dnInfo;
		}

		public DataNodeInfo getInfo(){
			return this.dnInfo;
		}
		
		
		public int size() {
			return CSIZE;
		}
		
		public short getType(){
			return NameNodeProtocol.REQ_GET_DATANODE;
		}		
		
		public int write(ByteBuffer buffer) {
			int written = dnInfo.write(buffer);
			return written;
		}		

		public void update(ByteBuffer buffer) {
			try {
				dnInfo.update(buffer);
			} catch (UnknownHostException e) {
				e.printStackTrace();
			}
		}		
	}	
	
	
	public static class DumpNameNodeReq implements NameNodeProtocol.NameNodeRpcMessage {
		public static int CSIZE = 4;
		
		protected int op;
		
		public DumpNameNodeReq(){
			this.op = 0;
		}

		public int getOp() {
			return op;
		}
		
		public int size() {
			return CSIZE;
		}
		
		public short getType(){
			return NameNodeProtocol.REQ_DUMP_NAMENODE;
		}		
		
		public int write(ByteBuffer buffer) {
			buffer.putInt(op);
			return CSIZE;
		}		

		public void update(ByteBuffer buffer) {
			op = buffer.getInt();
		}		
	}
	
	public static class PingNameNodeReq implements NameNodeProtocol.NameNodeRpcMessage {
		public static int CSIZE = 4;
		
		protected int op;
		
		public PingNameNodeReq(){
			this.op = 0;	
		}
		
		public int getOp(){
			return this.op;
		}
		
		public int size() {
			return CSIZE;
		}
	
		public short getType(){
			return NameNodeProtocol.REQ_PING_NAMENODE;
		}		
		
		public int write(ByteBuffer buffer) {
			buffer.putInt(op);
			return CSIZE;
		}		

		public void update(ByteBuffer buffer) {
			op = buffer.getInt();
		}		
	}	
	

}
