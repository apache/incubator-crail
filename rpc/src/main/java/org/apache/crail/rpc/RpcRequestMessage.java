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

import java.net.UnknownHostException;
import java.nio.ByteBuffer;

import org.apache.crail.CrailNodeType;
import org.apache.crail.metadata.BlockInfo;
import org.apache.crail.metadata.DataNodeInfo;
import org.apache.crail.metadata.FileInfo;
import org.apache.crail.metadata.FileName;

public class RpcRequestMessage {
	public static class CreateFileReq implements RpcProtocol.NameNodeRpcMessage {
		public static int CSIZE = FileName.CSIZE + 16;
		
		protected FileName filename;
		protected CrailNodeType type;
		protected int storageClass;
		protected int locationClass;
		protected boolean enumerable;
		
		public CreateFileReq(){
			this.filename = new FileName();
			this.type = CrailNodeType.DATAFILE;
			this.storageClass = 0;
			this.locationClass = 0;
			this.enumerable = true;
		}
		
		public CreateFileReq(FileName filename, CrailNodeType type, int storageClass, int locationClass, boolean enumerable) {
			this.filename = filename;
			this.type = type;
			this.storageClass = storageClass;
			this.locationClass = locationClass;
			this.enumerable = enumerable;
		}

		public FileName getFileName() {
			return filename;
		}

		public CrailNodeType getFileType(){
			return type;
		}
		
		public int getStorageClass() {
			return storageClass;
		}		
		
		public int getLocationClass() {
			return locationClass;
		}
		
		public boolean isEnumerable() {
			return enumerable;
		}

		public int size() {
			return CSIZE;
		}
		
		public short getType(){
			return RpcProtocol.REQ_CREATE_FILE;
		}		
		
		public int write(ByteBuffer buffer) {
			filename.write(buffer);
			buffer.putInt(type.getLabel());
			buffer.putInt(storageClass);
			buffer.putInt(locationClass);
			buffer.putInt(enumerable ? 1 : 0);
			
			return CSIZE;
		}		

		public void update(ByteBuffer buffer) {
			filename.update(buffer);
			int _type = buffer.getInt();
			type = CrailNodeType.parse(_type);
			storageClass = buffer.getInt();
			locationClass = buffer.getInt();
			int _enumerable = buffer.getInt();
			enumerable = (_enumerable == 1) ? true : false;			
		}

		@Override
		public String toString() {
			return "CreateFileReq [filename=" + filename + ", type=" + type
					+ ", storageClass=" + storageClass + ", locationClass="
					+ locationClass + ", enumerable=" + enumerable + "]";
		}
	}
	
	public static class GetFileReq implements RpcProtocol.NameNodeRpcMessage {
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
			return RpcProtocol.REQ_GET_FILE;
		}		
		
		public int write(ByteBuffer buffer) {
			filename.write(buffer);
			buffer.putInt(writeable ? 1 : 0);
			return CSIZE;
		}		

		public void update(ByteBuffer buffer) {
			filename.update(buffer);
			int tmp = buffer.getInt();
			writeable = (tmp == 1) ? true : false;
		}		
	}
	
	public static class SetFileReq implements RpcProtocol.NameNodeRpcMessage {
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
			return RpcProtocol.REQ_SET_FILE;
		}		
		
		public int write(ByteBuffer buffer) {
			fileInfo.write(buffer, true);
			buffer.putInt(close ? 1 : 0);
			return CSIZE;
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

		@Override
		public String toString() {
			return "SetFileReq [fileInfo=" + fileInfo + ", close=" + close
					+ "]";
		}		
	}
	
	public static class RemoveFileReq implements RpcProtocol.NameNodeRpcMessage {
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
			return RpcProtocol.REQ_REMOVE_FILE;
		}		
		
		public int write(ByteBuffer buffer) {
			filename.write(buffer);
			buffer.putInt(recursive ? 1 : 0);
			return CSIZE;
		}		

		public void update(ByteBuffer buffer) {
			filename.update(buffer);
			int tmp = buffer.getInt();
			recursive = (tmp == 1) ? true : false;
		}		
	}	
	
	public static class RenameFileReq implements RpcProtocol.NameNodeRpcMessage {
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
			return RpcProtocol.REQ_RENAME_FILE;
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

		@Override
		public String toString() {
			return "RenameFileReq [srcFileName=" + srcFileName
					+ ", dstFileName=" + dstFileName + "]";
		}		
	}	
	
	public static class GetBlockReq implements RpcProtocol.NameNodeRpcMessage {
		public static int CSIZE = 32;
		
		protected long fd;
		protected long token;
		protected long position;
		protected long capacity;

		public GetBlockReq(){
			this.fd = 0;
			this.token = 0;
			this.position = 0;
			this.capacity = 0;	
		}
		
		public GetBlockReq(long fd, long token, long position, long capacity) {
			this.fd = fd;
			this.token = token;
			this.position = position;
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
		
		public long getCapacity(){
			return capacity;
		}
		
		
		public int size() {
			return CSIZE;
		}
		
		public short getType(){
			return RpcProtocol.REQ_GET_BLOCK;
		}		
		
		public int write(ByteBuffer buffer) {
			buffer.putLong(fd);
			buffer.putLong(token);
			buffer.putLong(position);
			buffer.putLong(capacity);
			return CSIZE;
		}		

		public void update(ByteBuffer buffer) {
			fd = buffer.getLong();
			token = buffer.getLong();
			position = buffer.getLong();
			capacity = buffer.getLong();
		}

		@Override
		public String toString() {
			return "GetBlockReq [fd=" + fd + ", token=" + token + ", position="
					+ position + ", capacity=" + capacity + "]";
		}

		public void setToken(long value) {
			this.token = value;
		}		
	}
	
	public static class GetLocationReq implements RpcProtocol.NameNodeRpcMessage {
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
			return RpcProtocol.REQ_GET_LOCATION;
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
	
	public static class SetBlockReq implements RpcProtocol.NameNodeRpcMessage {
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
			return RpcProtocol.REQ_SET_BLOCK;
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

		@Override
		public String toString() {
			return "SetBlockReq [blockInfo=" + blockInfo + "]";
		}
		
		
	}	
	
	public static class GetDataNodeReq implements RpcProtocol.NameNodeRpcMessage {
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
			return RpcProtocol.REQ_GET_DATANODE;
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
	
	
	public static class DumpNameNodeReq implements RpcProtocol.NameNodeRpcMessage {
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
			return RpcProtocol.REQ_DUMP_NAMENODE;
		}		
		
		public int write(ByteBuffer buffer) {
			buffer.putInt(op);
			return CSIZE;
		}		

		public void update(ByteBuffer buffer) {
			op = buffer.getInt();
		}		
	}
	
	public static class PingNameNodeReq implements RpcProtocol.NameNodeRpcMessage {
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
			return RpcProtocol.REQ_PING_NAMENODE;
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
