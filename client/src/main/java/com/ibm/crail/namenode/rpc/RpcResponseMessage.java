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
import java.net.UnknownHostException;
import java.nio.ByteBuffer;

import com.ibm.crail.namenode.protocol.BlockInfo;
import com.ibm.crail.namenode.protocol.DataNodeStatistics;
import com.ibm.crail.namenode.protocol.FileInfo;

public class RpcResponseMessage {
	public static class VoidRes implements NameNodeProtocol.NameNodeRpcMessage {
		private short error;
		
		public VoidRes() {
			this.error = 0;
		}
		
		public int size() {
			return 0;
		}
		
		public short getType(){
			return NameNodeProtocol.RES_VOID;
		}
		
		public void update(ByteBuffer arg0) {
		}

		public int write(ByteBuffer arg0) {
			return 0;
		}
		
		public short getError(){
			return error;
		}

		public void setError(short error) {
			this.error = error;
		}		
	}
	
	public static class CreateFileRes implements NameNodeProtocol.NameNodeRpcMessage {
		public static int CSIZE = FileInfo.CSIZE*2 + BlockInfo.CSIZE*2;
		
		private FileInfo fileInfo;
		private FileInfo parentInfo;
		private BlockInfo fileBlock;
		private BlockInfo dirBlock;
		
		private boolean shipToken;
		private short error;
		

		public CreateFileRes() {
			this.fileInfo = new FileInfo();
			this.parentInfo = new FileInfo();
			this.fileBlock = new BlockInfo();
			this.dirBlock = new BlockInfo();
		
			this.shipToken = false;
			this.error = 0;
		}
		
		public int size() {
			return CSIZE;
		}
		
		public short getType(){
			return NameNodeProtocol.RES_CREATE_FILE;
		}
		
		public int write(ByteBuffer buffer) {
			int written = fileInfo.write(buffer, shipToken);
			written += parentInfo.write(buffer, false);
			written += fileBlock.write(buffer);
			written += dirBlock.write(buffer);
			return written;
		}		

		public void update(ByteBuffer buffer) {
			try {
				fileInfo.update(buffer);
				parentInfo.update(buffer);
				fileBlock.update(buffer);
				dirBlock.update(buffer);
			} catch (UnknownHostException e) {
				e.printStackTrace();
			}
		}

		public FileInfo getFile() {
			return fileInfo;
		}

		public void setFileInfo(FileInfo fileInfo) {
			if (fileInfo != null){
				this.fileInfo.setFileInfo(fileInfo);
//				this.shipToken = false;
			}
		}
		
		public FileInfo getParent() {
			return parentInfo;
		}
		
		public void setParentInfo(FileInfo parentInfo) {
			if (parentInfo != null){
				this.parentInfo.setFileInfo(parentInfo);
			}
		}

		public BlockInfo getFileBlock(){
			return fileBlock;
		}
		
		public void setFileBlock(BlockInfo blockInfo){
			this.fileBlock.setBlockInfo(blockInfo);
		}
		
		public BlockInfo getDirBlock(){
			return dirBlock;
		}
		
		public void setDirBlock(BlockInfo blockInfo){
			this.dirBlock.setBlockInfo(blockInfo);
		}		
		
		public void shipToken(boolean value){
			this.shipToken = value;
		}

		public short getError(){
			return error;
		}

		public void setError(short error) {
			this.error = error;
		}

		public boolean isShipToken() {
			return shipToken;
		}
	}	
	
	public static class GetFileRes implements NameNodeProtocol.NameNodeRpcMessage {
		public static int CSIZE = FileInfo.CSIZE + BlockInfo.CSIZE;
		
		private FileInfo fileInfo;
		private BlockInfo fileBlock;
		private boolean shipToken;
		private short error;

		public GetFileRes() {
			this.fileInfo = new FileInfo();
			this.fileBlock = new BlockInfo();
			
			this.shipToken = false;
			this.error = 0;
		}
		
		public int size() {
			return CSIZE;
		}
		
		public short getType(){
			return NameNodeProtocol.RES_GET_FILE;
		}
		
		public int write(ByteBuffer buffer) {
			int written = fileInfo.write(buffer, shipToken);
			written += fileBlock.write(buffer);
			return written;
		}		

		public void update(ByteBuffer buffer) {
			try {
				fileInfo.update(buffer);
				fileBlock.update(buffer);
			} catch (UnknownHostException e) {
				e.printStackTrace();
			}
		}

		public FileInfo getFile() {
			return fileInfo;
		}

		public void setFileInfo(FileInfo fileInfo) {
			if (fileInfo != null){
				this.fileInfo.setFileInfo(fileInfo);
				this.shipToken = false;
			}
		}
		
		public BlockInfo getFileBlock(){
			return fileBlock;
		}
		
		public void setFileBlock(BlockInfo blockInfo){
			if (blockInfo != null){
				fileBlock.setBlockInfo(blockInfo);
			}
		}
		
		public void shipToken(){
			this.shipToken = true;
		}

		public short getError(){
			return error;
		}

		public void setError(short error) {
			this.error = error;
		}
	}
	
	public static class DeleteFileRes implements NameNodeProtocol.NameNodeRpcMessage {
		public static int CSIZE = FileInfo.CSIZE*2;
		
		private FileInfo fileInfo;
		private FileInfo parentInfo;
		private boolean shipToken;
		
		private short error;
		

		public DeleteFileRes() {
			this.fileInfo = new FileInfo();
			this.parentInfo = new FileInfo();
			this.shipToken = false;
			
			this.error = 0;
		}
		
		public int size() {
			return CSIZE;
		}
		
		public short getType(){
			return NameNodeProtocol.RES_DELETE_FILE;
		}
		
		public int write(ByteBuffer buffer) {
			int written = fileInfo.write(buffer, shipToken);
			written += parentInfo.write(buffer, false);
			return written;
		}		

		public void update(ByteBuffer buffer) {
			try {
				fileInfo.update(buffer);
				parentInfo.update(buffer);
			} catch (UnknownHostException e) {
				e.printStackTrace();
			}
		}

		public FileInfo getFile() {
			return fileInfo;
		}

		public FileInfo getParent() {
			return parentInfo;
		}
		
		public void setFileInfo(FileInfo fileInfo) {
			if (fileInfo != null){
				this.fileInfo.setFileInfo(fileInfo);
				this.shipToken = false;
			}
		}
		
		public void setParentInfo(FileInfo parentInfo) {
			if (parentInfo != null){
				this.parentInfo.setFileInfo(parentInfo);
			}
		}		
		
		public void shipToken(){
			this.shipToken = true;
		}

		public short getError(){
			return error;
		}

		public void setError(short error) {
			this.error = error;
		}
	}	
	
	public static class RenameRes implements NameNodeProtocol.NameNodeRpcMessage {
		public static int CSIZE = FileInfo.CSIZE*4 + BlockInfo.CSIZE*2;
		
		private FileInfo srcParent;
		private FileInfo srcFile;
		private BlockInfo srcBlock;
		private FileInfo dstParent;
		private FileInfo dstFile;
		private BlockInfo dstBlock;
		private short error;

		public RenameRes() {
			this.srcParent = new FileInfo();
			this.srcFile = new FileInfo();
			this.srcBlock = new BlockInfo();
			this.dstParent = new FileInfo();
			this.dstFile = new FileInfo();	
			this.dstBlock = new BlockInfo();
			this.error = 0;
		}
		
		public int size() {
			return CSIZE;
		}
		
		public short getType(){
			return NameNodeProtocol.RES_RENAME_FILE;
		}
		
		public int write(ByteBuffer buffer) {
			int written = srcParent.write(buffer, false);
			written += srcFile.write(buffer, false);
			written += srcBlock.write(buffer);
			written += dstParent.write(buffer, false);
			written += dstFile.write(buffer, false);
			written += dstBlock.write(buffer);
			return written;
		}		

		public void update(ByteBuffer buffer) {
			try {
				srcParent.update(buffer);
				srcFile.update(buffer);
				srcBlock.update(buffer);
				dstParent.update(buffer);
				dstFile.update(buffer);
				dstBlock.update(buffer);
			} catch (UnknownHostException e) {
				e.printStackTrace();
			}
		}
		
		public FileInfo getSrcParent(){
			return srcParent;
		}

		public FileInfo getSrcFile() {
			return srcFile;
		}

		public FileInfo getDstParent() {
			return dstParent;
		}	
		
		public FileInfo getDstFile() {
			return this.dstFile;
		}		
		
		public void setSrcParent(FileInfo srcParent) {
			if (srcParent != null){
				this.srcParent.setFileInfo(srcParent);
			}
		}

		public void setSrcFile(FileInfo srcFile) {
			if (srcFile != null){
				this.srcFile.setFileInfo(srcFile);
			}
		}

		public void setDstParent(FileInfo dstParent) {
			if (dstParent != null){
				this.dstParent.setFileInfo(dstParent);
			}
		}

		public void setDstFile(FileInfo dstFile) {
			if (dstFile != null){
				this.dstFile.setFileInfo(dstFile);
			}
		}
		
		public short getError(){
			return error;
		}

		public void setError(short error) {
			this.error = error;
		}

		public void setSrcBlock(BlockInfo srcBlock) {
			if (srcBlock != null){
				this.srcBlock.setBlockInfo(srcBlock);
			}
		}

		public void setDstBlock(BlockInfo dstBlock) {
			if (dstBlock != null){
				this.dstBlock.setBlockInfo(dstBlock);
			}
		}

		public BlockInfo getSrcBlock() {
			return srcBlock;
		}

		public BlockInfo getDstBlock() {
			return dstBlock;
		}		
	}	
	


	public static class GetBlockRes implements NameNodeProtocol.NameNodeRpcMessage {
		public static int CSIZE = BlockInfo.CSIZE;
		
		private BlockInfo blockInfo;
		private short error;
		
		public GetBlockRes() {
			this.blockInfo = new BlockInfo();
			this.error = 0;
		}
		
		public int size() {
			return CSIZE;
		}
		
		public short getType(){
			return NameNodeProtocol.RES_GET_BLOCK;
		}
		
		public int write(ByteBuffer buffer) {
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

		public BlockInfo getBlockInfo() {
			return blockInfo;
		}

		public void setBlockInfo(BlockInfo blockInfo) {
			if (blockInfo != null){
				this.blockInfo.setBlockInfo(blockInfo);
			} 
		}
		
		public short getError(){
			return error;
		}

		public void setError(short error) {
			this.error = error;
		}

	}	
	
	public static class GetLocationRes implements NameNodeProtocol.NameNodeRpcMessage {
		public static int CSIZE = BlockInfo.CSIZE + 8;
		
		private BlockInfo blockInfo;
		protected long fd;
		private short error;
		
		public GetLocationRes() {
			this.blockInfo = new BlockInfo();
			this.fd = 0;		
			this.error = 0;
		}
		
		public int size() {
			return CSIZE;
		}
		
		public short getType(){
			return NameNodeProtocol.RES_GET_LOCATION;
		}
		
		public int write(ByteBuffer buffer) {
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

		public BlockInfo getBlockInfo() throws IOException {
			return blockInfo;
		}

		public void setBlockInfo(BlockInfo blockInfo) {
			if (blockInfo != null){
				this.blockInfo.setBlockInfo(blockInfo);
			} 
		}
		
		public long getFd() {
			return fd;
		}

		public void setFd(long fd) {
			this.fd = fd;
		}

		public short getError(){
			return error;
		}

		public void setError(short error) {
			this.error = error;
		}		
	}	
	
	public static class GetDataNodeRes implements NameNodeProtocol.NameNodeRpcMessage {
		public static int CSIZE = DataNodeStatistics.CSIZE;
		
		private DataNodeStatistics statistics;

		public GetDataNodeRes() {
			this.statistics = new DataNodeStatistics();
		}
		
		public void setError(short error) {
			
		}

		public int size() {
			return CSIZE;
		}
		
		public short getType(){
			return NameNodeProtocol.RES_GET_DATANODE;
		}	
		
		public int write(ByteBuffer buffer) {
			int written = statistics.write(buffer);
			return written;
		}		

		public void update(ByteBuffer buffer) {
			try {
				statistics.update(buffer);
			} catch (UnknownHostException e) {
				e.printStackTrace();
			}
		}

		public DataNodeStatistics getStatistics() {
			return this.statistics;
		}
		
		public void setFreeBlockCount(int blockCount) {
			this.statistics.setFreeBlockCount(blockCount);
		}
	}	
	
	public static class PingNameNodeRes implements NameNodeProtocol.NameNodeRpcMessage {
		public static int CSIZE = 4;
		
		private int data;
		private short error;
		
		public PingNameNodeRes() {
			this.data = 0;
			this.error = 0;
		}

		public int size() {
			return CSIZE;
		}
		
		public short getType(){
			return NameNodeProtocol.RES_PING_NAMENODE;
		}
		
		public int write(ByteBuffer buffer) {
			buffer.putInt(data);
			return CSIZE;
		}		

		public void update(ByteBuffer buffer) {
			data = buffer.getInt();
		}

		public int getData() {
			return data;
		}
		
		public void setData(int data){
			this.data = data;
		}

		public short getError(){
			return error;
		}

		public void setError(short error) {
			this.error = error;
		}
	}
}
