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

package com.ibm.crail.namenode;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.DelayQueue;

import org.slf4j.Logger;

import com.ibm.crail.CrailNodeType;
import com.ibm.crail.conf.CrailConstants;
import com.ibm.crail.namenode.protocol.BlockInfo;
import com.ibm.crail.namenode.protocol.DataNodeInfo;
import com.ibm.crail.namenode.protocol.FileInfo;
import com.ibm.crail.namenode.protocol.FileName;
import com.ibm.crail.namenode.rpc.NameNodeProtocol;
import com.ibm.crail.namenode.rpc.RpcNameNodeService;
import com.ibm.crail.namenode.rpc.RpcNameNodeState;
import com.ibm.crail.namenode.rpc.RpcRequestMessage;
import com.ibm.crail.namenode.rpc.RpcResponseMessage;
import com.ibm.crail.utils.CrailUtils;

public class NameNodeService implements RpcNameNodeService {
	private static final Logger LOG = CrailUtils.getLogger();
	
	//data structures for datanodes, blocks, files
	private BlockStore blockStore;
	private DelayQueue<AbstractNode> deleteQueue;
	private FileStore fileTree;
	private ConcurrentHashMap<Long, AbstractNode> fileTable;	

	
	NameNodeService(DelayQueue<AbstractNode> deleteQueue) throws IOException {
		this.blockStore = new BlockStore();
		this.deleteQueue = deleteQueue;
		this.fileTree = new FileStore();
		this.fileTable = new ConcurrentHashMap<Long, AbstractNode>();
		
		AbstractNode root = fileTree.getRoot();
		fileTable.put(root.getFd(), root);
	}
	
	@Override
	public short createFile(RpcRequestMessage.CreateFileReq request, RpcResponseMessage.CreateFileRes response, RpcNameNodeState errorState) throws Exception {
		//check protocol
		if (!NameNodeProtocol.verifyProtocol(NameNodeProtocol.CMD_CREATE_FILE, request, response)) {
			return NameNodeProtocol.ERR_PROTOCOL_MISMATCH;
		}

		//get params
		FileName fileHash = request.getFileName();
		CrailNodeType type = request.getFileType();
		boolean writeable = type.isDirectory() ? false : true; 
		int storageAffinity = request.getStorageAffinity();
		int locationAffinity = request.getLocationAffinity();
		
		//check params
		if (type.isContainer() && locationAffinity > 0){
			return NameNodeProtocol.ERR_DIR_LOCATION_AFFINITY_MISMATCH;
		}
		
		//rpc
		AbstractNode parentInfo = fileTree.retrieveParent(fileHash, errorState);
		if (errorState.getError() != NameNodeProtocol.ERR_OK){
			return errorState.getError();
		}		
		if (parentInfo == null) {
			return NameNodeProtocol.ERR_PARENT_MISSING;
		} 	
		if (!parentInfo.getType().isContainer()){
			return NameNodeProtocol.ERR_PARENT_NOT_DIR;
		}
		
		AbstractNode fileInfo = FileBlocks.createNode(fileHash.getFileComponent(), type);
		if (!parentInfo.addChild(fileInfo)){
			return NameNodeProtocol.ERR_FILE_EXISTS;
		}
		
		BlockInfo fileBlock = blockStore.getBlock(storageAffinity, locationAffinity);
		if (fileBlock == null){
			return NameNodeProtocol.ERR_NO_FREE_BLOCKS;
		}			
		if (!fileInfo.addBlock(0, fileBlock)){
			return NameNodeProtocol.ERR_ADD_BLOCK_FAILED;
		}
		
		int index = CrailUtils.computeIndex(fileInfo.getDirOffset());
		BlockInfo parentBlock = parentInfo.getBlock(index);
		if (parentBlock == null){
			parentBlock = blockStore.getBlock(0, 0);
			if (parentBlock == null){
				return NameNodeProtocol.ERR_NO_FREE_BLOCKS;
			}			
			if (!parentInfo.addBlock(index, parentBlock)){
				blockStore.addBlock(parentBlock);
				parentBlock = parentInfo.getBlock(index);
				if (parentBlock == null){
					blockStore.addBlock(fileBlock);
					return NameNodeProtocol.ERR_CREATE_FILE_FAILED;
				}
			}
		}
		parentInfo.incCapacity(CrailConstants.DIRECTORY_RECORD);
		fileTable.put(fileInfo.getFd(), fileInfo);
		
		if (writeable) {
			fileInfo.updateToken();
			response.shipToken(true);
		} else {
			response.shipToken(false);
		}
		response.setParentInfo(parentInfo);
		response.setFileInfo(fileInfo);
		response.setFileBlock(fileBlock);
		response.setDirBlock(parentBlock);
		
		if (CrailConstants.DEBUG){
			LOG.info("createFile: fd " + fileInfo.getFd() + ", parent " + parentInfo.getFd() + ", writeable " + writeable + ", token " + fileInfo.getToken() + ", capacity " + fileInfo.getCapacity() + ", dirOffset " + fileInfo.getDirOffset());
		}	
		
		return NameNodeProtocol.ERR_OK;
	}	
	
	@Override
	public short getFile(RpcRequestMessage.GetFileReq request, RpcResponseMessage.GetFileRes response, RpcNameNodeState errorState) throws Exception {
		//check protocol
		if (!NameNodeProtocol.verifyProtocol(NameNodeProtocol.CMD_GET_FILE, request, response)){
			return NameNodeProtocol.ERR_PROTOCOL_MISMATCH;
		}		
		
		//get params
		FileName fileHash = request.getFileName();
		boolean writeable = request.isWriteable();

		//rpc
		AbstractNode fileInfo = fileTree.retrieveFile(fileHash, errorState);
		if (errorState.getError() != NameNodeProtocol.ERR_OK){
			return errorState.getError();
		}		
		if (fileInfo == null){
			return NameNodeProtocol.ERR_GET_FILE_FAILED;
		}
		if (writeable && !fileInfo.tokenFree()){
			return NameNodeProtocol.ERR_TOKEN_TAKEN;			
		} 
		
		if (writeable){
			fileInfo.updateToken();
		}
		fileTable.put(fileInfo.getFd(), fileInfo);
		
		BlockInfo fileBlock = fileInfo.getBlock(0);
		
		response.setFileInfo(fileInfo);
		response.setFileBlock(fileBlock);
		if (writeable){
			response.shipToken();
		}
		
		if (CrailConstants.DEBUG){
			LOG.info("getFile: fd " + fileInfo.getFd() + ", isDir " + fileInfo.getType().isDirectory() + ", token " + fileInfo.getToken() + ", capacity " + fileInfo.getCapacity());
		}			
		
		return NameNodeProtocol.ERR_OK;
	}
	
	@Override
	public short setFile(RpcRequestMessage.SetFileReq request, RpcResponseMessage.VoidRes response, RpcNameNodeState errorState) throws Exception {
		//check protocol
		if (!NameNodeProtocol.verifyProtocol(NameNodeProtocol.CMD_SET_FILE, request, response)){
			return NameNodeProtocol.ERR_PROTOCOL_MISMATCH;
		}		
		
		//get params
		FileInfo fileInfo = request.getFileInfo();
		boolean close = request.isClose();

		//rpc
		AbstractNode storedFile = fileTable.get(fileInfo.getFd());
		if (storedFile == null){
			return NameNodeProtocol.ERR_FILE_NOT_OPEN;			
		}
		
		if (!storedFile.getType().isDirectory() && storedFile.getToken() > 0 && storedFile.getToken() == fileInfo.getToken()){
			storedFile.setCapacity(fileInfo.getCapacity());	
		}
		
		if (close){
			storedFile.resetToken();
		}
		
		if (CrailConstants.DEBUG){
			LOG.info("setFile: " + fileInfo.toString() + ", close " + close);
		}
		
		return NameNodeProtocol.ERR_OK;
	}

	@Override
	public short removeFile(RpcRequestMessage.RemoveFileReq request, RpcResponseMessage.DeleteFileRes response, RpcNameNodeState errorState) throws Exception {
		//check protocol
		if (!NameNodeProtocol.verifyProtocol(NameNodeProtocol.CMD_REMOVE_FILE, request, response)){
			return NameNodeProtocol.ERR_PROTOCOL_MISMATCH;
		}		
		
		//get params
		FileName fileHash = request.getFileName();
		
		//rpc
		AbstractNode parentInfo = fileTree.retrieveParent(fileHash, errorState);
		if (errorState.getError() != NameNodeProtocol.ERR_OK){
			return errorState.getError();
		}		
		if (parentInfo == null) {
			return NameNodeProtocol.ERR_CREATE_FILE_FAILED;
		} 		
		
		AbstractNode fileInfo = fileTree.retrieveFile(fileHash, errorState);
		if (errorState.getError() != NameNodeProtocol.ERR_OK){
			return errorState.getError();
		}		
		if (fileInfo == null){
			return NameNodeProtocol.ERR_GET_FILE_FAILED;
		}	
		
		response.setParentInfo(parentInfo);
		response.setFileInfo(fileInfo);
		
		fileInfo = parentInfo.removeChild(fileInfo);
		if (fileInfo == null){
			return NameNodeProtocol.ERR_GET_FILE_FAILED;
		}
		
		fileTable.remove(fileInfo.getFd());
		appendToDeleteQueue(fileInfo);
		
		if (CrailConstants.DEBUG){
			LOG.info("removeFile: filename, fd " + fileInfo.getFd());
		}	
		
		return NameNodeProtocol.ERR_OK;
	}	
	
	@Override
	public short renameFile(RpcRequestMessage.RenameFileReq request, RpcResponseMessage.RenameRes response, RpcNameNodeState errorState) throws Exception {
		//check protocol
		if (!NameNodeProtocol.verifyProtocol(NameNodeProtocol.CMD_RENAME_FILE, request, response)){
			return NameNodeProtocol.ERR_PROTOCOL_MISMATCH;
		}	
		
		//get params
		FileName srcFileHash = request.getSrcFileName();
		FileName dstFileHash = request.getDstFileName();
		
		//rpc
		AbstractNode srcParent = fileTree.retrieveParent(srcFileHash, errorState);
		if (errorState.getError() != NameNodeProtocol.ERR_OK){
			return errorState.getError();
		}		
		if (srcParent == null) {
			return NameNodeProtocol.ERR_GET_FILE_FAILED;
		} 		
		
		AbstractNode srcFile = fileTree.retrieveFile(srcFileHash, errorState);
		if (errorState.getError() != NameNodeProtocol.ERR_OK){
			return errorState.getError();
		}		
		if (srcFile == null){
			return NameNodeProtocol.ERR_SRC_FILE_NOT_FOUND;
		}
		
		//directory block
		int index = CrailUtils.computeIndex(srcFile.getDirOffset());
		BlockInfo srcBlock = srcParent.getBlock(index);
		if (srcBlock == null){
			return NameNodeProtocol.ERR_GET_FILE_FAILED;
		}
		//end
		
		response.setSrcParent(srcParent);
		response.setSrcFile(srcFile);
		response.setSrcBlock(srcBlock);
		
		AbstractNode dstParent = fileTree.retrieveParent(dstFileHash, errorState);
		if (errorState.getError() != NameNodeProtocol.ERR_OK){
			return errorState.getError();
		}		
		if (dstParent == null) {
			return NameNodeProtocol.ERR_GET_FILE_FAILED;
		} 
		
		AbstractNode dstFile = fileTree.retrieveFile(dstFileHash, errorState);
		if (dstFile != null && !dstFile.getType().isDirectory()){
			return NameNodeProtocol.ERR_FILE_EXISTS;
		}		
		if (dstFile != null && dstFile.getType().isDirectory()){
			dstParent = dstFile;
		} 
		
		srcFile = srcParent.removeChild(srcFile);
		if (srcFile == null){
			return NameNodeProtocol.ERR_SRC_FILE_NOT_FOUND;
		}
		srcFile.rename(dstFileHash.getFileComponent());
		if (!dstParent.addChild(srcFile)){
			return NameNodeProtocol.ERR_FILE_EXISTS;
		} else {
			dstFile = srcFile;
		}
		
		//directory block
		index = CrailUtils.computeIndex(srcFile.getDirOffset());
		BlockInfo dstBlock = dstParent.getBlock(index);
		if (dstBlock == null){
			dstBlock = blockStore.getBlock(0, 0);
			if (dstBlock == null){
				return NameNodeProtocol.ERR_NO_FREE_BLOCKS;
			}			
			if (!dstParent.addBlock(index, dstBlock)){
				blockStore.addBlock(dstBlock);
				dstBlock = dstParent.getBlock(index);
				if (dstBlock == null){
					blockStore.addBlock(srcBlock);
					return NameNodeProtocol.ERR_CREATE_FILE_FAILED;
				}
			} 
		}
		dstParent.incCapacity(CrailConstants.DIRECTORY_RECORD);
		//end
		
		response.setDstParent(dstParent);
		response.setDstFile(dstFile);
		response.setDstBlock(dstBlock);
		
		if (response.getDstParent().getCapacity() < response.getDstFile().getDirOffset() + CrailConstants.DIRECTORY_RECORD){
			LOG.info("rename: parent capacity does not match dst file offset, capacity " + response.getDstParent().getCapacity() + ", offset " + response.getDstFile().getDirOffset() + ", capacity " + dstParent.getCapacity() + ", offset " + dstFile.getDirOffset());
		}
		
		if (CrailConstants.DEBUG){
			LOG.info("renameFile: src-parent " + srcParent.getFd() + ", src-file " + srcFile.getFd() + ", dst-parent " + dstParent.getFd() + ", dst-fd " + dstFile.getFd());
		}	
		
		return NameNodeProtocol.ERR_OK;
	}	
	
	@Override
	public short getDataNode(RpcRequestMessage.GetDataNodeReq request, RpcResponseMessage.GetDataNodeRes response, RpcNameNodeState errorState) throws Exception {
		//check protocol
		if (!NameNodeProtocol.verifyProtocol(NameNodeProtocol.CMD_GET_DATANODE, request, response)){
			return NameNodeProtocol.ERR_PROTOCOL_MISMATCH;
		}			
		
		//get params
		DataNodeInfo dnInfo = request.getInfo();
		
		//rpc
		DataNodeBlocks dnInfoNn = blockStore.getDataNode(dnInfo);
		if (dnInfoNn == null){
			return NameNodeProtocol.ERR_DATANODE_NOT_REGISTERED;
		}
		
		response.setFreeBlockCount(dnInfoNn.getBlockCount());
		
		return NameNodeProtocol.ERR_OK;
	}	

	@Override
	public short setBlock(RpcRequestMessage.SetBlockReq request, RpcResponseMessage.VoidRes response, RpcNameNodeState errorState) throws Exception {
		//check protocol
		if (!NameNodeProtocol.verifyProtocol(NameNodeProtocol.CMD_SET_BLOCK, request, response)){
			return NameNodeProtocol.ERR_PROTOCOL_MISMATCH;
		}		
		
		//get params
		BlockInfo blockInfo = request.getBlockInfo();
		DataNodeInfo dnInfoExt = new DataNodeInfo(blockInfo.getDnInfo().getStorageTier(), blockInfo.getDnInfo().getLocationAffinity(), blockInfo.getDnInfo().getIpAddress(), blockInfo.getDnInfo().getPort());
		
		//rpc
		int realBlocks = (int) (((long) blockInfo.getLength()) / CrailConstants.BLOCK_SIZE) ;
		long offset = 0;
		short error = NameNodeProtocol.ERR_OK;
		for (int i = 0; i < realBlocks; i++){
			long newAddr = blockInfo.getAddr() + offset;
			BlockInfo nnBlock = new BlockInfo(dnInfoExt, newAddr, (int) CrailConstants.BLOCK_SIZE, blockInfo.getLkey());
			error = blockStore.addBlock(nnBlock);
			offset += CrailConstants.BLOCK_SIZE;
			
			if (error != NameNodeProtocol.ERR_OK){
				break;
			}
		}
		
		return error;
	}

	@Override
	public short getBlock(RpcRequestMessage.GetBlockReq request, RpcResponseMessage.GetBlockRes response, RpcNameNodeState errorState) throws Exception {
		//check protocol
		if (!NameNodeProtocol.verifyProtocol(NameNodeProtocol.CMD_GET_BLOCK, request, response)){
			return NameNodeProtocol.ERR_PROTOCOL_MISMATCH;
		}			
		
		//get params
		long fd = request.getFd();
		long token = request.getToken();
		long position = request.getPosition();
		int storageAffinity = request.getStorageAffinity();
		int locationAffinitiy = request.getLocationAffinity();
		long capacity = request.getCapacity();
		
		//check params
		if (position < 0){
			return NameNodeProtocol.ERR_POSITION_NEGATIV;
		}
	
		//rpc
		AbstractNode fileInfo = fileTable.get(fd);
		if (fileInfo == null){
			return NameNodeProtocol.ERR_FILE_NOT_OPEN;			
		}
		
		int index = CrailUtils.computeIndex(position);
		if (index < 0){
			return NameNodeProtocol.ERR_POSITION_NEGATIV;			
		}
		
		BlockInfo block = fileInfo.getBlock(index);
		if (block == null && fileInfo.getToken() == token){
			block = blockStore.getBlock(storageAffinity, locationAffinitiy);
			if (block == null){
				return NameNodeProtocol.ERR_NO_FREE_BLOCKS;
			}
			if (!fileInfo.addBlock(index, block)){
				return NameNodeProtocol.ERR_ADD_BLOCK_FAILED;
			}
			block = fileInfo.getBlock(index);
			if (block == null){
				return NameNodeProtocol.ERR_ADD_BLOCK_FAILED;
			}
			fileInfo.setCapacity(capacity);
		} else if (block == null && token > 0){ 
			return NameNodeProtocol.ERR_TOKEN_MISMATCH;
		} else if (block == null && token == 0){ 
			return NameNodeProtocol.ERR_CAPACITY_EXCEEDED;
		} 
		
		response.setBlockInfo(block);
		return NameNodeProtocol.ERR_OK;
	}
	
	@Override
	public short getLocation(RpcRequestMessage.GetLocationReq request, RpcResponseMessage.GetLocationRes response, RpcNameNodeState errorState) throws Exception {
		//check protocol
		if (!NameNodeProtocol.verifyProtocol(NameNodeProtocol.CMD_GET_LOCATION, request, response)){
			return NameNodeProtocol.ERR_PROTOCOL_MISMATCH;
		}			
		
		//get params
		FileName fileName = request.getFileName();
		long position = request.getPosition();
		
		//check params
		if (position < 0){
			return NameNodeProtocol.ERR_POSITION_NEGATIV;
		}	
		
		//rpc
		AbstractNode fileInfo = fileTree.retrieveFile(fileName, errorState);
		if (errorState.getError() != NameNodeProtocol.ERR_OK){
			return errorState.getError();
		}		
		if (fileInfo == null){
			return NameNodeProtocol.ERR_GET_FILE_FAILED;
		}	
		
		int index = CrailUtils.computeIndex(position);
		if (index < 0){
			return NameNodeProtocol.ERR_POSITION_NEGATIV;			
		}		
		BlockInfo block = fileInfo.getBlock(index);
		if (block == null){
			return NameNodeProtocol.ERR_OFFSET_TOO_LARGE;
		}
		
		response.setBlockInfo(block);
		
		return NameNodeProtocol.ERR_OK;
	}

	//------------------------
	
	@Override
	public short dump(RpcRequestMessage.DumpNameNodeReq request, RpcResponseMessage.VoidRes response, RpcNameNodeState errorState) throws Exception {
		if (!NameNodeProtocol.verifyProtocol(NameNodeProtocol.CMD_DUMP_NAMENODE, request, response)){
			return NameNodeProtocol.ERR_PROTOCOL_MISMATCH;
		}			
		
		System.out.println("#fd\t\tfilecomp\t\tcapacity\t\tisdir\t\t\tdiroffset");
		fileTree.dump();
		System.out.println("#fd\t\tfilecomp\t\tcapacity\t\tisdir\t\t\tdiroffset");
		dumpFastMap();
		
		return NameNodeProtocol.ERR_OK;
	}	
	
	@Override
	public short ping(RpcRequestMessage.PingNameNodeReq request, RpcResponseMessage.PingNameNodeRes response, RpcNameNodeState errorState) throws Exception {
		if (!NameNodeProtocol.verifyProtocol(NameNodeProtocol.CMD_PING_NAMENODE, request, response)){
			return NameNodeProtocol.ERR_PROTOCOL_MISMATCH;
		}	
		
		response.setData(request.getOp()+1);
		
		return NameNodeProtocol.ERR_OK;
	}
	
	
	//--------------- helper functions
	
	void appendToDeleteQueue(AbstractNode fileInfo) throws Exception {
		if (fileInfo != null) {
			fileInfo.setDelay(CrailConstants.TOKEN_EXPIRATION);
			deleteQueue.add(fileInfo);			
		}
	}	
	
	void freeFile(AbstractNode fileInfo) throws Exception {
		if (fileInfo != null) {
			fileInfo.freeBlocks(blockStore);
		}
	}

	private void dumpFastMap(){
		for (Long key : fileTable.keySet()){
			AbstractNode file = fileTable.get(key);
			System.out.println(file.toString());
		}		
	}
}
