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

import java.io.IOException;
import java.net.URI;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.crail.CrailNodeType;
import org.apache.crail.conf.CrailConstants;
import org.apache.crail.metadata.BlockInfo;
import org.apache.crail.metadata.DataNodeInfo;
import org.apache.crail.metadata.FileInfo;
import org.apache.crail.metadata.FileName;
import org.apache.crail.rpc.RpcErrors;
import org.apache.crail.rpc.RpcNameNodeService;
import org.apache.crail.rpc.RpcNameNodeState;
import org.apache.crail.rpc.RpcProtocol;
import org.apache.crail.rpc.RpcRequestMessage;
import org.apache.crail.rpc.RpcResponseMessage;
import org.apache.crail.utils.CrailUtils;
import org.slf4j.Logger;

public class NameNodeService implements RpcNameNodeService, Sequencer {
	private static final Logger LOG = CrailUtils.getLogger();
	
	//data structures for datanodes, blocks, files
	private long serviceId;
	private long serviceSize;
	private AtomicLong sequenceId;
	private BlockStore blockStore;
	private DelayQueue<AbstractNode> deleteQueue;
	private FileStore fileTree;
	private ConcurrentHashMap<Long, AbstractNode> fileTable;	
	private GCServer gcServer;
	
	public NameNodeService() throws IOException {
		URI uri = URI.create(CrailConstants.NAMENODE_ADDRESS);
		String query = uri.getRawQuery();
		StringTokenizer tokenizer = new StringTokenizer(query, "&");
		this.serviceId = Long.parseLong(tokenizer.nextToken().substring(3));
		this.serviceSize = Long.parseLong(tokenizer.nextToken().substring(5));
		this.sequenceId = new AtomicLong(serviceId);
		this.blockStore = new BlockStore();
		this.deleteQueue = new DelayQueue<AbstractNode>();
		this.fileTree = new FileStore(this);
		this.fileTable = new ConcurrentHashMap<Long, AbstractNode>();
		this.gcServer = new GCServer(this, deleteQueue);
		
		AbstractNode root = fileTree.getRoot();
		fileTable.put(root.getFd(), root);
		Thread gc = new Thread(gcServer);
		gc.start();				
	}
	
	public long getNextId(){
		return sequenceId.getAndAdd(serviceSize);
	}

	@Override
	public short createFile(RpcRequestMessage.CreateFileReq request, RpcResponseMessage.CreateFileRes response, RpcNameNodeState errorState) throws Exception {
		//check protocol
		if (!RpcProtocol.verifyProtocol(RpcProtocol.CMD_CREATE_FILE, request, response)) {
			return RpcErrors.ERR_PROTOCOL_MISMATCH;
		}

		//get params
		FileName fileHash = request.getFileName();
		CrailNodeType type = request.getFileType();
		boolean writeable = type.isDirectory() ? false : true; 
		int storageClass = request.getStorageClass();
		int locationClass = request.getLocationClass();
		boolean enumerable = request.isEnumerable();
		
		//check params
		if (type.isContainer() && locationClass > 0){
			return RpcErrors.ERR_DIR_LOCATION_AFFINITY_MISMATCH;
		}
		
		//rpc
		AbstractNode parentInfo = fileTree.retrieveParent(fileHash, errorState);
		if (errorState.getError() != RpcErrors.ERR_OK){
			return errorState.getError();
		}		
		if (parentInfo == null) {
			return RpcErrors.ERR_PARENT_MISSING;
		} 	
		if (!parentInfo.getType().isContainer()){
			return RpcErrors.ERR_PARENT_NOT_DIR;
		}
		
		if (storageClass < 0){
			storageClass = parentInfo.getStorageClass();
		}
		if (locationClass < 0){
			locationClass = parentInfo.getLocationClass();
		}
		
		AbstractNode fileInfo = fileTree.createNode(fileHash.getFileComponent(), type, storageClass, locationClass, enumerable);
		try {
			AbstractNode oldNode = parentInfo.putChild(fileInfo);
			if (oldNode != null && oldNode.getFd() != fileInfo.getFd()){
				appendToDeleteQueue(oldNode);				
			}		
		} catch(Exception e){
			return RpcErrors.ERR_FILE_EXISTS;
		}
		fileTable.put(fileInfo.getFd(), fileInfo);
		
		NameNodeBlockInfo fileBlock = blockStore.getBlock(fileInfo.getStorageClass(), fileInfo.getLocationClass());
		if (fileBlock == null){
			return RpcErrors.ERR_NO_FREE_BLOCKS;
		}			
		if (!fileInfo.addBlock(0, fileBlock)){
			return RpcErrors.ERR_ADD_BLOCK_FAILED;
		}
		
		NameNodeBlockInfo parentBlock = null;
		if (fileInfo.getDirOffset() >= 0){
			int index = CrailUtils.computeIndex(fileInfo.getDirOffset());
			parentBlock = parentInfo.getBlock(index);
			if (parentBlock == null){
				parentBlock = blockStore.getBlock(parentInfo.getStorageClass(), parentInfo.getLocationClass());
				if (parentBlock == null){
					return RpcErrors.ERR_NO_FREE_BLOCKS;
				}			
				if (!parentInfo.addBlock(index, parentBlock)){
					blockStore.addBlock(parentBlock);
					parentBlock = parentInfo.getBlock(index);
					if (parentBlock == null){
						blockStore.addBlock(fileBlock);
						return RpcErrors.ERR_CREATE_FILE_FAILED;
					}
				}
			}
			parentInfo.incCapacity(CrailConstants.DIRECTORY_RECORD);
		}
		
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
		
		return RpcErrors.ERR_OK;
	}	
	
	@Override
	public short getFile(RpcRequestMessage.GetFileReq request, RpcResponseMessage.GetFileRes response, RpcNameNodeState errorState) throws Exception {
		//check protocol
		if (!RpcProtocol.verifyProtocol(RpcProtocol.CMD_GET_FILE, request, response)){
			return RpcErrors.ERR_PROTOCOL_MISMATCH;
		}		
		
		//get params
		FileName fileHash = request.getFileName();
		boolean writeable = request.isWriteable();

		//rpc
		AbstractNode fileInfo = fileTree.retrieveFile(fileHash, errorState);
		if (errorState.getError() != RpcErrors.ERR_OK){
			return errorState.getError();
		}		
		if (fileInfo == null){
			return RpcErrors.ERR_GET_FILE_FAILED;
		}
		if (writeable && !fileInfo.tokenFree()){
			return RpcErrors.ERR_TOKEN_TAKEN;			
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
		
		return RpcErrors.ERR_OK;
	}
	
	@Override
	public short setFile(RpcRequestMessage.SetFileReq request, RpcResponseMessage.VoidRes response, RpcNameNodeState errorState) throws Exception {
		//check protocol
		if (!RpcProtocol.verifyProtocol(RpcProtocol.CMD_SET_FILE, request, response)){
			return RpcErrors.ERR_PROTOCOL_MISMATCH;
		}		
		
		//get params
		FileInfo fileInfo = request.getFileInfo();
		boolean close = request.isClose();

		//rpc
		AbstractNode storedFile = fileTable.get(fileInfo.getFd());
		if (storedFile == null){
			return RpcErrors.ERR_FILE_NOT_OPEN;			
		}
		
		if (storedFile.getToken() > 0 && storedFile.getToken() == fileInfo.getToken()){
			storedFile.setCapacity(fileInfo.getCapacity());	
		}		
		if (close){
			storedFile.resetToken();
		}
		
		if (CrailConstants.DEBUG){
			LOG.info("setFile: " + fileInfo.toString() + ", close " + close);
		}
		
		return RpcErrors.ERR_OK;
	}

	@Override
	public short removeFile(RpcRequestMessage.RemoveFileReq request, RpcResponseMessage.DeleteFileRes response, RpcNameNodeState errorState) throws Exception {
		//check protocol
		if (!RpcProtocol.verifyProtocol(RpcProtocol.CMD_REMOVE_FILE, request, response)){
			return RpcErrors.ERR_PROTOCOL_MISMATCH;
		}		
		
		//get params
		FileName fileHash = request.getFileName();
		
		//rpc
		AbstractNode parentInfo = fileTree.retrieveParent(fileHash, errorState);
		if (errorState.getError() != RpcErrors.ERR_OK){
			return errorState.getError();
		}		
		if (parentInfo == null) {
			return RpcErrors.ERR_CREATE_FILE_FAILED;
		} 		
		
		AbstractNode fileInfo = fileTree.retrieveFile(fileHash, errorState);
		if (errorState.getError() != RpcErrors.ERR_OK){
			return errorState.getError();
		}		
		if (fileInfo == null){
			return RpcErrors.ERR_GET_FILE_FAILED;
		}	
		
		response.setParentInfo(parentInfo);
		response.setFileInfo(fileInfo);
		
		fileInfo = parentInfo.removeChild(fileInfo.getComponent());
		if (fileInfo == null){
			return RpcErrors.ERR_GET_FILE_FAILED;
		}
		
		appendToDeleteQueue(fileInfo);
		
		if (CrailConstants.DEBUG){
			LOG.info("removeFile: filename, fd " + fileInfo.getFd());
		}	
		
		return RpcErrors.ERR_OK;
	}	
	
	@Override
	public short renameFile(RpcRequestMessage.RenameFileReq request, RpcResponseMessage.RenameRes response, RpcNameNodeState errorState) throws Exception {
		//check protocol
		if (!RpcProtocol.verifyProtocol(RpcProtocol.CMD_RENAME_FILE, request, response)){
			return RpcErrors.ERR_PROTOCOL_MISMATCH;
		}	
		
		//get params
		FileName srcFileHash = request.getSrcFileName();
		FileName dstFileHash = request.getDstFileName();
		
		//rpc
		AbstractNode srcParent = fileTree.retrieveParent(srcFileHash, errorState);
		if (errorState.getError() != RpcErrors.ERR_OK){
			return errorState.getError();
		}		
		if (srcParent == null) {
			return RpcErrors.ERR_GET_FILE_FAILED;
		} 		
		
		AbstractNode srcFile = fileTree.retrieveFile(srcFileHash, errorState);
		if (errorState.getError() != RpcErrors.ERR_OK){
			return errorState.getError();
		}		
		if (srcFile == null){
			return RpcErrors.ERR_SRC_FILE_NOT_FOUND;
		}
		
		//directory block
		int index = CrailUtils.computeIndex(srcFile.getDirOffset());
		NameNodeBlockInfo srcBlock = srcParent.getBlock(index);
		if (srcBlock == null){
			return RpcErrors.ERR_GET_FILE_FAILED;
		}
		//end
		
		response.setSrcParent(srcParent);
		response.setSrcFile(srcFile);
		response.setSrcBlock(srcBlock);
		
		AbstractNode dstParent = fileTree.retrieveParent(dstFileHash, errorState);
		if (errorState.getError() != RpcErrors.ERR_OK){
			return errorState.getError();
		}		
		if (dstParent == null) {
			return RpcErrors.ERR_GET_FILE_FAILED;
		} 
		
		AbstractNode dstFile = fileTree.retrieveFile(dstFileHash, errorState);
		if (dstFile != null && !dstFile.getType().isDirectory()){
			return RpcErrors.ERR_FILE_EXISTS;
		}		
		if (dstFile != null && dstFile.getType().isDirectory()){
			dstParent = dstFile;
		} 
		
		srcFile = srcParent.removeChild(srcFile.getComponent());
		if (srcFile == null){
			return RpcErrors.ERR_SRC_FILE_NOT_FOUND;
		}
		srcFile.rename(dstFileHash.getFileComponent());
		try {
			AbstractNode oldNode = dstParent.putChild(srcFile);
			if (oldNode != null && oldNode.getFd() != srcFile.getFd()){
				appendToDeleteQueue(oldNode);				
			}				
			dstFile = srcFile;
		} catch(Exception e){
			return RpcErrors.ERR_FILE_EXISTS;
		}
		
		//directory block
		index = CrailUtils.computeIndex(srcFile.getDirOffset());
		NameNodeBlockInfo dstBlock = dstParent.getBlock(index);
		if (dstBlock == null){
			dstBlock = blockStore.getBlock(dstParent.getStorageClass(), dstParent.getLocationClass());
			if (dstBlock == null){
				return RpcErrors.ERR_NO_FREE_BLOCKS;
			}			
			if (!dstParent.addBlock(index, dstBlock)){
				blockStore.addBlock(dstBlock);
				dstBlock = dstParent.getBlock(index);
				if (dstBlock == null){
					blockStore.addBlock(srcBlock);
					return RpcErrors.ERR_CREATE_FILE_FAILED;
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
		
		return RpcErrors.ERR_OK;
	}	
	
	@Override
	public short getDataNode(RpcRequestMessage.GetDataNodeReq request, RpcResponseMessage.GetDataNodeRes response, RpcNameNodeState errorState) throws Exception {
		//check protocol
		if (!RpcProtocol.verifyProtocol(RpcProtocol.CMD_GET_DATANODE, request, response)){
			return RpcErrors.ERR_PROTOCOL_MISMATCH;
		}			
		
		//get params
		DataNodeInfo dnInfo = request.getInfo();
		
		//rpc
		DataNodeBlocks dnInfoNn = blockStore.getDataNode(dnInfo);
		if (dnInfoNn == null){
			return RpcErrors.ERR_DATANODE_NOT_REGISTERED;
		}
		
		dnInfoNn.touch();
		response.setServiceId(serviceId);
		response.setFreeBlockCount(dnInfoNn.getBlockCount());
		
		return RpcErrors.ERR_OK;
	}	

	@Override
	public short setBlock(RpcRequestMessage.SetBlockReq request, RpcResponseMessage.VoidRes response, RpcNameNodeState errorState) throws Exception {
		//check protocol
		if (!RpcProtocol.verifyProtocol(RpcProtocol.CMD_SET_BLOCK, request, response)){
			return RpcErrors.ERR_PROTOCOL_MISMATCH;
		}		
		
		//get params
		BlockInfo region = new BlockInfo();
		region.setBlockInfo(request.getBlockInfo());
		
		short error = RpcErrors.ERR_OK;
		if (blockStore.regionExists(region)){
			error = blockStore.updateRegion(region);
		} else {
			//rpc
			int realBlocks = (int) (((long) region.getLength()) / CrailConstants.BLOCK_SIZE) ;
			long offset = 0;
			for (int i = 0; i < realBlocks; i++){
				NameNodeBlockInfo nnBlock = new NameNodeBlockInfo(region, offset, (int) CrailConstants.BLOCK_SIZE);
				error = blockStore.addBlock(nnBlock);
				offset += CrailConstants.BLOCK_SIZE;
				
				if (error != RpcErrors.ERR_OK){
					break;
				}
			}
		}
		
		return error;
	}

	@Override
	public short getBlock(RpcRequestMessage.GetBlockReq request, RpcResponseMessage.GetBlockRes response, RpcNameNodeState errorState) throws Exception {
		//check protocol
		if (!RpcProtocol.verifyProtocol(RpcProtocol.CMD_GET_BLOCK, request, response)){
			return RpcErrors.ERR_PROTOCOL_MISMATCH;
		}			
		
		//get params
		long fd = request.getFd();
		long token = request.getToken();
		long position = request.getPosition();
		long capacity = request.getCapacity();
		
		//check params
		if (position < 0){
			return RpcErrors.ERR_POSITION_NEGATIV;
		}
	
		//rpc
		AbstractNode fileInfo = fileTable.get(fd);
		if (fileInfo == null){
			return RpcErrors.ERR_FILE_NOT_OPEN;			
		}
		
		int index = CrailUtils.computeIndex(position);
		if (index < 0){
			return RpcErrors.ERR_POSITION_NEGATIV;			
		}
		
		NameNodeBlockInfo block = fileInfo.getBlock(index);
		if (block == null && fileInfo.getToken() == token){
			block = blockStore.getBlock(fileInfo.getStorageClass(), fileInfo.getLocationClass());
			if (block == null){
				return RpcErrors.ERR_NO_FREE_BLOCKS;
			}
			if (!fileInfo.addBlock(index, block)){
				return RpcErrors.ERR_ADD_BLOCK_FAILED;
			}
			block = fileInfo.getBlock(index);
			if (block == null){
				return RpcErrors.ERR_ADD_BLOCK_FAILED;
			}
			fileInfo.setCapacity(capacity);
		} else if (block == null && token > 0){ 
			return RpcErrors.ERR_TOKEN_MISMATCH;
		} else if (block == null && token == 0){ 
			return RpcErrors.ERR_CAPACITY_EXCEEDED;
		} 
		
		response.setBlockInfo(block);
		return RpcErrors.ERR_OK;
	}
	
	@Override
	public short getLocation(RpcRequestMessage.GetLocationReq request, RpcResponseMessage.GetLocationRes response, RpcNameNodeState errorState) throws Exception {
		//check protocol
		if (!RpcProtocol.verifyProtocol(RpcProtocol.CMD_GET_LOCATION, request, response)){
			return RpcErrors.ERR_PROTOCOL_MISMATCH;
		}			
		
		//get params
		FileName fileName = request.getFileName();
		long position = request.getPosition();
		
		//check params
		if (position < 0){
			return RpcErrors.ERR_POSITION_NEGATIV;
		}	
		
		//rpc
		AbstractNode fileInfo = fileTree.retrieveFile(fileName, errorState);
		if (errorState.getError() != RpcErrors.ERR_OK){
			return errorState.getError();
		}		
		if (fileInfo == null){
			return RpcErrors.ERR_GET_FILE_FAILED;
		}	
		
		int index = CrailUtils.computeIndex(position);
		if (index < 0){
			return RpcErrors.ERR_POSITION_NEGATIV;			
		}		
		BlockInfo block = fileInfo.getBlock(index);
		if (block == null){
			return RpcErrors.ERR_OFFSET_TOO_LARGE;
		}
		
		response.setBlockInfo(block);
		
		return RpcErrors.ERR_OK;
	}

	//------------------------
	
	@Override
	public short dump(RpcRequestMessage.DumpNameNodeReq request, RpcResponseMessage.VoidRes response, RpcNameNodeState errorState) throws Exception {
		if (!RpcProtocol.verifyProtocol(RpcProtocol.CMD_DUMP_NAMENODE, request, response)){
			return RpcErrors.ERR_PROTOCOL_MISMATCH;
		}			
		
		System.out.println("#fd\t\tfilecomp\t\tcapacity\t\tisdir\t\t\tdiroffset");
		fileTree.dump();
		System.out.println("#fd\t\tfilecomp\t\tcapacity\t\tisdir\t\t\tdiroffset");
		dumpFastMap();
		
		return RpcErrors.ERR_OK;
	}	
	
	@Override
	public short ping(RpcRequestMessage.PingNameNodeReq request, RpcResponseMessage.PingNameNodeRes response, RpcNameNodeState errorState) throws Exception {
		if (!RpcProtocol.verifyProtocol(RpcProtocol.CMD_PING_NAMENODE, request, response)){
			return RpcErrors.ERR_PROTOCOL_MISMATCH;
		}	
		
		response.setData(request.getOp()+1);
		
		return RpcErrors.ERR_OK;
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
			fileTable.remove(fileInfo.getFd());
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
