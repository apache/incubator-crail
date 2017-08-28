package com.ibm.crail.rpc;

import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.slf4j.Logger;

import com.ibm.crail.CrailNodeType;
import com.ibm.crail.metadata.BlockInfo;
import com.ibm.crail.metadata.DataNodeInfo;
import com.ibm.crail.metadata.FileInfo;
import com.ibm.crail.metadata.FileName;
import com.ibm.crail.utils.CrailUtils;

public class RpcDispatcher implements RpcConnection {
	private static final Logger LOG = CrailUtils.getLogger();
	
	private RpcConnection[] connections;
	private int setBlockIndex;
	private int getDataNodeIndex;

	public RpcDispatcher(ConcurrentLinkedQueue<RpcConnection> connectionList) {
		connections = new RpcConnection[connectionList.size()];
		for (int i = 0; i < connections.length; i++){
			connections[i] = connectionList.poll();
		}
		this.setBlockIndex = 0;
		this.getDataNodeIndex = 0;
	}

	@Override
	public RpcFuture<RpcCreateFile> createFile(FileName filename,
			CrailNodeType type, int storageClass, int locationClass)
			throws IOException {
		int index = filename.getComponent(0) % connections.length;
//		LOG.info("issuing create file for filename [" + filename.toString() + "], on index " + index);
		return connections[index].createFile(filename, type, storageClass, locationClass);
	}

	@Override
	public RpcFuture<RpcGetFile> getFile(FileName filename, boolean writeable)
			throws IOException {
		int index = filename.getComponent(0) % connections.length;
//		LOG.info("issuing get file for filename [" + filename.toString() + "], on index " + index);
		return connections[index].getFile(filename, writeable);
	}

	@Override
	public RpcFuture<RpcVoid> setFile(FileInfo fileInfo, boolean close)
			throws IOException {
		long connectionsLength = (long) connections.length;
		long _index = fileInfo.getFd() % connectionsLength;
		int index = (int) _index;
//		LOG.info("issuing set file for fd [" + fileInfo.getFd() + "], on index " + index);
		return connections[index].setFile(fileInfo, close);
	}

	@Override
	public RpcFuture<RpcDeleteFile> removeFile(FileName filename,
			boolean recursive) throws IOException {
		int index = filename.getComponent(0) % connections.length;
//		LOG.info("issuing remove file for filename [" + filename.toString() + "], on index " + index);		
		return connections[index].removeFile(filename, recursive);
	}

	@Override
	public RpcFuture<RpcRenameFile> renameFile(FileName srcHash,
			FileName dstHash) throws IOException {
		int srcIndex = srcHash.getComponent(0) % connections.length;
		int dstIndex = srcHash.getComponent(0) % connections.length;
//		LOG.info("issuing remove file for src [" + srcHash.toString() + "," + dstHash.toString() + "], on index " + srcIndex);	
		if (srcIndex != dstIndex){
			throw new IOException("Rename not supported across namenode domains");
		} else {
			return connections[srcIndex].renameFile(srcHash, dstHash);
		}
	}

	@Override
	public RpcFuture<RpcGetBlock> getBlock(long fd, long token, long position,
			long capacity) throws IOException {
		long connectionsLength = (long) connections.length;
		long _index = fd % connectionsLength;
		int index = (int) _index;
//		LOG.info("issuing get block for fd [" + fd + "], on index " + index);		
		return connections[index].getBlock(fd, token, position, capacity);
	}

	@Override
	public RpcFuture<RpcGetLocation> getLocation(FileName fileName,
			long position) throws IOException {
		int index = fileName.getComponent(0) % connections.length;
//		LOG.info("issuing get location for filename [" + fileName.toString() + "], on index " + index);			
		return connections[index].getLocation(fileName, position);
	}

	@Override
	public RpcFuture<RpcVoid> setBlock(BlockInfo blockInfo) throws Exception {
//		LOG.info("issuing set block on index " + setBlockIndex);
		RpcFuture<RpcVoid> res = connections[setBlockIndex].setBlock(blockInfo);
		setBlockIndex = (setBlockIndex + 1) % connections.length;
		return res;
	}

	@Override
	public RpcFuture<RpcGetDataNode> getDataNode(DataNodeInfo dnInfo)
			throws Exception {
//		LOG.info("issuing get datanode on index " + getDataNodeIndex);
		RpcFuture<RpcGetDataNode> res = connections[getDataNodeIndex].getDataNode(dnInfo);
		getDataNodeIndex = (getDataNodeIndex + 1) % connections.length;
		return res;
	}

	@Override
	public RpcFuture<RpcVoid> dumpNameNode() throws Exception {
		return connections[0].dumpNameNode();
	}

	@Override
	public RpcFuture<RpcPing> pingNameNode() throws Exception {
		return connections[0].pingNameNode();
	}

	@Override
	public String toString() {
		String address = "";
		for (RpcConnection connection : connections){
			address = address + ", " + connection.toString();
		}
		
		return address;
	}

	@Override
	public void close() throws Exception {
		connections[0].close();
	}
}
