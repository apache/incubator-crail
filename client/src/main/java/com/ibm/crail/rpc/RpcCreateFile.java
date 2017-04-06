package com.ibm.crail.rpc;

import com.ibm.crail.metadata.BlockInfo;
import com.ibm.crail.metadata.FileInfo;

public interface RpcCreateFile extends RpcResponse {
	public FileInfo getFile();
	public FileInfo getParent();
	public BlockInfo getFileBlock();
	public BlockInfo getDirBlock();
}
