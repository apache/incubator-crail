package com.ibm.crail.rpc;

import com.ibm.crail.metadata.BlockInfo;
import com.ibm.crail.metadata.FileInfo;

public interface RpcGetFile extends RpcResponse {
	public FileInfo getFile();
	public BlockInfo getFileBlock();
}
