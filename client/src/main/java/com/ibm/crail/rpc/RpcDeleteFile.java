package com.ibm.crail.rpc;

import com.ibm.crail.metadata.FileInfo;

public interface RpcDeleteFile extends RpcResponse {
	public FileInfo getFile();
	public FileInfo getParent();
}
