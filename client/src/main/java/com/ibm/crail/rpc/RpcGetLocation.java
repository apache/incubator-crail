package com.ibm.crail.rpc;

import java.io.IOException;

import com.ibm.crail.metadata.BlockInfo;

public interface RpcGetLocation extends RpcResponse {
	public BlockInfo getBlockInfo() throws IOException;
	public long getFd();
}
