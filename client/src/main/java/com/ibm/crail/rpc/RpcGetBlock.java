package com.ibm.crail.rpc;

import com.ibm.crail.metadata.BlockInfo;

public interface RpcGetBlock extends RpcResponse {
	public BlockInfo getBlockInfo();
	public void setBlockInfo(BlockInfo blockInfo);
}
