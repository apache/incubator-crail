package com.ibm.crail.rpc;

import com.ibm.crail.metadata.DataNodeStatistics;

public interface RpcGetDataNode extends RpcResponse {
	public DataNodeStatistics getStatistics();
}
