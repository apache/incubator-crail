package com.ibm.crail.rpc;

import com.ibm.crail.metadata.BlockInfo;
import com.ibm.crail.metadata.FileInfo;

public interface RpcRenameFile extends RpcResponse {
	public FileInfo getSrcParent();
	public FileInfo getSrcFile();
	public FileInfo getDstParent();
	public FileInfo getDstFile();
	public BlockInfo getSrcBlock();
	public BlockInfo getDstBlock();
}
