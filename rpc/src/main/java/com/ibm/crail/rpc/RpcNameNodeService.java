package com.ibm.crail.rpc;

public interface RpcNameNodeService {
	public abstract short createFile(RpcRequestMessage.CreateFileReq request,
			RpcResponseMessage.CreateFileRes response, RpcNameNodeState errorState)
			throws Exception;

	public abstract short getFile(RpcRequestMessage.GetFileReq request,
			RpcResponseMessage.GetFileRes response, RpcNameNodeState errorState)
			throws Exception;

	public abstract short setFile(RpcRequestMessage.SetFileReq request,
			RpcResponseMessage.VoidRes response, RpcNameNodeState errorState)
			throws Exception;

	public abstract short removeFile(RpcRequestMessage.RemoveFileReq request,
			RpcResponseMessage.DeleteFileRes response, RpcNameNodeState errorState)
			throws Exception;

	public abstract short renameFile(
			RpcRequestMessage.RenameFileReq request,
			RpcResponseMessage.RenameRes response, RpcNameNodeState errorState)
			throws Exception;

	public abstract short getDataNode(
			RpcRequestMessage.GetDataNodeReq request,
			RpcResponseMessage.GetDataNodeRes response, RpcNameNodeState errorState)
			throws Exception;

	public abstract short setBlock(RpcRequestMessage.SetBlockReq request,
			RpcResponseMessage.VoidRes response, RpcNameNodeState errorState)
			throws Exception;

	public abstract short getBlock(RpcRequestMessage.GetBlockReq request,
			RpcResponseMessage.GetBlockRes response, RpcNameNodeState errorState)
			throws Exception;

	public abstract short getLocation(RpcRequestMessage.GetLocationReq request,
			RpcResponseMessage.GetLocationRes response, RpcNameNodeState errorState)
			throws Exception;

	public abstract short dump(RpcRequestMessage.DumpNameNodeReq request,
			RpcResponseMessage.VoidRes response, RpcNameNodeState errorState)
			throws Exception;

	public abstract short ping(RpcRequestMessage.PingNameNodeReq request,
			RpcResponseMessage.PingNameNodeRes response, RpcNameNodeState errorState)
			throws Exception;
}
