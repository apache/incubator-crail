package com.ibm.crail.storage;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;

import com.ibm.crail.conf.CrailConstants;
import com.ibm.crail.metadata.BlockInfo;
import com.ibm.crail.metadata.DataNodeInfo;
import com.ibm.crail.metadata.DataNodeStatistics;
import com.ibm.crail.rpc.RpcErrors;
import com.ibm.crail.rpc.RpcClient;
import com.ibm.crail.rpc.RpcConnection;
import com.ibm.crail.rpc.RpcVoid;
import com.ibm.crail.utils.CrailUtils;

public class StorageRpcClient {
	public static final Logger LOG = CrailUtils.getLogger();
	
	private InetSocketAddress serverAddress;
	private int storageTierIndex;
	private int hostHash;
	private RpcClient rpcNameNode;
	private RpcConnection namenodeClientRpc;	

	public StorageRpcClient(int storageTierIndex, InetSocketAddress serverAddress) throws Exception {
		this.storageTierIndex = storageTierIndex;
		this.serverAddress = serverAddress;
		this.hostHash = CrailUtils.getHostHash();
		LOG.info("hosthash" + hostHash);		
		
		InetSocketAddress nnAddr = CrailUtils.getNameNodeAddress();
		this.rpcNameNode = RpcClient.createInstance(CrailConstants.NAMENODE_RPC_TYPE);
		this.namenodeClientRpc = rpcNameNode.connect(nnAddr);
		LOG.info("connected to namenode at " + nnAddr);				
	}
	
	public void setBlock(long addr, int length, int key) throws Exception {
		InetSocketAddress inetAddress = serverAddress;
		DataNodeInfo dnInfo = new DataNodeInfo(storageTierIndex, hostHash, inetAddress.getAddress().getAddress(), inetAddress.getPort());
		BlockInfo blockInfo = new BlockInfo(dnInfo, addr, length, key);
		RpcVoid res = namenodeClientRpc.setBlock(blockInfo).get(CrailConstants.RPC_TIMEOUT, TimeUnit.MILLISECONDS);
		if (res.getError() != RpcErrors.ERR_OK){
			LOG.info("setBlock: " + RpcErrors.messages[res.getError()]);
			throw new IOException("setBlock: " + RpcErrors.messages[res.getError()]);
		}
	}
	
	public DataNodeStatistics getDataNode() throws Exception{
		InetSocketAddress inetAddress = serverAddress;
		DataNodeInfo dnInfo = new DataNodeInfo(storageTierIndex, hostHash, inetAddress.getAddress().getAddress(), inetAddress.getPort());
		return this.namenodeClientRpc.getDataNode(dnInfo).get(CrailConstants.RPC_TIMEOUT, TimeUnit.MILLISECONDS).getStatistics();
	}	
}
