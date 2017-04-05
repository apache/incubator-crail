package com.ibm.crail.storage;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import com.ibm.crail.conf.CrailConstants;
import com.ibm.crail.namenode.protocol.BlockInfo;
import com.ibm.crail.namenode.protocol.DataNodeInfo;
import com.ibm.crail.namenode.protocol.DataNodeStatistics;
import com.ibm.crail.namenode.rpc.NameNodeProtocol;
import com.ibm.crail.namenode.rpc.RpcNameNode;
import com.ibm.crail.namenode.rpc.RpcNameNodeClient;
import com.ibm.crail.namenode.rpc.RpcResponseMessage;
import com.ibm.crail.utils.CrailUtils;

public class StorageRpcClient {
	public static final Logger LOG = CrailUtils.getLogger();
	
	private InetSocketAddress serverAddress;
	private int storageTierIndex;
	private int hostHash;
	private RpcNameNode rpcNameNode;
	private RpcNameNodeClient namenodeClientRpc;	

	public StorageRpcClient(int storageTierIndex, InetSocketAddress serverAddress) throws Exception {
		this.storageTierIndex = storageTierIndex;
		this.serverAddress = serverAddress;
		this.hostHash = CrailUtils.getHostHash();
		LOG.info("hosthash" + hostHash);		
		
		InetSocketAddress nnAddr = CrailUtils.getNameNodeAddress();
		this.rpcNameNode = RpcNameNode.createInstance(CrailConstants.NAMENODE_RPC_TYPE);
		this.namenodeClientRpc = rpcNameNode.getRpcClient(nnAddr);
		LOG.info("connected to namenode at " + nnAddr);				
	}
	
	public void setBlock(long addr, int length, int key) throws Exception {
		InetSocketAddress inetAddress = serverAddress;
		DataNodeInfo dnInfo = new DataNodeInfo(storageTierIndex, hostHash, inetAddress.getAddress().getAddress(), inetAddress.getPort());
		BlockInfo blockInfo = new BlockInfo(dnInfo, addr, length, key);
		RpcResponseMessage.VoidRes res = namenodeClientRpc.setBlock(blockInfo).get(CrailConstants.RPC_TIMEOUT, TimeUnit.MILLISECONDS);
		if (res.getError() != NameNodeProtocol.ERR_OK){
			LOG.info("setBlock: " + NameNodeProtocol.messages[res.getError()]);
			throw new IOException("setBlock: " + NameNodeProtocol.messages[res.getError()]);
		}
	}
	
	public DataNodeStatistics getDataNode() throws Exception{
		InetSocketAddress inetAddress = serverAddress;
		DataNodeInfo dnInfo = new DataNodeInfo(storageTierIndex, hostHash, inetAddress.getAddress().getAddress(), inetAddress.getPort());
		return this.namenodeClientRpc.getDataNode(dnInfo).get(CrailConstants.RPC_TIMEOUT, TimeUnit.MILLISECONDS).getStatistics();
	}	
}
