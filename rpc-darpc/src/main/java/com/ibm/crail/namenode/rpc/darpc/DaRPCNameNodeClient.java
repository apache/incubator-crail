package com.ibm.crail.namenode.rpc.darpc;

import java.net.InetSocketAddress;
import java.net.URI;
import org.slf4j.Logger;
import com.ibm.crail.conf.CrailConfiguration;
import com.ibm.crail.rpc.RpcClient;
import com.ibm.crail.rpc.RpcConnection;
import com.ibm.crail.utils.CrailUtils;
import com.ibm.darpc.DaRPCClientEndpoint;
import com.ibm.darpc.DaRPCClientGroup;

public class DaRPCNameNodeClient implements RpcClient {
	private static final Logger LOG = CrailUtils.getLogger();
	
	private DaRPCNameNodeProtocol namenodeProtocol;
	private DaRPCClientGroup<DaRPCNameNodeRequest, DaRPCNameNodeResponse> namenodeClientGroup;
	
	public DaRPCNameNodeClient(){
		this.namenodeProtocol = null;
		this.namenodeClientGroup = null;
	}
	
	public void init(CrailConfiguration conf, String[] args) throws Exception{
		DaRPCConstants.updateConstants(conf);
		DaRPCConstants.verify();
		this.namenodeProtocol = new DaRPCNameNodeProtocol();
		this.namenodeClientGroup = DaRPCClientGroup.createClientGroup(namenodeProtocol, 100, DaRPCConstants.NAMENODE_DARPC_MAXINLINE, DaRPCConstants.NAMENODE_DARPC_RECVQUEUE, DaRPCConstants.NAMENODE_DARPC_SENDQUEUE);
		LOG.info("rpc group started, recvQueue " + namenodeClientGroup.recvQueueSize());
	}
	
	public void printConf(Logger logger){
		DaRPCConstants.printConf(logger);
	}

	@Override
	public RpcConnection connect(InetSocketAddress address) throws Exception {
		DaRPCClientEndpoint<DaRPCNameNodeRequest, DaRPCNameNodeResponse> namenodeEndopoint = namenodeClientGroup.createEndpoint();
//		LOG.info("connecting to namenode at " + address);
		URI uri = URI.create("rdma://" + address.getAddress().getHostAddress() + ":" + address.getPort());
		namenodeEndopoint.connect(uri);
		DaRPCNameNodeConnection connection = new DaRPCNameNodeConnection(namenodeEndopoint);
		return connection;
		
	}

	@Override
	public void close() {
		try {
			if (namenodeClientGroup != null){
				namenodeClientGroup.close();
				namenodeClientGroup = null;
			}
		} catch(Exception e){
			e.printStackTrace();
			LOG.info("Error while closing " + e.getMessage());
		}
	}

}
