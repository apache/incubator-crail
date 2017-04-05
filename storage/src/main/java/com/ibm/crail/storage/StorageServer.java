package com.ibm.crail.storage;

import java.net.InetSocketAddress;
import java.util.StringTokenizer;

import org.slf4j.Logger;

import com.ibm.crail.conf.CrailConfiguration;
import com.ibm.crail.conf.CrailConstants;
import com.ibm.crail.namenode.protocol.DataNodeStatistics;
import com.ibm.crail.utils.CrailUtils;
import com.ibm.disni.util.GetOpt;

public interface StorageServer {
	public abstract void registerResources(StorageRpcClient client) throws Exception;	
	public abstract boolean isAlive();
	public abstract void join() throws Exception;
	public abstract InetSocketAddress getAddress();
	
	public static void main(String[] args){
		try{
			Logger LOG = CrailUtils.getLogger();
			GetOpt go = new GetOpt(args, "t:");
			go.optErr = true;
			int ch = -1;
			String name = "com.ibm.crail.storage.rdma.RdmaStorageTier";
			CrailConfiguration conf = new CrailConfiguration();
			
			while ((ch = go.getopt()) != GetOpt.optEOF) {
				if ((char) ch == 't') {
					name = go.optArgGet();
				}
			}

			CrailConstants.updateConstants(conf);
			CrailConstants.printConf();
			CrailConstants.verify();				
	
			StorageTier storageTier = StorageTier.createInstance(name);
			if (storageTier == null){
				throw new Exception("Cannot instantiate datanode of type " + name);
			}
			
			StringTokenizer tokenizer = new StringTokenizer(CrailConstants.STORAGE_TYPES, ",");
			int storageTierIndex = 0;
			while (tokenizer.hasMoreTokens()){
				String storageTierName = tokenizer.nextToken();
				if (storageTierName.equalsIgnoreCase(storageTier.getClass().getName())){
					break;
				} else {
					storageTierIndex++;
				}
			}	
			
			storageTier.init(conf, args);
			storageTier.printConf(LOG);			
			
			StorageServer server = storageTier.launchServer();
			StorageRpcClient rpcClient = new StorageRpcClient(storageTierIndex, server.getAddress());
			server.registerResources(rpcClient);
			
			while (server.isAlive()) {
				DataNodeStatistics statistics = rpcClient.getDataNode();
				LOG.info("datanode statistics, freeBlocks " + statistics.getFreeBlockCount());
				Thread.sleep(2000);
			}			
			
			server.join();
			
			System.exit(0);
		} catch(Exception e){
			e.printStackTrace();
		}		
	}
}
