package com.ibm.crail.storage.rdma;

import java.io.IOException;

import org.slf4j.Logger;

import com.ibm.crail.conf.CrailConfiguration;
import com.ibm.crail.conf.CrailConstants;

public class RdmaConstants {
	public static final String DATANODE_RDMA_INTERFACE_KEY = "crail.datanode.rdma.interface";
	public static String DATANODE_RDMA_INTERFACE = "eth5";

	public static final String DATANODE_RDMA_PORT_KEY = "crail.datanode.rdma.port";
	public static int DATANODE_RDMA_PORT = 50020;	
	
	public static final String DATANODE_RDMA_STORAGE_LIMIT_KEY = "crail.datanode.rdma.storagelimit";
	public static long DATANODE_RDMA_STORAGE_LIMIT = 1073741824;

	public static final String DATANODE_RDMA_ALLOCATION_SIZE_KEY = "crail.datanode.rdma.allocationsize";
	public static long DATANODE_RDMA_ALLOCATION_SIZE = CrailConstants.REGION_SIZE;

	public static final String DATANODE_RDMA_DATA_PATH_KEY = "crail.datanode.rdma.datapath";
	public static String DATANODE_RDMA_DATA_PATH = "/home/stu/craildata/data";

	public static final String DATANODE_RDMA_INDEX_PATH_KEY = "crail.datanode.rdma.indexpath";
	public static String DATANODE_RDMA_INDEX_PATH = "/home/stu/craildata/index";
	
	public static final String DATANODE_RDMA_LOCAL_MAP_KEY = "crail.datanode.rdma.localmap";
	public static boolean DATANODE_RDMA_LOCAL_MAP = true;
	
	public static final String DATANODE_RDMA_QUEUESIZE_KEY = "crail.datanode.rdma.queuesize";
	public static int DATANODE_RDMA_QUEUESIZE = 32;
	
	public static final String DATANODE_RDMA_TYPE_KEY = "crail.datanode.rdma.type";
	public static String DATANODE_RDMA_TYPE = "passive";	
	
	public static void updateConstants(CrailConfiguration conf){
		if (conf.get(DATANODE_RDMA_INTERFACE_KEY) != null) {
			DATANODE_RDMA_INTERFACE = conf.get(DATANODE_RDMA_INTERFACE_KEY);
		}
		if (conf.get(DATANODE_RDMA_PORT_KEY) != null) {
			DATANODE_RDMA_PORT = Integer.parseInt(conf.get(DATANODE_RDMA_PORT_KEY));
		}		
		if (conf.get(DATANODE_RDMA_STORAGE_LIMIT_KEY) != null) {
			DATANODE_RDMA_STORAGE_LIMIT = Long.parseLong(conf.get(DATANODE_RDMA_STORAGE_LIMIT_KEY));
		}		
		if (conf.get(DATANODE_RDMA_ALLOCATION_SIZE_KEY) != null) {
			DATANODE_RDMA_ALLOCATION_SIZE = Long.parseLong(conf.get(DATANODE_RDMA_ALLOCATION_SIZE_KEY));
		}	
		if (conf.get(DATANODE_RDMA_DATA_PATH_KEY) != null) {
			DATANODE_RDMA_DATA_PATH = conf.get(DATANODE_RDMA_DATA_PATH_KEY);
		}		
		if (conf.get(DATANODE_RDMA_INDEX_PATH_KEY) != null) {
			DATANODE_RDMA_INDEX_PATH = conf.get(DATANODE_RDMA_INDEX_PATH_KEY);
		}			
		if (conf.get(DATANODE_RDMA_LOCAL_MAP_KEY) != null) {
			DATANODE_RDMA_LOCAL_MAP = conf.getBoolean(DATANODE_RDMA_LOCAL_MAP_KEY, false);
		}			
		if (conf.get(DATANODE_RDMA_QUEUESIZE_KEY) != null) {
			DATANODE_RDMA_QUEUESIZE = Integer.parseInt(conf.get(DATANODE_RDMA_QUEUESIZE_KEY));
		}			
		if (conf.get(DATANODE_RDMA_TYPE_KEY) != null) {
			DATANODE_RDMA_TYPE = conf.get(DATANODE_RDMA_TYPE_KEY);
		}		
	}
	
	public static void verify() throws IOException {
		if (DATANODE_RDMA_ALLOCATION_SIZE % CrailConstants.BLOCK_SIZE != 0){
			throw new IOException("crail.datanode.rdma.allocationsize must be multiple of crail.blocksize");
		}
		if (DATANODE_RDMA_STORAGE_LIMIT % DATANODE_RDMA_ALLOCATION_SIZE != 0){
			throw new IOException("crail.datanode.rdma.storageLimit must be multiple of crail.datanode.rdma.allocationSize");
		}
		if (!DATANODE_RDMA_TYPE.equalsIgnoreCase("passive") && !DATANODE_RDMA_TYPE.equalsIgnoreCase("active")){
			throw new IOException("crail.datanode.type must be either <active> or <passive>, found " + DATANODE_RDMA_TYPE);
		}			
	}

	public static void printConf(Logger logger) {
		logger.info(DATANODE_RDMA_INTERFACE_KEY + " " + DATANODE_RDMA_INTERFACE);
		logger.info(DATANODE_RDMA_PORT_KEY + " " + DATANODE_RDMA_PORT);		
		logger.info(DATANODE_RDMA_STORAGE_LIMIT_KEY + " " + DATANODE_RDMA_STORAGE_LIMIT);
		logger.info(DATANODE_RDMA_ALLOCATION_SIZE_KEY + " " + DATANODE_RDMA_ALLOCATION_SIZE);
		logger.info(DATANODE_RDMA_DATA_PATH_KEY + " " + DATANODE_RDMA_DATA_PATH);
		logger.info(DATANODE_RDMA_INDEX_PATH_KEY + " " + DATANODE_RDMA_INDEX_PATH);
		logger.info(DATANODE_RDMA_LOCAL_MAP_KEY + " " + DATANODE_RDMA_LOCAL_MAP);
		logger.info(DATANODE_RDMA_QUEUESIZE_KEY + " " + DATANODE_RDMA_QUEUESIZE);
		logger.info(DATANODE_RDMA_TYPE_KEY + " " + DATANODE_RDMA_TYPE);
	}	
}
