/*
 * Crail: A Multi-tiered Distributed Direct Access File System
 *
 * Author: Patrick Stuedi <stu@zurich.ibm.com>
 *
 * Copyright (C) 2016, IBM Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.ibm.crail.storage.rdma;

import java.io.IOException;

import org.slf4j.Logger;

import com.ibm.crail.conf.CrailConfiguration;
import com.ibm.crail.conf.CrailConstants;

public class RdmaConstants {
	public static final String STORAGE_RDMA_INTERFACE_KEY = "crail.storage.rdma.interface";
	public static String STORAGE_RDMA_INTERFACE = "eth5";

	public static final String STORAGE_RDMA_PORT_KEY = "crail.storage.rdma.port";
	public static int STORAGE_RDMA_PORT = 50020;	
	
	public static final String STORAGE_RDMA_STORAGE_LIMIT_KEY = "crail.storage.rdma.storagelimit";
	public static long STORAGE_RDMA_STORAGE_LIMIT = 1073741824;

	public static final String STORAGE_RDMA_ALLOCATION_SIZE_KEY = "crail.storage.rdma.allocationsize";
	public static long STORAGE_RDMA_ALLOCATION_SIZE = CrailConstants.REGION_SIZE;

	public static final String STORAGE_RDMA_DATA_PATH_KEY = "crail.storage.rdma.datapath";
	public static String STORAGE_RDMA_DATA_PATH = "/home/stu/craildata/data";

	public static final String STORAGE_RDMA_INDEX_PATH_KEY = "crail.storage.rdma.indexpath";
	public static String STORAGE_RDMA_INDEX_PATH = "/home/stu/craildata/index";
	
	public static final String STORAGE_RDMA_LOCAL_MAP_KEY = "crail.storage.rdma.localmap";
	public static boolean STORAGE_RDMA_LOCAL_MAP = true;
	
	public static final String STORAGE_RDMA_QUEUESIZE_KEY = "crail.storage.rdma.queuesize";
	public static int STORAGE_RDMA_QUEUESIZE = 32;
	
	public static final String STORAGE_RDMA_TYPE_KEY = "crail.storage.rdma.type";
	public static String STORAGE_RDMA_TYPE = "passive";	
	
	public static void updateConstants(CrailConfiguration conf){
		if (conf.get(STORAGE_RDMA_INTERFACE_KEY) != null) {
			STORAGE_RDMA_INTERFACE = conf.get(STORAGE_RDMA_INTERFACE_KEY);
		}
		if (conf.get(STORAGE_RDMA_PORT_KEY) != null) {
			STORAGE_RDMA_PORT = Integer.parseInt(conf.get(STORAGE_RDMA_PORT_KEY));
		}		
		if (conf.get(STORAGE_RDMA_STORAGE_LIMIT_KEY) != null) {
			STORAGE_RDMA_STORAGE_LIMIT = Long.parseLong(conf.get(STORAGE_RDMA_STORAGE_LIMIT_KEY));
		}		
		if (conf.get(STORAGE_RDMA_ALLOCATION_SIZE_KEY) != null) {
			STORAGE_RDMA_ALLOCATION_SIZE = Long.parseLong(conf.get(STORAGE_RDMA_ALLOCATION_SIZE_KEY));
		}	
		if (conf.get(STORAGE_RDMA_DATA_PATH_KEY) != null) {
			STORAGE_RDMA_DATA_PATH = conf.get(STORAGE_RDMA_DATA_PATH_KEY);
		}		
		if (conf.get(STORAGE_RDMA_INDEX_PATH_KEY) != null) {
			STORAGE_RDMA_INDEX_PATH = conf.get(STORAGE_RDMA_INDEX_PATH_KEY);
		}			
		if (conf.get(STORAGE_RDMA_LOCAL_MAP_KEY) != null) {
			STORAGE_RDMA_LOCAL_MAP = conf.getBoolean(STORAGE_RDMA_LOCAL_MAP_KEY, false);
		}			
		if (conf.get(STORAGE_RDMA_QUEUESIZE_KEY) != null) {
			STORAGE_RDMA_QUEUESIZE = Integer.parseInt(conf.get(STORAGE_RDMA_QUEUESIZE_KEY));
		}			
		if (conf.get(STORAGE_RDMA_TYPE_KEY) != null) {
			STORAGE_RDMA_TYPE = conf.get(STORAGE_RDMA_TYPE_KEY);
		}		
	}
	
	public static void verify() throws IOException {
		if (STORAGE_RDMA_ALLOCATION_SIZE % CrailConstants.BLOCK_SIZE != 0){
			throw new IOException("crail.storage.rdma.allocationsize must be multiple of crail.blocksize");
		}
		if (STORAGE_RDMA_STORAGE_LIMIT % STORAGE_RDMA_ALLOCATION_SIZE != 0){
			throw new IOException("crail.storage.rdma.storageLimit must be multiple of crail.storage.rdma.allocationSize");
		}
		if (!STORAGE_RDMA_TYPE.equalsIgnoreCase("passive") && !STORAGE_RDMA_TYPE.equalsIgnoreCase("active")){
			throw new IOException("crail.storage.rdma.type must be either <active> or <passive>, found " + STORAGE_RDMA_TYPE);
		}			
	}

	public static void printConf(Logger logger) {
		logger.info(STORAGE_RDMA_INTERFACE_KEY + " " + STORAGE_RDMA_INTERFACE);
		logger.info(STORAGE_RDMA_PORT_KEY + " " + STORAGE_RDMA_PORT);		
		logger.info(STORAGE_RDMA_STORAGE_LIMIT_KEY + " " + STORAGE_RDMA_STORAGE_LIMIT);
		logger.info(STORAGE_RDMA_ALLOCATION_SIZE_KEY + " " + STORAGE_RDMA_ALLOCATION_SIZE);
		logger.info(STORAGE_RDMA_DATA_PATH_KEY + " " + STORAGE_RDMA_DATA_PATH);
		logger.info(STORAGE_RDMA_INDEX_PATH_KEY + " " + STORAGE_RDMA_INDEX_PATH);
		logger.info(STORAGE_RDMA_LOCAL_MAP_KEY + " " + STORAGE_RDMA_LOCAL_MAP);
		logger.info(STORAGE_RDMA_QUEUESIZE_KEY + " " + STORAGE_RDMA_QUEUESIZE);
		logger.info(STORAGE_RDMA_TYPE_KEY + " " + STORAGE_RDMA_TYPE);
	}	
}
