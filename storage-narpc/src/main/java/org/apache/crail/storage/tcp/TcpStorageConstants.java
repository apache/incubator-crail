/*
 * Copyright (C) 2015-2018, IBM Corporation
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.crail.storage.tcp;

import org.apache.crail.conf.CrailConfiguration;
import org.apache.crail.conf.CrailConstants;
import org.slf4j.Logger;

public class TcpStorageConstants {
	public static final String STORAGE_TCP_INTERFACE_KEY = "crail.storage.tcp.interface";
	public static String STORAGE_TCP_INTERFACE = "eth0";
	
	public static final String STORAGE_TCP_PORT_KEY = "crail.storage.tcp.port";
	public static int STORAGE_TCP_PORT = 50020;
	
	public static final String STORAGE_TCP_STORAGE_LIMIT_KEY = "crail.storage.tcp.storagelimit";
	public static long STORAGE_TCP_STORAGE_LIMIT = 1073741824;

	public static final String STORAGE_TCP_ALLOCATION_SIZE_KEY = "crail.storage.tcp.allocationsize";
	public static long STORAGE_TCP_ALLOCATION_SIZE = CrailConstants.REGION_SIZE;	
	
	public static final String STORAGE_TCP_DATA_PATH_KEY = "crail.storage.tcp.datapath";
	public static String STORAGE_TCP_DATA_PATH = "/home/stu/craildata/data";
	
	public static final String STORAGE_TCP_QUEUE_DEPTH_KEY = "crail.storage.tcp.queuedepth";
	public static int STORAGE_TCP_QUEUE_DEPTH = 16;	
	
	public static void updateConstants(CrailConfiguration conf){
		if (conf.get(STORAGE_TCP_INTERFACE_KEY) != null) {
			STORAGE_TCP_INTERFACE = conf.get(STORAGE_TCP_INTERFACE_KEY);
		}	
		if (conf.get(STORAGE_TCP_PORT_KEY) != null) {
			STORAGE_TCP_PORT = Integer.parseInt(conf.get(STORAGE_TCP_PORT_KEY));
		}		
		if (conf.get(STORAGE_TCP_STORAGE_LIMIT_KEY) != null) {
			STORAGE_TCP_STORAGE_LIMIT = Long.parseLong(conf.get(STORAGE_TCP_STORAGE_LIMIT_KEY));
		}			
		if (conf.get(STORAGE_TCP_ALLOCATION_SIZE_KEY) != null) {
			STORAGE_TCP_ALLOCATION_SIZE = Integer.parseInt(conf.get(STORAGE_TCP_ALLOCATION_SIZE_KEY));
		}			
		if (conf.get(STORAGE_TCP_DATA_PATH_KEY) != null) {
			STORAGE_TCP_DATA_PATH = conf.get(STORAGE_TCP_DATA_PATH_KEY);
		}	
		if (conf.get(STORAGE_TCP_QUEUE_DEPTH_KEY) != null) {
			STORAGE_TCP_QUEUE_DEPTH = Integer.parseInt(conf.get(STORAGE_TCP_QUEUE_DEPTH_KEY));
		}		
	}	
	
	public static void printConf(Logger logger) {
		logger.info(STORAGE_TCP_INTERFACE_KEY + " " + STORAGE_TCP_INTERFACE);
		logger.info(STORAGE_TCP_PORT_KEY + " " + STORAGE_TCP_PORT);		
		logger.info(STORAGE_TCP_STORAGE_LIMIT_KEY + " " + STORAGE_TCP_STORAGE_LIMIT);
		logger.info(STORAGE_TCP_ALLOCATION_SIZE_KEY + " " + STORAGE_TCP_ALLOCATION_SIZE);
		logger.info(STORAGE_TCP_DATA_PATH_KEY + " " + STORAGE_TCP_DATA_PATH);
		logger.info(STORAGE_TCP_QUEUE_DEPTH_KEY + " " + STORAGE_TCP_QUEUE_DEPTH);
	}	

}
