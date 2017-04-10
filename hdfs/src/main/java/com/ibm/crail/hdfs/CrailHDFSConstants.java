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

package com.ibm.crail.hdfs;

import java.io.IOException;

import org.slf4j.Logger;

import com.ibm.crail.conf.CrailConfiguration;

public class CrailHDFSConstants {
	public static final String STORAGE_AFFINITY_KEY = "crail.hdfs.storageaffinity";
	public static int STORAGE_AFFINITY = 1;
	
	public static final String LOCAL_AFFINITY_KEY = "crail.hdfs.localaffinity";
	public static boolean LOCAL_AFFINITY = false;

	public static void updateConstants(CrailConfiguration conf){
		if (conf.get(STORAGE_AFFINITY_KEY) != null) {
			STORAGE_AFFINITY = Integer.parseInt(conf.get(STORAGE_AFFINITY_KEY));
		}
		if (conf.get(LOCAL_AFFINITY_KEY) != null) {
			LOCAL_AFFINITY = Boolean.parseBoolean(conf.get(LOCAL_AFFINITY_KEY));
		}			
	}
	
	public static void verify() throws IOException {
	}

	public static void printConf(Logger logger) {
		logger.info(STORAGE_AFFINITY_KEY + " " + STORAGE_AFFINITY);
		logger.info(LOCAL_AFFINITY_KEY + " " + LOCAL_AFFINITY);
	}	
}
