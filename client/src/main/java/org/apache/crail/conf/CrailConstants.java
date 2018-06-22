/*
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

package org.apache.crail.conf;

import java.io.IOException;

import org.apache.crail.utils.CrailUtils;
import org.slf4j.Logger;

public class CrailConstants {
	private static final Logger LOG = CrailUtils.getLogger();

	public static final String VERSION_KEY = "crail.version";
	public static int VERSION = 3101;

	public static final String DIRECTORY_DEPTH_KEY = "crail.directorydepth";
	public static int DIRECTORY_DEPTH = 16;

	public static final String TOKEN_EXPIRATION_KEY = "crail.tokenexpiration";
	public static long TOKEN_EXPIRATION = 10;

	public static final String BLOCK_SIZE_KEY = "crail.blocksize";
	public static long BLOCK_SIZE = 1048576;

	public static final String CACHE_LIMIT_KEY = "crail.cachelimit";
	public static long CACHE_LIMIT = 1073741824;

	public static final String CACHE_PATH_KEY = "crail.cachepath";
	public static String CACHE_PATH = "/dev/hugepages/cache";

	public static final String USER_KEY = "crail.user";
	public static String USER = "crail";

	public static final String SHADOW_REPLICATION_KEY = "crail.shadowreplication";
	public static int SHADOW_REPLICATION = 1;

	public static final String DEBUG_KEY = "crail.debug";
	public static boolean DEBUG = false;

	public static final String STATISTICS_KEY = "crail.statistics";
	public static boolean STATISTICS = true;

	public static final String RPC_TIMEOUT_KEY = "crail.rpctimeout";
	public static int RPC_TIMEOUT = 1000;

	public static final String DATA_TIMEOUT_KEY = "crail.datatimeout";
	public static int DATA_TIMEOUT = 1000;

	public static final String BUFFER_SIZE_KEY = "crail.buffersize";
	public static int BUFFER_SIZE = 1048576;

	public static final String SLICE_SIZE_KEY = "crail.slicesize";
	public static int SLICE_SIZE = 524288;

	public static final String SINGLETON_KEY = "crail.singleton";
	public static boolean SINGLETON = true;

	public static final String REGION_SIZE_KEY = "crail.regionsize";
	public static long REGION_SIZE = 1073741824;

	public static final String DIRECTORY_RECORD_KEY = "crail.directoryrecord";
	public static int DIRECTORY_RECORD = 512;

	public static final String DIRECTORY_RANDOMIZE_KEY = "crail.directoryrandomize";
	public static boolean DIRECTORY_RANDOMIZE = true;

	public static final String CACHE_IMPL_KEY = "crail.cacheimpl";
	public static String CACHE_IMPL = "org.apache.crail.memory.MappedBufferCache";

	public static final String LOCATION_MAP_KEY = "crail.locationmap";
	public static String LOCATION_MAP = "";

	//namenode interface
	public static final String NAMENODE_ADDRESS_KEY = "crail.namenode.address";
	public static String NAMENODE_ADDRESS = "crail://localhost:9060";

	public static final String NAMENODE_FILEBLOCKS_KEY = "crail.namenode.fileblocks";
	public static int NAMENODE_FILEBLOCKS = 16;

	public static final String NAMENODE_BLOCKSELECTION_KEY = "crail.namenode.blockselection";
	public static String NAMENODE_BLOCKSELECTION = "roundrobin";

	public static final String NAMENODE_RPC_TYPE_KEY = "crail.namenode.rpctype";
	public static String NAMENODE_RPC_TYPE = "org.apache.crail.namenode.rpc.tcp.TcpNameNode";

	public static final String NAMENODE_RPC_SERVICE_KEY = "crail.namenode.rpcservice";
	public static String NAMENODE_RPC_SERVICE = "org.apache.crail.namenode.NameNodeService";

	public static final String NAMENODE_LOG_KEY = "crail.namenode.log";
	public static String NAMENODE_LOG = "";

	//storage interface
	public static final String STORAGE_TYPES_KEY = "crail.storage.types";
	public static String STORAGE_TYPES = "org.apache.crail.storage.tcp.TcpStorageTier";

	public static final String STORAGE_CLASSES_KEY = "crail.storage.classes";
	public static int STORAGE_CLASSES = 1;

	public static final String STORAGE_ROOTCLASS_KEY = "crail.storage.rootclass";
	public static int STORAGE_ROOTCLASS = 0;

	public static final String STORAGE_KEEPALIVE_KEY = "crail.storage.keepalive";
	public static int STORAGE_KEEPALIVE = 2;

	public static void updateConstants(CrailConfiguration conf){
		//general
		if (conf.get(DIRECTORY_DEPTH_KEY) != null) {
			DIRECTORY_DEPTH = Integer.parseInt(conf.get(DIRECTORY_DEPTH_KEY));
		}
		if (conf.get(TOKEN_EXPIRATION_KEY) != null) {
			TOKEN_EXPIRATION = Long.parseLong(conf.get(TOKEN_EXPIRATION_KEY));
		}
		if (conf.get(BLOCK_SIZE_KEY) != null) {
			BLOCK_SIZE = Long.parseLong(conf.get(BLOCK_SIZE_KEY));
		}
		if (conf.get(CACHE_LIMIT_KEY) != null) {
			CACHE_LIMIT = Long.parseLong(conf.get(CACHE_LIMIT_KEY));
		}
		if (conf.get(CACHE_PATH_KEY) != null) {
			CACHE_PATH = conf.get(CACHE_PATH_KEY);
		}
		if (conf.get(USER_KEY) != null) {
			USER = conf.get(CrailConstants.USER_KEY);
		}
		if (conf.get(SHADOW_REPLICATION_KEY) != null) {
			SHADOW_REPLICATION = Integer.parseInt(conf.get(SHADOW_REPLICATION_KEY));
		}
		if (conf.get(DEBUG_KEY) != null) {
			DEBUG = Boolean.parseBoolean(conf.get(DEBUG_KEY));
		}
		if (conf.get(STATISTICS_KEY) != null) {
			STATISTICS = Boolean.parseBoolean(conf.get(STATISTICS_KEY));
		}
		if (conf.get(RPC_TIMEOUT_KEY) != null) {
			RPC_TIMEOUT = Integer.parseInt(conf.get(RPC_TIMEOUT_KEY));
		}
		if (conf.get(DATA_TIMEOUT_KEY) != null) {
			DATA_TIMEOUT = Integer.parseInt(conf.get(DATA_TIMEOUT_KEY));
		}
		if (conf.get(BUFFER_SIZE_KEY) != null) {
			BUFFER_SIZE = Integer.parseInt(conf.get(BUFFER_SIZE_KEY));
		}
		if (conf.get(SLICE_SIZE_KEY) != null) {
			SLICE_SIZE = Integer.parseInt(conf.get(SLICE_SIZE_KEY));
		}
		if (conf.get(CrailConstants.SINGLETON_KEY) != null) {
			SINGLETON = conf.getBoolean(CrailConstants.SINGLETON_KEY, false);
		}
		if (conf.get(REGION_SIZE_KEY) != null) {
			REGION_SIZE = Integer.parseInt(conf.get(REGION_SIZE_KEY));
		}
		if (conf.get(DIRECTORY_RECORD_KEY) != null) {
			DIRECTORY_RECORD = Integer.parseInt(conf.get(DIRECTORY_RECORD_KEY));
		}
		if (conf.get(CrailConstants.DIRECTORY_RANDOMIZE_KEY) != null) {
			DIRECTORY_RANDOMIZE = conf.getBoolean(CrailConstants.DIRECTORY_RANDOMIZE_KEY, false);
		}
		if (conf.get(CACHE_IMPL_KEY) != null) {
			CACHE_IMPL = conf.get(CACHE_IMPL_KEY);
		}
		if (conf.get(LOCATION_MAP_KEY) != null) {
			LOCATION_MAP = conf.get(LOCATION_MAP_KEY);
		}

		//namenode interface
		if (conf.get(NAMENODE_ADDRESS_KEY) != null) {
			NAMENODE_ADDRESS = conf.get(NAMENODE_ADDRESS_KEY);
		}
		if (conf.get(NAMENODE_BLOCKSELECTION_KEY) != null) {
			NAMENODE_BLOCKSELECTION = conf.get(NAMENODE_BLOCKSELECTION_KEY);
		}
		if (conf.get(NAMENODE_FILEBLOCKS_KEY) != null) {
			NAMENODE_FILEBLOCKS = Integer.parseInt(conf.get(NAMENODE_FILEBLOCKS_KEY));
		}
		if (conf.get(NAMENODE_RPC_TYPE_KEY) != null) {
			NAMENODE_RPC_TYPE = conf.get(NAMENODE_RPC_TYPE_KEY);
		}
		if (conf.get(NAMENODE_LOG_KEY) != null) {
			NAMENODE_LOG = conf.get(NAMENODE_LOG_KEY);
		}

		//storage interface
		if (conf.get(STORAGE_TYPES_KEY) != null) {
			STORAGE_TYPES = conf.get(STORAGE_TYPES_KEY);
		}
		if (conf.get(STORAGE_CLASSES_KEY) != null) {
			STORAGE_CLASSES = Math.max(Integer.parseInt(conf.get(STORAGE_CLASSES_KEY)), CrailUtils.getStorageClasses(STORAGE_TYPES));
		} else {
			STORAGE_CLASSES = CrailUtils.getStorageClasses(STORAGE_TYPES);
		}
		if (conf.get(STORAGE_ROOTCLASS_KEY) != null) {
			STORAGE_ROOTCLASS = Integer.parseInt(conf.get(STORAGE_ROOTCLASS_KEY));
		}
		if (conf.get(STORAGE_KEEPALIVE_KEY) != null) {
			STORAGE_KEEPALIVE = Integer.parseInt(conf.get(STORAGE_KEEPALIVE_KEY));
		}
	}

	public static void printConf(){
		LOG.info(VERSION_KEY + " " + VERSION);
		LOG.info(DIRECTORY_DEPTH_KEY + " " + DIRECTORY_DEPTH);
		LOG.info(TOKEN_EXPIRATION_KEY + " " + TOKEN_EXPIRATION);
		LOG.info(BLOCK_SIZE_KEY + " " + BLOCK_SIZE);
		LOG.info(CACHE_LIMIT_KEY + " " + CACHE_LIMIT);
		LOG.info(CACHE_PATH_KEY + " " + CACHE_PATH);
		LOG.info(USER_KEY + " " + USER);
		LOG.info(SHADOW_REPLICATION_KEY + " " + SHADOW_REPLICATION);
		LOG.info(DEBUG_KEY + " " + DEBUG);
		LOG.info(STATISTICS_KEY + " " + STATISTICS);
		LOG.info(RPC_TIMEOUT_KEY + " " + RPC_TIMEOUT);
		LOG.info(DATA_TIMEOUT_KEY + " " + DATA_TIMEOUT);
		LOG.info(BUFFER_SIZE_KEY + " " + BUFFER_SIZE);
		LOG.info(SLICE_SIZE_KEY + " " + SLICE_SIZE);
		LOG.info(SINGLETON_KEY + " " + SINGLETON);
		LOG.info(REGION_SIZE_KEY + " " + REGION_SIZE);
		LOG.info(DIRECTORY_RECORD_KEY + " " + DIRECTORY_RECORD);
		LOG.info(DIRECTORY_RANDOMIZE_KEY + " " + DIRECTORY_RANDOMIZE);
		LOG.info(CACHE_IMPL_KEY + " " + CACHE_IMPL);
		LOG.info(LOCATION_MAP_KEY + " " + LOCATION_MAP);
		LOG.info(NAMENODE_ADDRESS_KEY + " " + NAMENODE_ADDRESS);
		LOG.info(NAMENODE_BLOCKSELECTION_KEY + " " + NAMENODE_BLOCKSELECTION);
		LOG.info(NAMENODE_FILEBLOCKS_KEY + " " + NAMENODE_FILEBLOCKS);
		LOG.info(NAMENODE_RPC_TYPE_KEY + " " + NAMENODE_RPC_TYPE);
		LOG.info(NAMENODE_LOG_KEY + " " + NAMENODE_LOG);
		LOG.info(STORAGE_TYPES_KEY + " " + STORAGE_TYPES);
		LOG.info(STORAGE_CLASSES_KEY + " " + STORAGE_CLASSES);
		LOG.info(STORAGE_ROOTCLASS_KEY + " " + STORAGE_ROOTCLASS);
		LOG.info(STORAGE_KEEPALIVE_KEY + " " + STORAGE_KEEPALIVE);
	}

	public static void verify() throws IOException {
		if (CrailConstants.BUFFER_SIZE % CrailConstants.DIRECTORY_RECORD != 0){
			throw new IOException("crail.buffersize must be multiple of " + CrailConstants.DIRECTORY_RECORD);
		}
		if (Math.max(CrailConstants.BUFFER_SIZE, CrailConstants.SLICE_SIZE) % Math.min(CrailConstants.BUFFER_SIZE, CrailConstants.SLICE_SIZE) != 0){
			throw new IOException("crail.slicesize must be multiple of buffersize " + CrailConstants.BUFFER_SIZE);
		}
		if (CrailConstants.STORAGE_CLASSES < CrailUtils.getStorageClasses(STORAGE_TYPES)){
			throw new IOException("crail.storage.classes cannot be smaller than the number of storage types " + CrailUtils.getStorageClasses(STORAGE_TYPES));
		}

	}
}
