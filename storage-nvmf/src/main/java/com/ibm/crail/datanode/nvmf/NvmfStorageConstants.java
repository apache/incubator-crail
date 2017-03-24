/*
 * Crail: A Multi-tiered Distributed Direct Access File System
 *
 * Author:
 * Jonas Pfefferle <jpf@zurich.ibm.com>
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

package com.ibm.crail.datanode.nvmf;

import com.ibm.crail.conf.CrailConfiguration;
import com.ibm.crail.conf.CrailConstants;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

public class NvmfStorageConstants {

	private final static String PREFIX = "crail.datanode.nvmf";

	public static final String IP_ADDR_KEY = "bindip";
	public static InetAddress IP_ADDR;

	public static final String PORT_KEY = "port";
	public static int PORT = 50025;

	public static final String PCIE_ADDR_KEY = "pcieaddr";
	public static String PCIE_ADDR;

	public static final String NAMESPACE_KEY = "namespace";
	public static int NAMESPACE = 1;

	public static final String ALLOCATION_SIZE_KEY = "allocationsize";
	public static long ALLOCATION_SIZE = 1073741824; /* 1GB */

	public static final String HUGEDIR_KEY = "hugedir";
	public static String HUGEDIR  = null;

	public static final String SOCKETMEM_KEY = "socketmem";
	public static long[] SOCKETMEM = {256, 256};

	public static final TimeUnit TIME_UNIT = TimeUnit.MINUTES;
	public static final long TIME_OUT = 15;

	private static String fullKey(String key) {
		return PREFIX + "." + key;
	}

	private static String get(CrailConfiguration conf, String key) {
		return conf.get(fullKey(key));
	}

	public static void updateConstants(CrailConfiguration conf) throws UnknownHostException {
		String arg = get(conf, PCIE_ADDR_KEY);
		if (arg != null) {
			PCIE_ADDR = arg;
		}

		arg = get(conf, NAMESPACE_KEY);
		if (arg != null) {
			NAMESPACE = Integer.parseInt(arg);
		}

		arg = get(conf, IP_ADDR_KEY);
		if (arg != null) {
			IP_ADDR = InetAddress.getByName(arg);
		}

		arg = get(conf, PORT_KEY);
		if (arg != null) {
			PORT = Integer.parseInt(arg);
		}

		arg = get(conf, ALLOCATION_SIZE_KEY);
		if (arg != null) {
			ALLOCATION_SIZE = Long.parseLong(arg);
		}

		arg = get(conf, HUGEDIR_KEY);
		if (arg != null) {
			HUGEDIR = arg.length() == 0 ? null : arg;
		}

		arg = get(conf, SOCKETMEM_KEY);
		if (arg != null) {
			String[] split = arg.split(",");
			SOCKETMEM = new long[split.length];
			for (int i = 0; i < split.length; i++) {
				SOCKETMEM[i] = Long.parseLong(split[i]);
			}
		}
	}

	public static void verify() throws IOException {
		if (NAMESPACE <= 0){
			throw new IOException("Namespace must be > 0");
		}
		if (ALLOCATION_SIZE % CrailConstants.BLOCK_SIZE != 0){
			throw new IOException("allocationsize must be multiple of crail.blocksize");
		}
	}

	public static void printConf(Logger logger) {
		if (IP_ADDR != null) {
			logger.info(fullKey(IP_ADDR_KEY) + " " + IP_ADDR.getHostAddress());
		}
		logger.info(fullKey(PORT_KEY) + " " + PORT);
		logger.info(fullKey(PCIE_ADDR_KEY) + " " + PCIE_ADDR);
		logger.info(fullKey(NAMESPACE_KEY) + " " + NAMESPACE);
		logger.info(fullKey(ALLOCATION_SIZE_KEY) + " " + ALLOCATION_SIZE);
		logger.info(fullKey(HUGEDIR_KEY) + " " + HUGEDIR);
		logger.info(fullKey(SOCKETMEM_KEY) + " " + Arrays.toString(SOCKETMEM));
	}
}
