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

package org.apache.crail.storage.nvmf;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.crail.conf.CrailConfiguration;
import org.apache.crail.conf.CrailConstants;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;

public class NvmfStorageConstants {

	private final static String PREFIX = "crail.storage.nvmf";

	public static final String IP_ADDR_KEY = "ip";
	public static InetAddress IP_ADDR;

	public static final String PORT_KEY = "port";
	public static int PORT = 50025;

	public static final String NQN_KEY = "nqn";
	public static String NQN = "nqn.2016-06.io.spdk:cnode1";

	public static final String NAMESPACE_KEY = "namespace";
	public static int NAMESPACE = 1;

	public static final String ALLOCATION_SIZE_KEY = "allocationsize";
	public static long ALLOCATION_SIZE = 1073741824; /* 1GB */

	public static final String SERVER_MEMPOOL_KEY = "servermempool";
	public static long SERVER_MEMPOOL = 256;

	public static final String CLIENT_MEMPOOL_KEY = "clientmempool";
	public static long CLIENT_MEMPOOL = 256;

	public static final TimeUnit TIME_UNIT = TimeUnit.MINUTES;
	public static final long TIME_OUT = 15;

	private static String fullKey(String key) {
		return PREFIX + "." + key;
	}

	private static String get(CrailConfiguration conf, String key) {
		return conf.get(fullKey(key));
	}

	public static void updateConstants(CrailConfiguration conf) throws UnknownHostException {
		String arg = get(conf, NAMESPACE_KEY);
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

		arg = get(conf, NQN_KEY);
		if (arg != null) {
			NQN = arg;
		}

		arg = get(conf, ALLOCATION_SIZE_KEY);
		if (arg != null) {
			ALLOCATION_SIZE = Long.parseLong(arg);
		}

		arg = get(conf, SERVER_MEMPOOL_KEY);
		if (arg != null) {
			SERVER_MEMPOOL = Long.parseLong(arg);
		}

		arg = get(conf, CLIENT_MEMPOOL_KEY);
		if (arg != null) {
			CLIENT_MEMPOOL = Long.parseLong(arg);
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
		logger.info(fullKey(NQN_KEY) + " " + NQN);
		logger.info(fullKey(NAMESPACE_KEY) + " " + NAMESPACE);
		logger.info(fullKey(ALLOCATION_SIZE_KEY) + " " + ALLOCATION_SIZE);
		logger.info(fullKey(SERVER_MEMPOOL_KEY) + " " + SERVER_MEMPOOL);
		logger.info(fullKey(CLIENT_MEMPOOL_KEY) + " " + CLIENT_MEMPOOL);
	}
	
	public static void parseCmdLine(CrailConfiguration crailConfiguration, String[] args) throws IOException {
		NvmfStorageConstants.updateConstants(crailConfiguration);

		if (args != null) {
			Options options = new Options();

			Option bindIp = Option.builder("a").desc("target ip address").hasArg().build();
			if (IP_ADDR == null) {
				bindIp.setRequired(true);
			}
			Option port = Option.builder("p").desc("target port").hasArg().type(Number.class).build();
			Option nqn = Option.builder("nqn").desc("target subsystem NQN").hasArg().build();
			options.addOption(bindIp);
			options.addOption(port);
			options.addOption(nqn);
			CommandLineParser parser = new DefaultParser();
			HelpFormatter formatter = new HelpFormatter();
			CommandLine line = null;
			try {
				line = parser.parse(options, args);
				if (line.hasOption(port.getOpt())) {
					NvmfStorageConstants.PORT = ((Number) line.getParsedOptionValue(port.getOpt())).intValue();
				}
			} catch (ParseException e) {
				System.err.println(e.getMessage());
				formatter.printHelp("NVMe storage tier", options);
				System.exit(-1);
			}
			if (line.hasOption(bindIp.getOpt())) {
				NvmfStorageConstants.IP_ADDR = InetAddress.getByName(line.getOptionValue(bindIp.getOpt()));
			}
			if (line.hasOption(nqn.getOpt())) {
				NvmfStorageConstants.NQN = line.getOptionValue(nqn.getOpt());
			}
		}

		NvmfStorageConstants.verify();
	}	
}
