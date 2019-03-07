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

package org.apache.crail.storage.nvmf;

import com.ibm.jnvmf.NamespaceIdentifier;
import com.ibm.jnvmf.NvmeQualifiedName;
import org.apache.commons.cli.*;
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
	public static NvmeQualifiedName NQN = new NvmeQualifiedName("nqn.2017-06.io.crail:cnode");

	public static final String HOST_NQN_KEY = "hostnqn";
	public static NvmeQualifiedName HOST_NQN;

	/* this is a server property, the client will get the nsid from the namenode */
	public static NamespaceIdentifier NAMESPACE = new NamespaceIdentifier(1);

	public static final String ALLOCATION_SIZE_KEY = "allocationsize";
	public static int ALLOCATION_SIZE = 1073741824; /* 1GB */

	public static final String QUEUE_SIZE_KEY = "queueSize";
	public static int QUEUE_SIZE = 64;

	public static final String STAGING_CACHE_SIZE_KEY = "stagingcachesize";
	public static int STAGING_CACHE_SIZE = 262144;

	/* We use the default keep alive timer of 120s in jNVMf */
	public static long KEEP_ALIVE_INTERVAL_MS = TimeUnit.MILLISECONDS.convert(60, TimeUnit.SECONDS);

	private static String fullKey(String key) {
		return PREFIX + "." + key;
	}

	private static String get(CrailConfiguration conf, String key) {
		return conf.get(fullKey(key));
	}

	public static void updateConstants(CrailConfiguration conf) throws UnknownHostException {
		String arg = get(conf, IP_ADDR_KEY);
		if (arg != null) {
			IP_ADDR = InetAddress.getByName(arg);
		}

		arg = get(conf, PORT_KEY);
		if (arg != null) {
			PORT = Integer.parseInt(arg);
		}

		arg = get(conf, NQN_KEY);
		if (arg != null) {
			NQN = new NvmeQualifiedName(arg);
		}

		arg = get(conf, HOST_NQN_KEY);
		if (arg != null) {
			HOST_NQN = new NvmeQualifiedName(arg);
		}

		arg = get(conf, ALLOCATION_SIZE_KEY);
		if (arg != null) {
			ALLOCATION_SIZE = Integer.parseInt(arg);
		}

		arg = get(conf, QUEUE_SIZE_KEY);
		if (arg != null) {
			QUEUE_SIZE = Integer.parseInt(arg);
		}

		arg = get(conf, STAGING_CACHE_SIZE_KEY);
		if (arg != null) {
			STAGING_CACHE_SIZE = Integer.parseInt(arg);
		}
	}

	public static void verify() {
		if (ALLOCATION_SIZE % CrailConstants.BLOCK_SIZE != 0){
			throw new IllegalArgumentException(fullKey(ALLOCATION_SIZE_KEY) + " (" + ALLOCATION_SIZE +
					") must be multiple of crail.blocksize (" + CrailConstants.BLOCK_SIZE + ")");
		}
		if (QUEUE_SIZE < 0) {
			throw new IllegalArgumentException("Queue size negative");
		}
	}

	public static void printConf(Logger logger) {
		if (IP_ADDR != null) {
			logger.info(fullKey(IP_ADDR_KEY) + " " + IP_ADDR.getHostAddress());
		}
		logger.info(fullKey(PORT_KEY) + " " + PORT);
		logger.info(fullKey(NQN_KEY) + " " + NQN);
		logger.info(fullKey(HOST_NQN_KEY) + " " + HOST_NQN);
		logger.info(fullKey(ALLOCATION_SIZE_KEY) + " " + ALLOCATION_SIZE);
		logger.info(fullKey(QUEUE_SIZE_KEY) + " " + QUEUE_SIZE);
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
			Option namespace = Option.builder("n").desc("namespace id").hasArg().type(Number.class).build();
			Option nqn = Option.builder("nqn").desc("target subsystem NQN").hasArg().build();
			Option hostnqn = Option.builder("hostnqn").desc("host NQN").hasArg().build();
			options.addOption(bindIp);
			options.addOption(port);
			options.addOption(nqn);
			options.addOption(hostnqn);
			options.addOption(namespace);
			CommandLineParser parser = new DefaultParser();
			HelpFormatter formatter = new HelpFormatter();
			CommandLine line = null;
			try {
				line = parser.parse(options, args);
				if (line.hasOption(port.getOpt())) {
					NvmfStorageConstants.PORT = ((Number) line.getParsedOptionValue(port.getOpt())).intValue();
				}
				if (line.hasOption(namespace.getOpt())) {
					NvmfStorageConstants.NAMESPACE = new
							NamespaceIdentifier(((Number) line.getParsedOptionValue(namespace.getOpt())).intValue());
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
				NvmfStorageConstants.NQN = new NvmeQualifiedName(line.getOptionValue(nqn.getOpt()));
			}
			if (line.hasOption(hostnqn.getOpt())) {
				NvmfStorageConstants.HOST_NQN = new NvmeQualifiedName(line.getOptionValue(hostnqn.getOpt()));
			}
		}

		NvmfStorageConstants.verify();
	}
}
