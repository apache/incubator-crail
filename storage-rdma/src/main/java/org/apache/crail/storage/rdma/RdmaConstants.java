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

package org.apache.crail.storage.rdma;

import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.crail.conf.CrailConfiguration;
import org.apache.crail.conf.CrailConstants;
import org.apache.crail.utils.CrailUtils;
import org.slf4j.Logger;

public class RdmaConstants {
	private static final Logger LOG = CrailUtils.getLogger();

	public static final String STORAGE_RDMA_INTERFACE_KEY = "crail.storage.rdma.interface";
	public static String STORAGE_RDMA_INTERFACE = "eth5";

	public static final String STORAGE_RDMA_PORT_KEY = "crail.storage.rdma.port";
	public static int STORAGE_RDMA_PORT = 50020;

	public static final String STORAGE_RDMA_STORAGE_LIMIT_KEY = "crail.storage.rdma.storagelimit";
	public static long STORAGE_RDMA_STORAGE_LIMIT = 1073741824;

	public static final String STORAGE_RDMA_ALLOCATION_SIZE_KEY = "crail.storage.rdma.allocationsize";
	public static long STORAGE_RDMA_ALLOCATION_SIZE = CrailConstants.REGION_SIZE;

	public static final String STORAGE_RDMA_DATA_PATH_KEY = "crail.storage.rdma.datapath";
	public static String STORAGE_RDMA_DATA_PATH = "/dev/hugepages/data";

	public static final String STORAGE_RDMA_LOCAL_MAP_KEY = "crail.storage.rdma.localmap";
	public static boolean STORAGE_RDMA_LOCAL_MAP = true;

	public static final String STORAGE_RDMA_QUEUESIZE_KEY = "crail.storage.rdma.queuesize";
	public static int STORAGE_RDMA_QUEUESIZE = 32;

	public static final String STORAGE_RDMA_TYPE_KEY = "crail.storage.rdma.type";
	public static String STORAGE_RDMA_TYPE = "passive";

	public static final String STORAGE_RDMA_PERSISTENT_KEY = "crail.storage.rdma.persistent";
	public static boolean STORAGE_RDMA_PERSISTENT = false;
	
	public static final String STORAGE_RDMA_BACKLOG_KEY = "crail.storage.rdma.backlog";
	public static int STORAGE_RDMA_BACKLOG = 100;	
	
	public static final String STORAGE_RDMA_CONNECTTIMEOUT_KEY = "crail.storage.rdma.connecttimeout";
	public static int STORAGE_RDMA_CONNECTTIMEOUT = 1000;		

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
		if (conf.get(STORAGE_RDMA_LOCAL_MAP_KEY) != null) {
			STORAGE_RDMA_LOCAL_MAP = conf.getBoolean(STORAGE_RDMA_LOCAL_MAP_KEY, false);
		}
		if (conf.get(STORAGE_RDMA_QUEUESIZE_KEY) != null) {
			STORAGE_RDMA_QUEUESIZE = Integer.parseInt(conf.get(STORAGE_RDMA_QUEUESIZE_KEY));
		}
		if (conf.get(STORAGE_RDMA_TYPE_KEY) != null) {
			STORAGE_RDMA_TYPE = conf.get(STORAGE_RDMA_TYPE_KEY);
		}
		if (conf.get(STORAGE_RDMA_PERSISTENT_KEY) != null) {
			STORAGE_RDMA_PERSISTENT = conf.getBoolean(STORAGE_RDMA_PERSISTENT_KEY, false);
		}
		if (conf.get(STORAGE_RDMA_BACKLOG_KEY) != null) {
			STORAGE_RDMA_BACKLOG = Integer.parseInt(conf.get(STORAGE_RDMA_BACKLOG_KEY));
		}	
		if (conf.get(STORAGE_RDMA_CONNECTTIMEOUT_KEY) != null) {
			STORAGE_RDMA_CONNECTTIMEOUT = Integer.parseInt(conf.get(STORAGE_RDMA_CONNECTTIMEOUT_KEY));
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
		logger.info(STORAGE_RDMA_LOCAL_MAP_KEY + " " + STORAGE_RDMA_LOCAL_MAP);
		logger.info(STORAGE_RDMA_QUEUESIZE_KEY + " " + STORAGE_RDMA_QUEUESIZE);
		logger.info(STORAGE_RDMA_TYPE_KEY + " " + STORAGE_RDMA_TYPE);
		logger.info(STORAGE_RDMA_BACKLOG_KEY + " " + STORAGE_RDMA_BACKLOG);
		logger.info(STORAGE_RDMA_CONNECTTIMEOUT_KEY + " " + STORAGE_RDMA_CONNECTTIMEOUT);		
	}

	public static void init(CrailConfiguration conf, String[] args) throws IOException {
		if (args != null) {
			Option interfaceOption = Option.builder("i").desc("interface to start server on").hasArg().build();
			Option portOption = Option.builder("p").desc("port to start server on").hasArg().build();
			Option persistencyOption = Option.builder("s").desc("start from persistent state").build();
			Options options = new Options();
			options.addOption(interfaceOption);
			options.addOption(portOption);
			options.addOption(persistencyOption);
			CommandLineParser parser = new DefaultParser();

			try {
				CommandLine line = parser.parse(options, Arrays.copyOfRange(args, 0, args.length));
				if (line.hasOption(interfaceOption.getOpt())) {
					String ifname = line.getOptionValue(interfaceOption.getOpt());
					LOG.info("using custom interface " + ifname);
					conf.set(RdmaConstants.STORAGE_RDMA_INTERFACE_KEY, ifname);
				}
				if (line.hasOption(portOption.getOpt())) {
					String port = line.getOptionValue(portOption.getOpt());
					LOG.info("using custom port " + port);
					conf.set(RdmaConstants.STORAGE_RDMA_PORT_KEY, port);
				}
				if (line.hasOption(persistencyOption.getOpt())) {
					conf.set(RdmaConstants.STORAGE_RDMA_PERSISTENT_KEY, "true");
				}
			} catch (ParseException e) {
				HelpFormatter formatter = new HelpFormatter();
				formatter.printHelp("RDMA storage tier", options);
				System.exit(-1);
			}
		}

		RdmaConstants.updateConstants(conf);
		RdmaConstants.verify();
	}
}
