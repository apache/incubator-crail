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
import java.util.Arrays;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import com.ibm.crail.conf.CrailConfiguration;
import com.ibm.crail.metadata.DataNodeInfo;
import com.ibm.crail.storage.StorageTier;
import com.ibm.crail.storage.StorageEndpoint;
import com.ibm.crail.storage.rdma.client.RdmaStorageActiveEndpointFactory;
import com.ibm.crail.storage.rdma.client.RdmaStorageActiveGroup;
import com.ibm.crail.storage.rdma.client.RdmaStoragePassiveEndpointFactory;
import com.ibm.crail.storage.rdma.client.RdmaStoragePassiveGroup;
import com.ibm.crail.utils.CrailUtils;

public class RdmaStorageTier extends StorageTier {
	private static final Logger LOG = CrailUtils.getLogger();
	
	private MrCache clientMrCache = null;
	private RdmaStorageGroup clientGroup = null;
	
	public RdmaStorageTier(){
		this.clientGroup = null;
		this.clientMrCache = null;
	}
	
	public void init(CrailConfiguration conf, String[] args) throws Exception {
		if (args != null) {
			Option interfaceOption = Option.builder("i").desc("interface to start server on").hasArg().build();
			Option portOption = Option.builder("p").desc("port to start server on").hasArg().build();
			Options options = new Options();
			options.addOption(interfaceOption);
			options.addOption(portOption);
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
			} catch (ParseException e) {
				HelpFormatter formatter = new HelpFormatter();
				formatter.printHelp("RDMA storage tier", options);
				System.exit(-1);
			}
		}

		RdmaConstants.updateConstants(conf);
		RdmaConstants.verify();
	}
	
	public void printConf(Logger logger){
		RdmaConstants.printConf(logger);
	}	
	
	@Override
	public StorageEndpoint createEndpoint(DataNodeInfo info) throws IOException {
		if (clientMrCache == null){
			synchronized(this){
				if (clientMrCache == null){
					this.clientMrCache = new MrCache();
				}
			}
		}
		if (clientGroup == null){
			synchronized(this){
				if (clientGroup == null){
					if (RdmaConstants.STORAGE_RDMA_TYPE.equalsIgnoreCase("passive")){
						LOG.info("passive data client ");
						RdmaStoragePassiveGroup _endpointGroup = new RdmaStoragePassiveGroup(100, RdmaConstants.STORAGE_RDMA_QUEUESIZE, 4, RdmaConstants.STORAGE_RDMA_QUEUESIZE*2, clientMrCache);
						_endpointGroup.init(new RdmaStoragePassiveEndpointFactory(_endpointGroup));
						this.clientGroup = _endpointGroup;
					} else {
						LOG.info("active data client ");
						RdmaStorageActiveGroup _endpointGroup = new RdmaStorageActiveGroup(100, false, RdmaConstants.STORAGE_RDMA_QUEUESIZE, 4, RdmaConstants.STORAGE_RDMA_QUEUESIZE*2, clientMrCache);
						_endpointGroup.init(new RdmaStorageActiveEndpointFactory(_endpointGroup));
						this.clientGroup = _endpointGroup;
					}		
				}
			}
		}
		
		return clientGroup.createEndpoint(info);
	}


	public void close() throws Exception {
		if (clientGroup != null){
			this.clientGroup.close();
		}
	}	
	
	
	public RdmaStorageServer launchServer () throws Exception {
		RdmaStorageServer datanodeServer = new RdmaStorageServer();
		Thread dataNode = new Thread(datanodeServer);
		dataNode.start();
		
		return datanodeServer;
	}
}
