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

package com.ibm.crail.storage;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;

import com.ibm.crail.CrailStorageClass;
import com.ibm.crail.conf.CrailConfiguration;
import com.ibm.crail.conf.CrailConstants;
import com.ibm.crail.metadata.DataNodeStatistics;
import com.ibm.crail.rpc.RpcClient;
import com.ibm.crail.rpc.RpcConnection;
import com.ibm.crail.utils.CrailUtils;

public interface StorageServer {
	public abstract StorageResource allocateResource() throws Exception;
	public abstract boolean isAlive();
	public abstract InetSocketAddress getAddress();
	
	public static void main(String[] args) throws Exception {
		Logger LOG = CrailUtils.getLogger();
		CrailConfiguration conf = new CrailConfiguration();
		CrailConstants.updateConstants(conf);
		CrailConstants.printConf();
		CrailConstants.verify();
		
		int splitIndex = 0;
		for (String param : args){
			if (param.equalsIgnoreCase("--")){
				break;
			} 
			splitIndex++;
		}
		
		//default values
		StringTokenizer tokenizer = new StringTokenizer(CrailConstants.STORAGE_TYPES, ",");
		if (!tokenizer.hasMoreTokens()){
			throw new Exception("No storage types defined!");
		}
		String storageName = tokenizer.nextToken();
		int storageType = 0;
		HashMap<String, Integer> storageTypes = new HashMap<String, Integer>();
		storageTypes.put(storageName, storageType);
		for (int type = 1; tokenizer.hasMoreElements(); type++){
			String name = tokenizer.nextToken();
			storageTypes.put(name, type);
		}
		int storageClass = -1;
		
		//custom values
		if (args != null) {
			Option typeOption = Option.builder("t").desc("storage type to start").hasArg().build();
			Option classOption = Option.builder("c").desc("storage class the server will attach to").hasArg().build();
			Options options = new Options();
			options.addOption(typeOption);
			options.addOption(classOption);
			CommandLineParser parser = new DefaultParser();
			
			try {
				CommandLine line = parser.parse(options, Arrays.copyOfRange(args, 0, splitIndex));
				if (line.hasOption(typeOption.getOpt())) {
					storageName = line.getOptionValue(typeOption.getOpt());
					storageType = storageTypes.get(storageName).intValue();
					System.out.println("has custom storageName " + storageName);
				}				
				if (line.hasOption(classOption.getOpt())) {
					storageClass = Integer.parseInt(line.getOptionValue(classOption.getOpt()));
					System.out.println("has custom storageClass " + storageClass);
				}					
			} catch (ParseException e) {
				HelpFormatter formatter = new HelpFormatter();
				formatter.printHelp("Storage tier", options);
				System.exit(-1);
			}
		}
		if (storageClass < 0){
			storageClass = storageType;
		}
		
		StorageTier storageTier = StorageTier.createInstance(storageName);
		if (storageTier == null){
			throw new Exception("Cannot instantiate datanode of type " + storageName);
		}
		
		String extraParams[] = null;
		splitIndex++;
		if (args.length > splitIndex){
			extraParams = new String[args.length - splitIndex];
			for (int i = splitIndex; i < args.length; i++){
				extraParams[i-splitIndex] = args[i];
			}
		}
		storageTier.init(conf, extraParams);
		storageTier.printConf(LOG);		
		
		InetSocketAddress nnAddr = CrailUtils.getNameNodeAddress();
		RpcClient rpcClient = RpcClient.createInstance(CrailConstants.NAMENODE_RPC_TYPE);
		rpcClient.init(conf, args);
		rpcClient.printConf(LOG);					
		RpcConnection rpcConnection = rpcClient.connect(nnAddr);
		LOG.info("connected to namenode at " + nnAddr);				
		
		StorageServer server = storageTier.launchServer();
		StorageRpcClient storageRpc = new StorageRpcClient(storageType, CrailStorageClass.get(storageClass), server.getAddress(), rpcConnection);
		
		while (server.isAlive()) {
			StorageResource resource = server.allocateResource();
			if (resource == null){
				break;
			} else {
				storageRpc.setBlock(resource.getAddress(), resource.getLength(), resource.getKey());
				LOG.info("datanode statistics, freeBlocks " + storageRpc.getDataNode().getFreeBlockCount());		
			}
		}
		
		while (server.isAlive()) {
			DataNodeStatistics statistics = storageRpc.getDataNode();
			LOG.info("datanode statistics, freeBlocks " + statistics.getFreeBlockCount());
			Thread.sleep(2000);
		}			
	}
}
