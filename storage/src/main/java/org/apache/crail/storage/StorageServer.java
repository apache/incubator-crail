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

package org.apache.crail.storage;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.crail.CrailStorageClass;
import org.apache.crail.conf.Configurable;
import org.apache.crail.conf.CrailConfiguration;
import org.apache.crail.conf.CrailConstants;
import org.apache.crail.metadata.DataNodeStatistics;
import org.apache.crail.rpc.RpcClient;
import org.apache.crail.rpc.RpcConnection;
import org.apache.crail.rpc.RpcDispatcher;
import org.apache.crail.utils.CrailUtils;
import org.slf4j.Logger;

public interface StorageServer extends Configurable, Runnable {
	public abstract StorageResource allocateResource() throws Exception;
	public abstract boolean isAlive();
	public abstract InetSocketAddress getAddress();
	
	public static void main(String[] args) throws Exception {
		Logger LOG = CrailUtils.getLogger();
		CrailConfiguration conf = CrailConfiguration.createConfigurationFromFile();
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
				}				
				if (line.hasOption(classOption.getOpt())) {
					storageClass = Integer.parseInt(line.getOptionValue(classOption.getOpt()));
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
		StorageServer server = storageTier.launchServer();
		
		String extraParams[] = null;
		splitIndex++;
		if (args.length > splitIndex){
			extraParams = new String[args.length - splitIndex];
			for (int i = splitIndex; i < args.length; i++){
				extraParams[i-splitIndex] = args[i];
			}
		}
		server.init(conf, extraParams);
		server.printConf(LOG);
		
		Thread thread = new Thread(server);
		thread.start();
		
		RpcClient rpcClient = RpcClient.createInstance(CrailConstants.NAMENODE_RPC_TYPE);
		rpcClient.init(conf, args);
		rpcClient.printConf(LOG);					
		
		ConcurrentLinkedQueue<InetSocketAddress> namenodeList = CrailUtils.getNameNodeList();
		ConcurrentLinkedQueue<RpcConnection> connectionList = new ConcurrentLinkedQueue<RpcConnection>();
		while(!namenodeList.isEmpty()){
			InetSocketAddress address = namenodeList.poll();
			RpcConnection connection = rpcClient.connect(address);
			connectionList.add(connection);
		}
		RpcConnection rpcConnection = connectionList.peek();
		if (connectionList.size() > 1){
			rpcConnection = new RpcDispatcher(connectionList);
		}		
		LOG.info("connected to namenode(s) " + rpcConnection.toString());				
		
		
		StorageRpcClient storageRpc = new StorageRpcClient(storageType, CrailStorageClass.get(storageClass), server.getAddress(), rpcConnection);
		
		HashMap<Long, Long> blockCount = new HashMap<Long, Long>();
		long sumCount = 0;
		long lba = 0;
		while (server.isAlive()) {
			StorageResource resource = server.allocateResource();
			if (resource == null){
				break;
			} else {
				storageRpc.setBlock(lba, resource.getAddress(), resource.getLength(), resource.getKey());
				lba += (long) resource.getLength();
				
				DataNodeStatistics stats = storageRpc.getDataNode();
				long newCount = stats.getFreeBlockCount();
				long serviceId = stats.getServiceId();
				
				long oldCount = 0;
				if (blockCount.containsKey(serviceId)){
					oldCount = blockCount.get(serviceId);
				}
				long diffCount = newCount - oldCount;
				blockCount.put(serviceId, newCount);
				sumCount += diffCount;
				LOG.info("datanode statistics, freeBlocks " + sumCount);		
			}
		}
		
		while (server.isAlive()) {
			DataNodeStatistics stats = storageRpc.getDataNode();
			long newCount = stats.getFreeBlockCount();
			long serviceId = stats.getServiceId();
			
			long oldCount = 0;
			if (blockCount.containsKey(serviceId)){
				oldCount = blockCount.get(serviceId);
			}
			long diffCount = newCount - oldCount;
			blockCount.put(serviceId, newCount);
			sumCount += diffCount;			
			
			LOG.info("datanode statistics, freeBlocks " + sumCount);
			Thread.sleep(CrailConstants.STORAGE_KEEPALIVE*1000);
		}			
	}
}
