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

package org.apache.crail.storage.tcp;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.crail.conf.CrailConfiguration;
import org.apache.crail.conf.CrailConstants;
import org.apache.crail.storage.StorageResource;
import org.apache.crail.storage.StorageServer;
import org.apache.crail.utils.CrailUtils;
import org.slf4j.Logger;

import com.ibm.narpc.NaRPCServerChannel;
import com.ibm.narpc.NaRPCServerEndpoint;
import com.ibm.narpc.NaRPCServerGroup;
import com.ibm.narpc.NaRPCService;

public class TcpStorageServer implements Runnable, StorageServer, NaRPCService<TcpStorageRequest, TcpStorageResponse> {
	private static final Logger LOG = CrailUtils.getLogger();
	
	private NaRPCServerGroup<TcpStorageRequest, TcpStorageResponse> serverGroup;
	private NaRPCServerEndpoint<TcpStorageRequest, TcpStorageResponse> serverEndpoint;
	private InetSocketAddress address;
	private boolean alive;
	private long regions;
	private long keys;
	private ConcurrentHashMap<Integer, ByteBuffer> dataBuffers;
	private String dataDirPath;
	
	@Override
	public void init(CrailConfiguration conf, String[] args) throws Exception {
		TcpStorageConstants.updateConstants(conf);
		
		this.serverGroup = new NaRPCServerGroup<TcpStorageRequest, TcpStorageResponse>(this, TcpStorageConstants.STORAGE_TCP_QUEUE_DEPTH, (int) CrailConstants.BLOCK_SIZE*2, false);
		this.serverEndpoint = serverGroup.createServerEndpoint();
		this.address = getDataNodeAddress();
		serverEndpoint.bind(address);
		this.alive = false;
		this.regions = TcpStorageConstants.STORAGE_TCP_STORAGE_LIMIT/TcpStorageConstants.STORAGE_TCP_ALLOCATION_SIZE;
		this.keys = 0;
		this.dataBuffers = new ConcurrentHashMap<Integer, ByteBuffer>();
		this.dataDirPath = getDatanodeDirectory(address);
		clean();
	}

	@Override
	public void printConf(Logger logger) {
		TcpStorageConstants.printConf(logger);
	}

	@Override
	public StorageResource allocateResource() throws Exception {
		StorageResource resource = null;
		if (keys < regions){
			int fileId = (int) keys++;
			String dataFilePath = Paths.get(dataDirPath, Integer.toString(fileId)).toString();
			RandomAccessFile dataFile = new RandomAccessFile(dataFilePath, "rw");
			FileChannel dataChannel = dataFile.getChannel();
			ByteBuffer buffer = dataChannel.map(MapMode.READ_WRITE, 0, TcpStorageConstants.STORAGE_TCP_ALLOCATION_SIZE);
			dataBuffers.put(fileId, buffer);
			dataFile.close();
			dataChannel.close();			
			long address = CrailUtils.getAddress(buffer);
			resource = StorageResource.createResource(address, buffer.capacity(), fileId);
//			LOG.info("allocating resource, key " + resource.getKey() + ", address " + resource.getAddress() + ", length " + resource.getLength());
		}
		return resource;
	}

	@Override
	public InetSocketAddress getAddress() {
		return address;
	}

	@Override
	public boolean isAlive() {
		return alive;
	}

	@Override
	public void run() {
		try {
			LOG.info("running TCP storage server, address " + address);
			this.alive = true;
			while(true){
				NaRPCServerChannel endpoint = serverEndpoint.accept();
				LOG.info("new connection " + endpoint.address());
			}
		} catch(Exception e){
			e.printStackTrace();
		}
	}

	@Override
	public TcpStorageRequest createRequest() {
		return new TcpStorageRequest();
	}

	@Override
	public TcpStorageResponse processRequest(TcpStorageRequest request) {
		if (request.type() == TcpStorageProtocol.REQ_WRITE){
			TcpStorageRequest.WriteRequest writeRequest = request.getWriteRequest();
			ByteBuffer buffer = dataBuffers.get(writeRequest.getKey()).duplicate();
			long offset = writeRequest.getAddress() - CrailUtils.getAddress(buffer);
//			LOG.info("processing write request, key " + writeRequest.getKey() + ", address " + writeRequest.getAddress() + ", length " + writeRequest.length() + ", remaining " + writeRequest.getBuffer().remaining() + ", offset " + offset);
			buffer.clear().position((int) offset);
			buffer.put(writeRequest.getBuffer());
			TcpStorageResponse.WriteResponse writeResponse = new TcpStorageResponse.WriteResponse(writeRequest.length());
			return new TcpStorageResponse(writeResponse);
		} else if (request.type() == TcpStorageProtocol.REQ_READ){
			TcpStorageRequest.ReadRequest readRequest = request.getReadRequest();
			ByteBuffer buffer = dataBuffers.get(readRequest.getKey()).duplicate();
			long offset = readRequest.getAddress() - CrailUtils.getAddress(buffer);
//			LOG.info("processing read request, address " + readRequest.getAddress() + ", length " + readRequest.length() + ", offset " + offset);
			long limit = offset + readRequest.length();
			buffer.clear().position((int) offset).limit((int) limit);
			TcpStorageResponse.ReadResponse readResponse = new TcpStorageResponse.ReadResponse(buffer);
			return new TcpStorageResponse(readResponse);
		} else {
			LOG.info("processing unknown request");
			return new TcpStorageResponse(TcpStorageProtocol.RET_RPC_UNKNOWN);
		}
	}
	
	private void clean(){
		File dataDir = new File(dataDirPath);
		if (!dataDir.exists()){
			dataDir.mkdirs();
		}
		for (File child : dataDir.listFiles()) {
			child.delete();
		}
	}	
	
	public static InetSocketAddress getDataNodeAddress() throws IOException {
		String ifname = TcpStorageConstants.STORAGE_TCP_INTERFACE;
		int port = TcpStorageConstants.STORAGE_TCP_PORT;
		
		NetworkInterface netif = NetworkInterface.getByName(ifname);
		if (netif == null){
			return null;
		}
		List<InterfaceAddress> addresses = netif.getInterfaceAddresses();
		InetAddress addr = null;
		for (InterfaceAddress address: addresses){
			if (address.getBroadcast() != null){
				InetAddress _addr = address.getAddress();
				addr = _addr;
			}
		}		
		InetSocketAddress inetAddr = new InetSocketAddress(addr, port);
		return inetAddr;
	}	
	
	public static String getDatanodeDirectory(InetSocketAddress inetAddress){
		String address = inetAddress.getAddress().toString();
		return TcpStorageConstants.STORAGE_TCP_DATA_PATH + address + "-"  + inetAddress.getPort();
	}	
}
