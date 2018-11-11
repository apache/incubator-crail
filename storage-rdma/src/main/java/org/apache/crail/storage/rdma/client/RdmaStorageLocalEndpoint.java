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

package org.apache.crail.storage.rdma.client;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.crail.CrailBuffer;
import org.apache.crail.conf.CrailConstants;
import org.apache.crail.memory.OffHeapBuffer;
import org.apache.crail.metadata.BlockInfo;
import org.apache.crail.storage.StorageEndpoint;
import org.apache.crail.storage.StorageFuture;
import org.apache.crail.storage.StorageUtils;
import org.apache.crail.storage.rdma.RdmaConstants;
import org.apache.crail.utils.CrailUtils;
import org.slf4j.Logger;

import com.ibm.disni.verbs.*;

import sun.misc.Unsafe;

public class RdmaStorageLocalEndpoint implements StorageEndpoint {
	private static final Logger LOG = CrailUtils.getLogger();
	private ConcurrentHashMap<Long, CrailBuffer> bufferMap;
	private Unsafe unsafe;
	private InetSocketAddress address;
	
	public RdmaStorageLocalEndpoint(InetSocketAddress datanodeAddr) throws Exception {
		LOG.info("new local endpoint for address " + datanodeAddr);
		String dataPath = StorageUtils.getDatanodeDirectory(RdmaConstants.STORAGE_RDMA_DATA_PATH, datanodeAddr);
		File dataDir = new File(dataPath);
		if (!dataDir.exists()){
			throw new Exception("Local RDMA data path missing");
		}
		this.address = datanodeAddr;
		this.bufferMap = new ConcurrentHashMap<Long, CrailBuffer>();
		this.unsafe = getUnsafe();
		for (File dataFile : dataDir.listFiles()) {
			long lba = Long.parseLong(dataFile.getName());
			OffHeapBuffer offHeapBuffer = OffHeapBuffer.wrap(mmap(dataFile));
			bufferMap.put(lba, offHeapBuffer);
		}				
	}

	@Override
	public StorageFuture write(CrailBuffer buffer, BlockInfo remoteMr, long remoteOffset) throws IOException,
			InterruptedException {
		if (buffer.remaining() > CrailConstants.BLOCK_SIZE){
			throw new IOException("write size too large " + buffer.remaining());
		}
		if (buffer.remaining() <= 0){
			throw new IOException("write size too small, len " + buffer.remaining());
		}	
		if (remoteOffset < 0){
			throw new IOException("remote offset too small " + remoteOffset);
		}	
		
		long alignedLba = getAlignedLba(remoteMr.getLba());
		long lbaOffset = getLbaOffset(remoteMr.getLba());
		
		CrailBuffer mappedBuffer = bufferMap.get(alignedLba);
		if (mappedBuffer == null){
			throw new IOException("No mapped buffer for this key " + remoteMr.getLkey() + ", address " + address);
		}
		
		if (lbaOffset + remoteOffset + buffer.remaining() > RdmaConstants.STORAGE_RDMA_ALLOCATION_SIZE){
			long tmpAddr = lbaOffset + remoteOffset + buffer.remaining();
			throw new IOException("remote fileOffset + remoteOffset + len too large " + tmpAddr);
		}		
		long srcAddr = buffer.address() + buffer.position();
		long dstAddr = mappedBuffer.address() + lbaOffset + remoteOffset; 
		RdmaLocalFuture future = new RdmaLocalFuture(unsafe, srcAddr, dstAddr, buffer.remaining());
		return future;
	}

	@Override
	public StorageFuture read(CrailBuffer buffer, BlockInfo remoteMr, long remoteOffset) throws IOException,
			InterruptedException {
		if (buffer.remaining() > CrailConstants.BLOCK_SIZE){
			throw new IOException("read size too large");
		}	
		if (buffer.remaining() <= 0){
			throw new IOException("read size too small, len " + buffer.remaining());
		}
		if (remoteOffset < 0){
			throw new IOException("remote offset too small " + remoteOffset);
		}
		if (buffer.position() < 0){
			throw new IOException("local offset too small " + buffer.position());
		}
		
		long alignedLba = getAlignedLba(remoteMr.getLba());
		long lbaOffset = getLbaOffset(remoteMr.getLba());
		
		CrailBuffer mappedBuffer = bufferMap.get(alignedLba);
		if (mappedBuffer == null){
			throw new IOException("No mapped buffer for this key");
		}
		if (lbaOffset + remoteOffset + buffer.remaining() > RdmaConstants.STORAGE_RDMA_ALLOCATION_SIZE){
			long tmpAddr = lbaOffset + remoteOffset + buffer.remaining();
			throw new IOException("remote fileOffset + remoteOffset + len too large " + tmpAddr);
		}			
		long srcAddr = mappedBuffer.address() + lbaOffset + remoteOffset;
		long dstAddr = buffer.address() + buffer.position();
		RdmaLocalFuture future = new RdmaLocalFuture(unsafe, srcAddr, dstAddr, buffer.remaining());
		return future;
	}
	
	private static long getAlignedLba(long remoteLba){
		return remoteLba / RdmaConstants.STORAGE_RDMA_ALLOCATION_SIZE;
	}
	
	private static long getLbaOffset(long remoteLba){
		return remoteLba % RdmaConstants.STORAGE_RDMA_ALLOCATION_SIZE;
	}

	@Override
	public void close() throws IOException, InterruptedException {
	}

	public void connect(SocketAddress inetAddress, int i)
			throws InterruptedException, IOException {
	}

	public int getEndpointId() {
		return 0;
	}

	public int getFreeSlots() {
		return 0;
	}

	public boolean isConnected() {
		return true;
	}

	public IbvQP getQp() {
		return null;
	}

	public String getAddress() throws IOException {
		return null;
	}

	public RdmaCmId getContext() {
		return null;
	}
	
	private MappedByteBuffer mmap(File file) throws IOException{
		RandomAccessFile randomFile = new RandomAccessFile(file.getAbsolutePath(), "rw");
		FileChannel channel = randomFile.getChannel();
		MappedByteBuffer mappedBuffer = channel.map(MapMode.READ_WRITE, 0, RdmaConstants.STORAGE_RDMA_ALLOCATION_SIZE);
		randomFile.close();
		return mappedBuffer;
	}	
	
	private Unsafe getUnsafe() throws Exception {
		Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
		theUnsafe.setAccessible(true);
		Unsafe unsafe = (Unsafe) theUnsafe.get(null);
		return unsafe;
	}

	@Override
	public boolean isLocal() {
		return true;
	}

}
