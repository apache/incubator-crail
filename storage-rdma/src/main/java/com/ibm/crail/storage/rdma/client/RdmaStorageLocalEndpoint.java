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

package com.ibm.crail.storage.rdma.client;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.concurrent.ConcurrentHashMap;

import com.ibm.crail.CrailBuffer;
import com.ibm.crail.conf.CrailConstants;
import com.ibm.crail.memory.OffHeapBuffer;
import com.ibm.crail.metadata.BlockInfo;
import com.ibm.crail.storage.StorageEndpoint;
import com.ibm.crail.storage.StorageFuture;
import com.ibm.crail.storage.rdma.RdmaConstants;
import com.ibm.crail.storage.rdma.RdmaStorageServer;
import com.ibm.disni.rdma.verbs.*;
import sun.misc.Unsafe;

public class RdmaStorageLocalEndpoint implements StorageEndpoint {
	private String indexDirPath;
	private ConcurrentHashMap<Integer, CrailBuffer> bufferMap;
	private ConcurrentHashMap<Integer, RdmaBlockIndex> indexMap;
	private Unsafe unsafe;
	
	public RdmaStorageLocalEndpoint(InetSocketAddress datanodeAddr) throws IOException {
		if (datanodeAddr == null){
			throw new IOException("Datanode address not valid!");
		}
		
		try {
			this.bufferMap = new ConcurrentHashMap<Integer, CrailBuffer>();
			this.indexMap = new ConcurrentHashMap<Integer, RdmaBlockIndex>();
			this.unsafe = getUnsafe();
			this.indexDirPath = RdmaStorageServer.getIndexDirectory(datanodeAddr);
			File indexDir = new File(indexDirPath);
			ByteBuffer fileBuffer = ByteBuffer.allocate(CrailConstants.BUFFER_SIZE);
			if (indexDir.exists()){
				for (File indexFile : indexDir.listFiles()) {
					FileInputStream indexStream = new FileInputStream(indexFile);
					FileChannel indexChannel = indexStream.getChannel();
					fileBuffer.clear();
					indexChannel.read(fileBuffer);
					fileBuffer.flip();
					RdmaBlockIndex blockIndex = new RdmaBlockIndex();
					blockIndex.update(fileBuffer);
					File dataFile = new File(blockIndex.getPath());
					MappedByteBuffer mappedBuffer = mmap(dataFile);
					indexStream.close();
					indexChannel.close();
					
					bufferMap.put(blockIndex.getKey(), OffHeapBuffer.wrap(mappedBuffer));
					indexMap.put(blockIndex.getKey(), blockIndex);
				}				
			}			
		} catch(Exception e){
			throw new IOException(e);
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
		
		CrailBuffer mappedBuffer = bufferMap.get(remoteMr.getLkey());
		if (mappedBuffer == null){
			throw new IOException("No mapped buffer for this key");
		}
		RdmaBlockIndex blockIndex = indexMap.get(remoteMr.getLkey());
		if (blockIndex == null){
			throw new IOException("No index for this key");
		}
		
		long blockOffset = remoteMr.getAddr() - blockIndex.getAddr();
		if (blockOffset + remoteOffset + buffer.remaining() > RdmaConstants.STORAGE_RDMA_ALLOCATION_SIZE){
			long tmpAddr = blockOffset + remoteOffset + buffer.remaining();
			throw new IOException("remote fileOffset + remoteOffset + len too large " + tmpAddr);
		}		
		long srcAddr = buffer.address() + buffer.position();
		long dstAddr = mappedBuffer.address() + blockOffset + remoteOffset; 
//		unsafe.copyMemory(srcAddr, dstAddr, buffer.remaining());
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
		if (buffer.position() < 0){
			throw new IOException("local offset too small " + buffer.position());
		}
		if (remoteOffset < 0){
			throw new IOException("remote offset too small " + remoteOffset);
		}
		
		CrailBuffer mappedBuffer = bufferMap.get(remoteMr.getLkey());
		if (mappedBuffer == null){
			throw new IOException("No mapped buffer for this key");
		}
		RdmaBlockIndex blockIndex = indexMap.get(remoteMr.getLkey());
		if (blockIndex == null){
			throw new IOException("No index for this key");
		}		

		long blockOffset = remoteMr.getAddr() - blockIndex.getAddr();
		if (blockOffset + remoteOffset + buffer.remaining() > RdmaConstants.STORAGE_RDMA_ALLOCATION_SIZE){
			long tmpAddr = blockOffset + remoteOffset + buffer.remaining();
			throw new IOException("remote fileOffset + remoteOffset + len too large " + tmpAddr);
		}			
		long srcAddr = mappedBuffer.address() + blockOffset + remoteOffset;
		long dstAddr = buffer.address() + buffer.position();
//		unsafe.copyMemory(srcAddr, dstAddr, buffer.remaining());
		RdmaLocalFuture future = new RdmaLocalFuture(unsafe, srcAddr, dstAddr, buffer.remaining());
		return future;
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
