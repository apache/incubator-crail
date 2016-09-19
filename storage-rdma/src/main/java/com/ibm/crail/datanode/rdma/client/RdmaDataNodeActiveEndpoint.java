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

package com.ibm.crail.datanode.rdma.client;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;

import com.ibm.crail.conf.CrailConstants;
import com.ibm.crail.datanode.DataNodeEndpoint;
import com.ibm.crail.datanode.DataResult;
import com.ibm.crail.datanode.rdma.MrCache;
import com.ibm.crail.datanode.rdma.MrCache.DeviceMrCache;
import com.ibm.crail.datanode.rdma.RdmaConstants;
import com.ibm.crail.namenode.protocol.BlockInfo;
import com.ibm.crail.utils.AtomicIntegerModulo;
import com.ibm.disni.util.MemoryUtils;
import com.ibm.disni.verbs.*;
import com.ibm.disni.verbs.SVCPostSend.SendWRMod;
import com.ibm.disni.verbs.SVCPostSend.SgeMod;
import com.ibm.disni.endpoints.*;

public class RdmaDataNodeActiveEndpoint extends RdmaActiveClientEndpoint implements DataNodeEndpoint {
	private LinkedBlockingQueue<SVCPostSend> writeOps;
	private LinkedBlockingQueue<SVCPostSend> readOps;
	private AtomicIntegerModulo opcount;
	private Semaphore sendQueueAvailable;
	private ConcurrentHashMap<Long, RdmaDataActiveFuture> futureMap;
	private MrCache mrCache;
	private DeviceMrCache deviceCache;
	
	public RdmaDataNodeActiveEndpoint(RdmaDataNodeActiveGroup group, RdmaCmId id) throws IOException {
		super(group, id);
		writeOps = new LinkedBlockingQueue<SVCPostSend>();
		readOps = new LinkedBlockingQueue<SVCPostSend>();
		this.opcount = new AtomicIntegerModulo();
		this.futureMap = new ConcurrentHashMap<Long, RdmaDataActiveFuture>();
		this.sendQueueAvailable = new Semaphore(group.getMaxWR());
		this.mrCache = group.getMrCache();
		this.deviceCache = null;
	}

	@Override
	protected synchronized void init() throws IOException {
		super.init();
		
		for (int i = 0; i < RdmaConstants.DATANODE_RDMA_CONCURRENT_POSTS; i++){
			SVCPostSend write = initWriteOp();
			writeOps.add(write);
			SVCPostSend read = initReadOp();
			readOps.add(read);
		}
	}
	
	private SVCPostSend initWriteOp() throws IOException {
		LinkedList<IbvSendWR> wrList_send = new LinkedList<IbvSendWR>();
		
		IbvSendWR writeWR = new IbvSendWR();
		writeWR.setWr_id(opcount.getAndIncrement());
		writeWR.setOpcode(IbvSendWR.IBV_WR_RDMA_WRITE);
		LinkedList<IbvSge> sgeListWrite = new LinkedList<IbvSge>();
		IbvSge sgeSendWrite = new IbvSge();
		sgeListWrite.add(sgeSendWrite);
		writeWR.setSg_list(sgeListWrite);
		wrList_send.add(writeWR);
	
		IbvSendWR readWR = new IbvSendWR();
		readWR.setWr_id(opcount.getAndIncrement());
		readWR.setOpcode(IbvSendWR.IBV_WR_RDMA_READ);
		readWR.setSend_flags(IbvSendWR.IBV_SEND_SIGNALED);
		
		LinkedList<IbvSge> sgeListRead = new LinkedList<IbvSge>();
		IbvSge sgeSendRead = new IbvSge();
		sgeSendRead.setLength(1);
		sgeListRead.add(sgeSendRead);
		readWR.setSg_list(sgeListRead);
		wrList_send.add(readWR);
		
		SVCPostSend rdmaOp = this.postSend(wrList_send);
		return rdmaOp;
	}

	private SVCPostSend initReadOp() throws IOException{
		LinkedList<IbvSendWR> wrList_send = new LinkedList<IbvSendWR>();
		LinkedList<IbvSge> sgeList = new LinkedList<IbvSge>();
		IbvSge sgeSend = new IbvSge();
		IbvSendWR sendWR = new IbvSendWR();
		
		sgeList.add(sgeSend);
		sendWR.setSg_list(sgeList);
		wrList_send.add(sendWR);			
		
		sendWR.setWr_id(opcount.getAndIncrement());
		sendWR.setOpcode(IbvSendWR.IBV_WR_RDMA_READ);
		sendWR.setSend_flags(IbvSendWR.IBV_SEND_SIGNALED);
		
		SVCPostSend rdmaOp = this.postSend(wrList_send);
		return rdmaOp;
	}
	
	@Override
	public Future<DataResult> write(ByteBuffer buffer, ByteBuffer region, BlockInfo remoteMr, long remoteOffset) throws IOException, InterruptedException {
		if (buffer.remaining() > CrailConstants.BLOCK_SIZE){
			throw new IOException("write size too large " + buffer.remaining());
		}
		if (buffer.remaining() <= 0){
			throw new IOException("write size too small, len " + buffer.remaining());
		}	
		if (remoteOffset < 0){
			throw new IOException("remote offset too small " + remoteOffset);
		}	
		if (remoteMr.getAddr() == 0){
			throw new IOException("remote addr is 0 " + remoteMr.getAddr());
		}		
		if (remoteMr.getLkey() == 0){
			throw new IOException("remote key is 0 " + remoteMr.getLkey());
		}
		
		if (deviceCache == null){
			deviceCache = mrCache.getDeviceCache(this.getPd());
		}
		IbvMr localMr = deviceCache.get(region);
		if (localMr == null){
			localMr = this.registerMemory(region).execute().free().getMr();
			deviceCache.put(localMr);
		}
		long bufferAddress = MemoryUtils.getAddress(buffer);
		
		SVCPostSend writeOp = writeOps.take();
		
		SendWRMod sendWriteWR = writeOp.getWrMod(0);
		sendWriteWR.setWr_id(opcount.getAndIncrement());
		sendWriteWR.getRdmaMod().setRemote_addr(remoteMr.getAddr() + remoteOffset);
		sendWriteWR.getRdmaMod().setRkey(remoteMr.getLkey());			
		
		SgeMod sgeSendWrite = writeOp.getWrMod(0).getSgeMod(0);
		sgeSendWrite.setAddr(bufferAddress + buffer.position());
		sgeSendWrite.setLength(buffer.remaining());
		sgeSendWrite.setLkey(localMr.getLkey());

		SendWRMod sendReadWR = writeOp.getWrMod(1);
		sendReadWR.setWr_id(opcount.getAndIncrement());
		sendReadWR.getRdmaMod().setRemote_addr(remoteMr.getAddr() + remoteOffset);
		sendReadWR.getRdmaMod().setRkey(remoteMr.getLkey());			
		
		SgeMod sgeSendRead = writeOp.getWrMod(1).getSgeMod(0);
		sgeSendRead.setAddr(bufferAddress + buffer.position());
		sgeSendRead.setLkey(localMr.getLkey());
		
		sendQueueAvailable.acquire();
		sendQueueAvailable.acquire();
		
		if (writeOp.getWrMod(0).getRdmaMod().getRkey() == 0){
			throw new IOException("stag is zero, can't be");
		}
		if (writeOp.getWrMod(1).getRdmaMod().getRkey() == 0){
			throw new IOException("stag is zero, can't be");
		}
		
		RdmaDataActiveFuture future = new RdmaDataActiveFuture(sendReadWR.getWr_id(), sgeSendWrite.getLength(), true);
		
		futureMap.put(future.getWrid(), future);
		if (!writeOp.execute().success()){
			throw new IOException("error in posting write");
		}
		
		writeOps.add(writeOp);
		
		return future;
	}

	@Override
	public Future<DataResult> read(ByteBuffer buffer, ByteBuffer region, BlockInfo remoteMr, long remoteOffset) throws IOException, InterruptedException {
		if (buffer.remaining() > CrailConstants.BLOCK_SIZE){
			throw new IOException("read size too large");
		}	
		if (buffer.remaining() <= 0){
			throw new IOException("read size too small, len " + buffer.remaining());
		}
		if (remoteOffset < 0){
			throw new IOException("remote offset too small " + remoteOffset);
		}
		if (remoteMr.getAddr() == 0){
			throw new IOException("remote addr is 0 " + remoteMr.getAddr());
		}		
		if (remoteMr.getLkey() == 0){
			throw new IOException("remote key is 0 " + remoteMr.getLkey());
		}	
		
		if (deviceCache == null){
			deviceCache = mrCache.getDeviceCache(this.getPd());
		}
		IbvMr localMr = deviceCache.get(region);
		if (localMr == null){
			localMr = this.registerMemory(region).execute().free().getMr();
			deviceCache.put(localMr);
		}
		long bufferAddress = MemoryUtils.getAddress(buffer);
				
		SVCPostSend readOp = readOps.take();
		
		SendWRMod sendWR = readOp.getWrMod(0);
		sendWR.setWr_id(opcount.getAndIncrement());
		SgeMod sgeSend = sendWR.getSgeMod(0);
		sgeSend.setAddr(bufferAddress + buffer.position());
		sgeSend.setLength(buffer.remaining());
		sgeSend.setLkey(localMr.getLkey());
		
		sendWR.getRdmaMod().setRemote_addr(remoteMr.getAddr() + remoteOffset);
		sendWR.getRdmaMod().setRkey(remoteMr.getLkey());
		
		sendQueueAvailable.acquire();
		
		if (readOp.getWrMod(0).getRdmaMod().getRkey() == 0){
			throw new IOException("stag is zero, can't be");
		}
		
		RdmaDataActiveFuture future = new RdmaDataActiveFuture(sendWR.getWr_id(), sgeSend.getLength(), false);
		
		futureMap.put(future.getWrid(), future);
		if (!readOp.execute().success()){
			throw new IOException("error in posting read");
		}
		
		readOps.add(readOp);
		
		return future;		
	}
	
	@Override
	public void dispatchCqEvent(IbvWC wc) throws IOException {
		if (wc.getStatus() == 0){
			RdmaDataActiveFuture future = futureMap.remove(wc.getWr_id());
			if (future != null){
				future.signal();
				if (future.isWrite()){
					sendQueueAvailable.release(2);
				} else {
					sendQueueAvailable.release();
				}
			} else {
				throw new IOException("cannot find future object for wrid " + wc.getWr_id() + ", status " + wc.getStatus() + ", opcount " + opcount + ", wc.qpnum " + wc.getQp_num() + ", this.qp.num " + this.qp.getQp_num() + ", connstate " + this.getConnState() + ", futureMap.size " + futureMap.size());
			}
		} else if (wc.getStatus() == 5){
		} else {
			throw new IOException("error in wc, status " + wc.getStatus());			
		}
	}

	@Override
	public void close() throws IOException, InterruptedException {
		super.close();
	}

	public int getFreeSlots() {
		return this.sendQueueAvailable.availablePermits();
	}

	public String getAddress() throws IOException {
		return super.getDstAddr().toString();
	}

	public RdmaCmId getContext() {
		return super.getIdPriv();
	}

	@Override
	public boolean isLocal() {
		return false;
	}
}
