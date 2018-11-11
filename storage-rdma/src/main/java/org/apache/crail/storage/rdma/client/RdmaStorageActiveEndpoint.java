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

import java.io.IOException;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;

import org.apache.crail.CrailBuffer;
import org.apache.crail.conf.CrailConstants;
import org.apache.crail.metadata.BlockInfo;
import org.apache.crail.storage.StorageEndpoint;
import org.apache.crail.storage.StorageFuture;
import org.apache.crail.storage.rdma.MrCache;
import org.apache.crail.storage.rdma.RdmaConstants;
import org.apache.crail.storage.rdma.MrCache.DeviceMrCache;
import org.apache.crail.utils.AtomicIntegerModulo;

import com.ibm.disni.verbs.*;
import com.ibm.disni.verbs.SVCPostSend.SendWRMod;
import com.ibm.disni.verbs.SVCPostSend.SgeMod;
import com.ibm.disni.*;

public class RdmaStorageActiveEndpoint extends RdmaActiveEndpoint implements StorageEndpoint {
	private LinkedBlockingQueue<SVCPostSend> writeOps;
	private LinkedBlockingQueue<SVCPostSend> readOps;
	private AtomicIntegerModulo opcount;
	private Semaphore sendQueueAvailable;
	private ConcurrentHashMap<Long, RdmaActiveFuture> futureMap;
	private MrCache mrCache;
	private DeviceMrCache deviceCache;
	
	public RdmaStorageActiveEndpoint(RdmaStorageActiveGroup group, RdmaCmId id, boolean serverSide) throws IOException {
		super(group, id, serverSide);
		writeOps = new LinkedBlockingQueue<SVCPostSend>();
		readOps = new LinkedBlockingQueue<SVCPostSend>();
		this.opcount = new AtomicIntegerModulo();
		this.futureMap = new ConcurrentHashMap<Long, RdmaActiveFuture>();
		this.sendQueueAvailable = new Semaphore(RdmaConstants.STORAGE_RDMA_QUEUESIZE);
		this.mrCache = group.getMrCache();
		this.deviceCache = null;
	}

	@Override
	protected synchronized void init() throws IOException {
		super.init();
		
		for (int i = 0; i < RdmaConstants.STORAGE_RDMA_QUEUESIZE; i++){
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
	public StorageFuture write(CrailBuffer buffer, BlockInfo remoteMr, long remoteOffset) throws IOException, InterruptedException {
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
		IbvMr localMr = deviceCache.get(buffer.getRegion());
		if (localMr == null){
			localMr = this.registerMemory(buffer.getRegion().getByteBuffer()).execute().free().getMr();
			deviceCache.put(localMr);
		}
		long bufferAddress = buffer.address();
		
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
		
		RdmaActiveFuture future = new RdmaActiveFuture(sendReadWR.getWr_id(), sgeSendWrite.getLength(), true);
		
		futureMap.put(future.getWrid(), future);
		writeOp.execute();
		writeOps.add(writeOp);
		
		return future;
	}

	@Override
	public StorageFuture read(CrailBuffer buffer, BlockInfo remoteMr, long remoteOffset) throws IOException, InterruptedException {
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
		IbvMr localMr = deviceCache.get(buffer.getRegion());
		if (localMr == null){
			localMr = this.registerMemory(buffer.getRegion().getByteBuffer()).execute().free().getMr();
			deviceCache.put(localMr);
		}
		long bufferAddress = buffer.address();
				
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
		
		RdmaActiveFuture future = new RdmaActiveFuture(sendWR.getWr_id(), sgeSend.getLength(), false);
		
		futureMap.put(future.getWrid(), future);
		readOp.execute();
		readOps.add(readOp);
		
		return future;		
	}
	
	@Override
	public void dispatchCqEvent(IbvWC wc) throws IOException {
		if (wc.getStatus() == 0){
			RdmaActiveFuture future = futureMap.remove(wc.getWr_id());
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
