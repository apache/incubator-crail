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

package com.ibm.crail;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import com.ibm.crail.CrailStatistics.StatisticsProvider;

public class CrailMultiStream extends InputStream {
//	private static final Logger LOG = CrailUtils.getLogger();
	
	private CrailFS fs;
	private Iterator<String> paths;
	private int outstanding;
	
	//state
	private LinkedBlockingQueue<SubStream> streams;
	private LinkedBlockingQueue<SubStream> runningStreams;
	private LinkedBlockingQueue<SubStream> tmpStreams;
	private long triggeredPosition;
	private long consumedPosition;
	private byte[] tmpByteBuf;
	private boolean isClosed;
	private int filesProcessed;
	private MultiStreamStatistics statistics;
	
	public CrailMultiStream(CrailFS fs, Iterator<String> paths, int outstanding, int files) throws Exception{
		this.fs = fs;
		this.paths = paths;
		this.outstanding = Math.min(outstanding, files);
		
		this.streams = new LinkedBlockingQueue<SubStream>();
		this.runningStreams = new LinkedBlockingQueue<SubStream>();
		this.tmpStreams = new LinkedBlockingQueue<SubStream>();
		this.triggeredPosition = 0;
		this.consumedPosition = 0;
		this.tmpByteBuf = new byte[1];
		this.isClosed = false;
		this.filesProcessed = 0;
		this.statistics = new MultiStreamStatistics();
		
		for (int i = 0; i < this.outstanding; i++){
			SubStream substream = nextSubStream();
			if (substream != null){
				streams.add(substream);
			} else {
				break;
			}
		}
	}	
	
	public final synchronized int read() throws IOException {
		int ret = read(tmpByteBuf);
		return (ret <= 0) ? -1 : (tmpByteBuf[0] & 0xff);
	}
	
    public final synchronized int read(byte b[]) throws IOException {
        return read(b, 0, b.length);
    }	

	public final synchronized int read(byte[] buffer, int off, int len)
			throws IOException {
		try {
			if (buffer == null) {
				throw new NullPointerException();
			} else if (off < 0 || len < 0 || len > buffer.length - off) {
				throw new IndexOutOfBoundsException();
			} else if (len == 0) {
				return 0;
			} else if (isClosed){
				return -1;
			}
		
//			LOG.info("starting read, position " + consumedPosition);
			int bufferPosition = off;
			int bufferRemaining = len;
			int bufferLimit = off + len;
			long streamPosition = consumedPosition;
			long anticipatedPosition = consumedPosition + bufferRemaining;
			
			while(!streams.isEmpty()){
				SubStream substream = streams.peek();
				if (substream.current() < anticipatedPosition){
					substream = streams.poll();
					runningStreams.add(substream);
				} else {
					break;
				}
			}
			
			int sum = 0;
			while(sum < bufferRemaining && !runningStreams.isEmpty()){
				SubStream substream = runningStreams.poll();
				int available = substream.available();
//				LOG.info("checking file " +  substream.getPath() + ", available " + available);
				if (available > 0){
					int bufferOffset = (int) (substream.current() - streamPosition);
					int bufferReadPosition = bufferPosition + bufferOffset;
					available = Math.min(available, bufferLimit - bufferReadPosition); 
					int dataRead = substream.read(buffer, bufferReadPosition, available);
					sum += dataRead;
					if (substream.isEnd()){
//						LOG.info("closing substream at position " + substream.current() + ", path " + substream.getPath());
						substream.close();
						filesProcessed++;
						substream = nextSubStream();
						if (substream != null && substream.current() < anticipatedPosition){
							runningStreams.add(substream);
						} else if (substream != null) {
							streams.add(substream);
						}
					} else if (substream.current() >= anticipatedPosition){
//						long leftover = substream.end() - substream.current();
//						LOG.info("moving tmp substream at position " + substream.current() + ", path " + substream.getPath() + ", leftover " + leftover);
						tmpStreams.add(substream);
					} else {
						runningStreams.add(substream);
					}
				} else {
					runningStreams.add(substream);
				}
//				LOG.info("");
			}
			while(!tmpStreams.isEmpty()){
				SubStream substream = tmpStreams.poll();
				runningStreams.add(substream);
			}
			if (sum > 0){
				consumedPosition += sum;
			} 
			return sum > 0 ? sum : -1;
		} catch (Exception e) {
			throw new IOException(e);
		}
	}
	
	public final synchronized int read(ByteBuffer buffer)
			throws IOException {
		try {
			if (buffer == null) {
				throw new NullPointerException();
			} else if (buffer.remaining() == 0) {
				return 0;
			} else if (isClosed){
				return -1;
			}
			
//			LOG.info("starting read, position " + consumedPosition);
			int bufferPosition = buffer.position();
			int bufferRemaining = buffer.remaining();
			int bufferLimit = buffer.limit();
			long streamPosition = consumedPosition;
			long anticipatedPosition = consumedPosition + bufferRemaining;
			boolean blocking = false;
			
			while(!streams.isEmpty()){
				SubStream substream = streams.peek();
				if (substream.current() < anticipatedPosition){
					substream = streams.poll();
					runningStreams.add(substream);
				} else {
					break;
				}
			}
			
			int sum = 0;
			while(sum < bufferRemaining && !runningStreams.isEmpty()){
				SubStream substream = runningStreams.poll();
				int available = substream.available();
//				LOG.info("checking file " +  substream.getPath() + ", available " + available);
				if (available > 0){
					int bufferOffset = (int) (substream.current() - streamPosition);
					int bufferReadPosition = bufferPosition + bufferOffset;
					int tmpLimit = Math.min(bufferReadPosition + available, bufferLimit);					
					buffer.limit(tmpLimit);					
					buffer.position(bufferReadPosition);
					int dataRead = substream.read(buffer);
					sum += dataRead;
					if (substream.isEnd()){
//						LOG.info("closing substream at position " + substream.current() + ", path " + substream.getPath());
						substream.close();
						filesProcessed++;
						substream = nextSubStream();
						if (substream != null && substream.current() < anticipatedPosition){
							runningStreams.add(substream);
						} else if (substream != null) {
							streams.add(substream);
						}
					} else if (substream.current() >= anticipatedPosition){
//						long leftover = substream.end() - substream.current();
//						LOG.info("moving tmp substream at position " + substream.current() + ", path " + substream.getPath() + ", leftover " + leftover);
						tmpStreams.add(substream);
					} else {
						runningStreams.add(substream);
					}
				} else {
					blocking = true;
					runningStreams.add(substream);
				}
//				LOG.info("");
			}
			while(!tmpStreams.isEmpty()){
				SubStream substream = tmpStreams.poll();
				runningStreams.add(substream);
			}
			buffer.limit(bufferLimit);
			if (sum > 0){
				buffer.position(bufferPosition + sum);
				consumedPosition += sum;
			} else {
				buffer.position(bufferPosition);
			}
			
			statistics.incTotalOps();
			if (blocking){
				statistics.incBlockingOps();
			} else {
				statistics.incNonBlockingOps();
			}
			
			return sum > 0 ? sum : -1;
		} catch (Exception e) {
			e.printStackTrace();
			throw new IOException(e);
		}
	}
	
	@Override
	public final synchronized void close() throws IOException {
		if (isClosed) {
			return;
		}

		while (!tmpStreams.isEmpty()) {
			SubStream stream = tmpStreams.poll();
			stream.close();
		}
		while (!runningStreams.isEmpty()) {
			SubStream stream = runningStreams.poll();
			stream.close();
		}
		while (!streams.isEmpty()) {
			SubStream stream = streams.poll();
			stream.close();
		}
		while (paths.hasNext()) {
			paths.next();
		}

		this.fs.getStatistics().addProvider(statistics);
		this.isClosed = true;
	}

	public final synchronized int available() {
		return 0;
	}
	
	public void seek(long pos) throws IOException {
		throw new IOException("operation not implemented!");
	}

	public boolean isOpen() {
		return true;
	}
	
	public int getFilesProcessed(){
		return this.filesProcessed;
	}

	private SubStream nextSubStream() throws Exception {
		SubStream substream = null;
		while (paths.hasNext() && substream == null){
			String path = paths.next();
			CrailFile file = fs.lookup(path).get().asFile();
			if (file == null){
				throw new Exception("File not found, name " + path);
			}
			if (file.getCapacity() > 0){
				CrailBufferedInputStream stream = file.getBufferedInputStream(file.getCapacity());
//				LOG.info("starting new substream, triggeredPosition " + triggeredPosition + ", file " + file.getPath());
				substream = new SubStream(file, stream, triggeredPosition);
				triggeredPosition += file.getCapacity();
			}
		} 
		return substream;
	}

	private class SubStream {
		private CrailBufferedInputStream stream;
		private long current;
		private long end;
		
		public SubStream(CrailFile file, CrailBufferedInputStream stream, long start) {
			this.stream = stream;
			this.current = start;
			this.end = start + file.getCapacity();
		}

		public long current(){
			return current;
		}
		
		public long end(){
			return end;
		}		
		
		public boolean isEnd() {
			return current == end;
		}
	
		public int available() {
			return stream.available();
		}		
		
		public void close() throws IOException {
			stream.close();
		}

		public int read(ByteBuffer buffer) throws IOException {
			int ret = stream.read(buffer);
			if (ret <= 0){
				throw new IOException("Stream indicated available > 0, but reading returned " + ret);
			}
//			LOG.info("copying out.. multistream position " + current + ", ret " + ret + ", path " + file.getPath());
			current += ret;
			return ret;			
		}
		
		public int read(byte[] buffer, int off, int len) throws IOException {
			int ret = stream.read(buffer, off, len);
			if (ret <= 0){
				throw new IOException("Stream indicated available > 0, but reading returned " + ret);
			}			
			current += ret;
			return ret;		
		}		
	}
	
	public static class MultiStreamStatistics implements StatisticsProvider {
		private AtomicLong totalOps;
		private AtomicLong blockingOps;
		private AtomicLong nonBlockingOps;
		
		public MultiStreamStatistics(){
			this.totalOps = new AtomicLong(0);
			this.blockingOps = new AtomicLong(0);
			this.nonBlockingOps = new AtomicLong(0);
		}
		
		public void mergeStatistics(StatisticsProvider provider){
			if (provider instanceof MultiStreamStatistics){
				MultiStreamStatistics newProvider = (MultiStreamStatistics) provider;
				this.totalOps.addAndGet(newProvider.getTotalOps());
				this.blockingOps.addAndGet(newProvider.getBlockingOps());
				this.nonBlockingOps.addAndGet(newProvider.getNonBlockingOps());
			}
		}
		
		@Override
		public String providerName() {
			return "MultiStream";
		}

		@Override
		public String printStatistics() {
			return "totalOps " + getTotalOps() + ", blockingOps " + getBlockingOps() + ", nonBlockingOps " + getNonBlockingOps();
		}

		@Override
		public void resetStatistics() {
			this.totalOps.set(0);
			this.blockingOps.set(0);
			this.nonBlockingOps.set(0);
		}
		
		public void incTotalOps(){
			this.totalOps.incrementAndGet();
		}
		
		public void incBlockingOps(){
			this.blockingOps.incrementAndGet();
		}
		
		public void incNonBlockingOps(){
			this.nonBlockingOps.incrementAndGet();
		}
		
		public long getTotalOps(){
			return totalOps.get();
		}
		
		public long getBlockingOps(){
			return blockingOps.get();
		}
		
		public long getNonBlockingOps(){
			return nonBlockingOps.get();
		}
	}
}
