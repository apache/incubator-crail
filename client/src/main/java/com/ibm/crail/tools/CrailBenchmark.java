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

package com.ibm.crail.tools;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import com.ibm.crail.CrailBufferedInputStream;
import com.ibm.crail.CrailBufferedOutputStream;
import com.ibm.crail.CrailFile;
import com.ibm.crail.CrailFS;
import com.ibm.crail.CrailInputStream;
import com.ibm.crail.CrailResult;
import com.ibm.crail.conf.CrailConfiguration;
import com.ibm.crail.conf.CrailConstants;
import com.ibm.crail.utils.GetOpt;

public class CrailBenchmark {
	private int warmup;
	
	public CrailBenchmark(int warmup){
		this.warmup = warmup;
	}
	
	public static void usage() {
		System.out.println("Usage: ");
		System.out.println(
				"iobench -t <writeClusterHeap|writeClusterDirect|writeLocalHeap|writeLocalDirect|writeAsyncCluster|writeAsyncLocal|"
				+ "readSequentialHeap|readSequentialDirect|readRandomHeap|readRandomDirect|readAsync|readMultiStream|"
				+ "enumerateDir|keyGet|createFile|getFile"
				+ "-f <filename> -s <size> -k <iterations> -b batch");
		System.exit(1);
	}

	void writeSequential(String filename, int size, int loop, boolean affinity, boolean direct) throws Exception {
		System.out.println("writeSequential, filename " + filename  + ", size " + size + ", loop " + loop + ", affinity " + affinity + ", direct " + direct);
		CrailConfiguration conf = new CrailConfiguration();
		CrailFS fs = CrailFS.newInstance(conf);
		
		int hosthash = 0;
		if (affinity){
			hosthash = fs.getHostHash();
		}
		
		ByteBuffer buf = null;
		if (direct){
			buf = ByteBuffer.allocateDirect(size);
		} else {
			buf = ByteBuffer.allocate(size);
		}
		buf.clear();	
		
		//warmup
		ConcurrentLinkedQueue<ByteBuffer> warmupQueue = new ConcurrentLinkedQueue<ByteBuffer>();
		warmupQueue.add(buf);
		warmUp(fs, filename, warmup, warmupQueue);
		
		//benchmark
		System.out.println("starting benchmark...");
		fs.resetStatistics();
		long _loop = (long) loop;
		long _bufsize = (long) CrailConstants.BUFFER_SIZE;
		long _capacity = _loop*_bufsize;
		double sumbytes = 0;
		double ops = 0;
		CrailFile file = fs.createFile(filename, 0, hosthash).get().syncDir();
		CrailBufferedOutputStream outstream = file.getBufferedOutputStream(_capacity);			
		long start = System.currentTimeMillis();
		while (ops < loop) {
			buf.clear();
			outstream.write(buf);
			sumbytes = sumbytes + buf.capacity();
			ops = ops + 1.0;
		}
		outstream.flush();
		long end = System.currentTimeMillis();
		double executionTime = ((double) (end - start)) / 1000.0;
		double throughput = 0.0;
		double latency = 0.0;
		double sumbits = sumbytes * 8.0;
		if (executionTime > 0) {
			throughput = sumbits / executionTime / 1000.0 / 1000.0;
			latency = 1000000.0 * executionTime / ops;
		}
		outstream.close();	
		
		System.out.println("execution time " + executionTime);
		System.out.println("ops " + ops);
		System.out.println("sumbytes " + sumbytes);
		System.out.println("throughput " + throughput);
		System.out.println("latency " + latency);
		
		fs.printStatistics("close");
		fs.close();		
	}
	
	void writeSequentialAsync(String filename, int size, int loop, int batch, boolean affinity, boolean direct) throws Exception {
		System.out.println("writeSequentialAsync, filename " + filename  + ", size " + size + ", loop " + loop + ", batch " + batch + ", affinity " + affinity + ", direct " + direct);
		CrailConfiguration conf = new CrailConfiguration();
		CrailFS fs = CrailFS.newInstance(conf);
		
		int hosthash = 0;
		if (affinity){
			hosthash = fs.getHostHash();
		}
		
		
		ConcurrentLinkedQueue<ByteBuffer> warmupQueue = new ConcurrentLinkedQueue<ByteBuffer>(); 
		LinkedBlockingQueue<ByteBuffer> bufferQueue = new LinkedBlockingQueue<ByteBuffer>();
		LinkedBlockingQueue<Future<CrailResult>> futureQueue = new LinkedBlockingQueue<Future<CrailResult>>();
		HashMap<Integer, ByteBuffer> futureMap = new HashMap<Integer, ByteBuffer>();
		for (int i = 0; i < batch; i++){
			ByteBuffer buf = null;
			if (direct){
				buf = ByteBuffer.allocateDirect(size);
			} else {
				buf = ByteBuffer.allocate(size);
			}
			bufferQueue.add(buf);
			warmupQueue.add(buf);
		}
		
		//warmup
		warmUp(fs, filename, warmup, warmupQueue);				
		
		//benchmark
		System.out.println("starting benchmark...");
		fs.resetStatistics();
		long _loop = (long) loop;
		long _bufsize = (long) CrailConstants.BUFFER_SIZE;
		long _capacity = _loop*_bufsize;
		double sumbytes = 0;
		double ops = 0;
		CrailFile file = fs.createFile(filename, hosthash, 0).get();
		CrailBufferedOutputStream outstream = file.getBufferedOutputStream(_capacity);	
		long start = System.currentTimeMillis();
		for (int i = 0; i < batch - 1 && ops < loop; i++){
			ByteBuffer buf = bufferQueue.poll();
			buf.clear();
			Future<CrailResult> future = outstream.writeAsync(buf);
			futureQueue.add(future);
			futureMap.put(future.hashCode(), buf);
			ops = ops + 1.0;
		}
		while (ops < loop) {
			ByteBuffer buf = bufferQueue.poll();
			buf.clear();
			Future<CrailResult> future = outstream.writeAsync(buf);
			futureQueue.add(future);
			futureMap.put(future.hashCode(), buf);
			
			future = futureQueue.poll();
			future.get();
			buf = futureMap.get(future.hashCode());
			bufferQueue.add(buf);
			
			sumbytes = sumbytes + buf.capacity();
			ops = ops + 1.0;
		}
		while (!futureQueue.isEmpty()){
			Future<CrailResult> future = futureQueue.poll();
			future.get();
			ByteBuffer buf = futureMap.get(future.hashCode());
			sumbytes = sumbytes + buf.capacity();
			ops = ops + 1.0;			
		}
		outstream.flush();
		long end = System.currentTimeMillis();
		double executionTime = ((double) (end - start)) / 1000.0;
		double throughput = 0.0;
		double latency = 0.0;
		double sumbits = sumbytes * 8.0;
		if (executionTime > 0) {
			throughput = sumbits / executionTime / 1000.0 / 1000.0;
			latency = 1000000.0 * executionTime / ops;
		}
		outstream.close();	
		
		System.out.println("execution time " + executionTime);
		System.out.println("ops " + ops);
		System.out.println("sumbytes " + sumbytes);
		System.out.println("throughput " + throughput);
		System.out.println("latency " + latency);
		
		fs.printStatistics("close");
		fs.close();		
	}

	void readSequential(String filename, int size, int loop, boolean direct) throws Exception {
		System.out.println("readSequential, filename " + filename  + ", size " + size + ", loop " + loop + ", direct " + direct);
		CrailConfiguration conf = new CrailConfiguration();
		CrailFS fs = CrailFS.newInstance(conf);

		ByteBuffer buf = null;
		if (direct){
			buf = ByteBuffer.allocateDirect(size);
		} else {
			buf = ByteBuffer.allocate(size);
		}
		buf.clear();	
		
		//warmup
		ConcurrentLinkedQueue<ByteBuffer> warmupQueue = new ConcurrentLinkedQueue<ByteBuffer>();
		warmupQueue.add(buf);
		warmUp(fs, filename, warmup, warmupQueue);
		
		CrailFile file = fs.lookupFile(filename, false).get();
		CrailBufferedInputStream instream = file.getBufferedInputStream(file.getCapacity());
		
		//benchmark
		System.out.println("starting benchmark...");
		fs.resetStatistics();
		double sumbytes = 0;
		double ops = 0;
		long start = System.currentTimeMillis();
		while (ops < loop) {
			buf.clear();
			double ret = (double) instream.read(buf);
			if (ret > 0) {
				sumbytes = sumbytes + ret;
				ops = ops + 1.0;
			} else {
				ops = ops + 1.0;
				if (instream.getPos() == 0){
					break;
				} else {
					instream.seek(0);
				}
			}
		}
		long end = System.currentTimeMillis();
		double executionTime = ((double) (end - start)) / 1000.0;
		double throughput = 0.0;
		double latency = 0.0;
		double sumbits = sumbytes * 8.0;
		if (executionTime > 0) {
			throughput = sumbits / executionTime / 1000.0 / 1000.0;
			latency = 1000000.0 * executionTime / ops;
		}
		instream.close();	
		
		System.out.println("execution time " + executionTime);
		System.out.println("ops " + ops);
		System.out.println("sumbytes " + sumbytes);
		System.out.println("throughput " + throughput);
		System.out.println("latency " + latency);
		
		fs.printStatistics("close");
		fs.close();
	}
	
	void readRandom(String filename, int size, int loop, boolean direct) throws Exception{
		System.out.println("readRandom, filename " + filename  + ", size " + size + ", loop " + loop + ", direct " + direct);
		CrailConfiguration conf = new CrailConfiguration();
		CrailFS fs = CrailFS.newInstance(conf);

		ByteBuffer buf = null;
		if (direct){
			buf = ByteBuffer.allocateDirect(size);
		} else {
			buf = ByteBuffer.allocate(size);
		}		
		
		//warmup
		ConcurrentLinkedQueue<ByteBuffer> warmupQueue = new ConcurrentLinkedQueue<ByteBuffer>();
		warmupQueue.add(buf);
		warmUp(fs, filename, warmup, warmupQueue);		
		
		//benchmark
		System.out.println("starting benchmark...");
		fs.resetStatistics();
		CrailFile file = fs.lookupFile(filename, false).get();
		CrailBufferedInputStream instream = file.getBufferedInputStream(file.getCapacity());				
		double sumbytes = 0;
		double ops = 0;
        long _range = file.getCapacity() - ((long)buf.capacity());
        double range = (double) _range;
		Random random = new Random();
		
		long start = System.currentTimeMillis();
		while (ops < loop) {
			buf.clear();
            double _offset = range*random.nextDouble();
            long offset = (long) _offset;
			instream.seek(offset);
			double ret = (double) instream.read(buf);
			if (ret > 0) {
				sumbytes = sumbytes + ret;
				ops = ops + 1.0;
			} else {
				break;
			}
		}
		long end = System.currentTimeMillis();
		double executionTime = ((double) (end - start)) / 1000.0;
		double throughput = 0.0;
		double latency = 0.0;
		double sumbits = sumbytes * 8.0;
		if (executionTime > 0) {
			throughput = sumbits / executionTime / 1000.0 / 1000.0;
			latency = 1000000.0 * executionTime / ops;
		}		
		instream.close();
		
		System.out.println("execution time " + executionTime);
		System.out.println("ops " + ops);
		System.out.println("sumbytes " + sumbytes);
		System.out.println("throughput " + throughput);
		System.out.println("latency " + latency);
		
		fs.printStatistics("close");
		fs.close();
	}	
	
	void readSequentialAsync(String filename, int size, int loop, int batch, boolean direct) throws Exception {
		System.out.println("readSequentialAsync, filename " + filename  + ", size " + size + ", loop " + loop + ", batch " + batch + ", direct " + direct);
		CrailConfiguration conf = new CrailConfiguration();
		CrailFS fs = CrailFS.newInstance(conf);
		
		ConcurrentLinkedQueue<ByteBuffer> warmupQueue = new ConcurrentLinkedQueue<ByteBuffer>();
		LinkedBlockingQueue<ByteBuffer> bufferQueue = new LinkedBlockingQueue<ByteBuffer>();
		LinkedBlockingQueue<Future<CrailResult>> futureQueue = new LinkedBlockingQueue<Future<CrailResult>>();
		HashMap<Integer, ByteBuffer> futureMap = new HashMap<Integer, ByteBuffer>();
		for (int i = 0; i < batch; i++){
			ByteBuffer buf = null;
			if (direct){
				buf = ByteBuffer.allocateDirect(size);
			} else {
				buf = ByteBuffer.allocate(size);
			}
			bufferQueue.add(buf);
			warmupQueue.add(buf);
		}
		double sumbytes = 0;
		double ops = 0;

		//warmup
		warmUp(fs, filename, warmup, warmupQueue);	
		
		//benchmark
		System.out.println("starting benchmark...");
		fs.resetStatistics();
		CrailFile file = fs.lookupFile(filename, false).get();
		CrailBufferedInputStream instream = file.getBufferedInputStream(file.getCapacity());			
		long start = System.currentTimeMillis();
		for (int i = 0; i < batch - 1 && ops < loop; i++){
			ByteBuffer buf = bufferQueue.poll();
			buf.clear();
			Future<CrailResult> future = instream.readAsync(buf);
			futureQueue.add(future);
			futureMap.put(future.hashCode(), buf);
			ops = ops + 1.0;
		}
		while (ops < loop) {
			ByteBuffer buf = bufferQueue.poll();
			buf.clear();
			Future<CrailResult> future = instream.readAsync(buf);
			futureQueue.add(future);
			futureMap.put(future.hashCode(), buf);
			
			future = futureQueue.poll();
			future.get();
			buf = futureMap.get(future.hashCode());
			bufferQueue.add(buf);
			
			sumbytes = sumbytes + buf.capacity();
			ops = ops + 1.0;
		}
		while (!futureQueue.isEmpty()){
			Future<CrailResult> future = futureQueue.poll();
			future.get();
			ByteBuffer buf = futureMap.get(future.hashCode());
			sumbytes = sumbytes + buf.capacity();
			ops = ops + 1.0;			
		}
		long end = System.currentTimeMillis();
		double executionTime = ((double) (end - start)) / 1000.0;
		double throughput = 0.0;
		double latency = 0.0;
		double sumbits = sumbytes * 8.0;
		if (executionTime > 0) {
			throughput = sumbits / executionTime / 1000.0 / 1000.0;
			latency = 1000000.0 * executionTime / ops;
		}
		instream.close();	
		
		System.out.println("execution time " + executionTime);
		System.out.println("ops " + ops);
		System.out.println("sumbytes " + sumbytes);
		System.out.println("throughput " + throughput);
		System.out.println("latency " + latency);
		
		fs.printStatistics("close");
		fs.close();		
	}

	void readMultiStream(String filename, int size, int loop, int batch) throws Exception {
		System.out.println("readMultiStream, filename " + filename  + ", size " + size + ", loop " + loop + ", batch " + batch);
		CrailConfiguration conf = new CrailConfiguration();
		CrailFS fs = CrailFS.newInstance(conf);
		
		//warmup
		ConcurrentLinkedQueue<ByteBuffer> warmupQueue = new ConcurrentLinkedQueue<ByteBuffer>();
		ConcurrentLinkedQueue<ByteBuffer> freeQueue = new ConcurrentLinkedQueue<ByteBuffer>();
		for (int i = 0; i < warmup; i++){
			ByteBuffer buf = fs.allocateBuffer();
			warmupQueue.add(buf);
			freeQueue.add(buf);
		}
		warmUp(fs, filename, warmup, warmupQueue);
		while(!freeQueue.isEmpty()){
			ByteBuffer buf = freeQueue.poll();
			fs.freeBuffer(buf);
		}
		
		//benchmark
		System.out.println("starting benchmark...");
		fs.resetStatistics();
		for (int i = 0; i < loop; i++){
			Iterator<String> files = fs.listEntries(filename);
			CrailInputStream multiStream = fs.getMultiStream(files, batch);
			ByteBuffer buf = fs.allocateBuffer();
			
			double sumbytes = 0;
			long _sumbytes = 0;
			double ops = 0;
			long start = System.currentTimeMillis();
			int ret = multiStream.read(buf);
			while(ret > 0){
				sumbytes = sumbytes + ret;
				long _ret = (long) ret;
				_sumbytes +=  _ret;				
				ops = ops + 1.0;
				buf.clear();
				ret = multiStream.read(buf);
			}
			long end = System.currentTimeMillis();
			multiStream.close();	
			
			double executionTime = ((double) (end - start)) / 1000.0;
			double throughput = 0.0;
			double latency = 0.0;
			double sumbits = sumbytes * 8.0;
			if (executionTime > 0) {
				throughput = sumbits / executionTime / 1000.0 / 1000.0;
				latency = 1000000.0 * executionTime / ops;
			}
			
			System.out.println("round " + i + ":");
			System.out.println("execution time " + executionTime);
			System.out.println("ops " + ops);
			System.out.println("throughput " + throughput);
			System.out.println("latency " + latency);
		}
	
		fs.printStatistics("close");
		fs.close();
	}

	void getFile(String filename, int loop) throws Exception, InterruptedException {
		System.out.println("getFile, filename " + filename  + ", loop " + loop);
		CrailConfiguration conf = new CrailConfiguration();
		CrailFS fs = CrailFS.newInstance(conf);	
		
		
		//warmup
		ConcurrentLinkedQueue<ByteBuffer> warmupQueue = new ConcurrentLinkedQueue<ByteBuffer>();
		ByteBuffer buf = fs.allocateBuffer();
		warmupQueue.add(buf);
		warmUp(fs, filename, warmup, warmupQueue);		
		fs.freeBuffer(buf);
		
		//benchmark
		System.out.println("starting benchmark...");
		fs.resetStatistics();
		double ops = 0;
		long start = System.currentTimeMillis();
		while (ops < loop) {
			ops = ops + 1.0;
			fs.lookupFile(filename, false).get();
		}
		long end = System.currentTimeMillis();
		double executionTime = ((double) (end - start)) / 1000.0;
		double latency = 0.0;
		if (executionTime > 0) {
			latency = 1000000.0 * executionTime / ops;
		}	
		System.out.println("execution time " + executionTime);
		System.out.println("ops " + ops);
		System.out.println("latency " + latency);
		
		fs.printStatistics("close");
		fs.close();
	}
	
	void getFileAsync(String filename, int loop, int batch) throws Exception, InterruptedException {
		System.out.println("getFileAsync, filename " + filename  + ", loop " + loop + ", batch " + batch);
		CrailConfiguration conf = new CrailConfiguration();
		CrailFS fs = CrailFS.newInstance(conf);	
		
		//warmup
		ConcurrentLinkedQueue<ByteBuffer> warmupQueue = new ConcurrentLinkedQueue<ByteBuffer>();
		ByteBuffer buf = fs.allocateBuffer();
		warmupQueue.add(buf);
		warmUp(fs, filename, warmup, warmupQueue);		
		fs.freeBuffer(buf);	
		
		//benchmark
		System.out.println("starting benchmark...");
		fs.resetStatistics();
		LinkedBlockingQueue<Future<CrailFile>> fileQueue = new LinkedBlockingQueue<Future<CrailFile>>();
		long start = System.currentTimeMillis();
		for (int i = 0; i < loop; i++){
			//single operation == loop
			for (int j = 0; j < batch; j++){
				Future<CrailFile> future = fs.lookupFile(filename, false);
				fileQueue.add(future);
			}
			for (int j = 0; j < batch; j++){
				Future<CrailFile> future = fileQueue.poll();
				future.get();
			}
		}
		long end = System.currentTimeMillis();
		double executionTime = ((double) (end - start));
		double latency = executionTime*1000.0 / ((double) batch);
		System.out.println("execution time [ms] " + executionTime);
		System.out.println("latency [us] " + latency);
		
		fs.printStatistics("close");
		fs.close();
	}
	
	void createFile(String filename, int loop) throws Exception, InterruptedException {
		System.out.println("createFile, filename " + filename  + ", loop " + loop);
		CrailConfiguration conf = new CrailConfiguration();
		CrailFS fs = CrailFS.newInstance(conf);	
		
		//warmup
		ConcurrentLinkedQueue<ByteBuffer> warmupQueue = new ConcurrentLinkedQueue<ByteBuffer>();
		ByteBuffer buf = fs.allocateBuffer();
		warmupQueue.add(buf);
		warmUp(fs, filename, warmup, warmupQueue);		
		fs.freeBuffer(buf);	
		
		//benchmark
		System.out.println("starting benchmark...");
		fs.resetStatistics();
		LinkedBlockingQueue<String> pathQueue = new LinkedBlockingQueue<String>();
		fs.createDir(filename).get().syncDir();
		int filecounter = 0;
		for (int i = 0; i < loop; i++){
			String name = "" + filecounter++;
			String f = filename + "/" + name;
			pathQueue.add(f);
		}		
		
		double ops = 0;
		long start = System.currentTimeMillis();
		while(!pathQueue.isEmpty()){
			String path = pathQueue.poll();
			fs.createFile(path, 0, 0).get().syncDir();
		}
		long end = System.currentTimeMillis();
		double executionTime = ((double) (end - start)) / 1000.0;
		double latency = 0.0;
		if (executionTime > 0) {
			latency = 1000000.0 * executionTime / ops;
		}	
		
		System.out.println("execution time " + executionTime);
		System.out.println("ops " + ops);
		System.out.println("latency " + latency);
		
		fs.printStatistics("close");
		fs.close();
	}	
	
	void createFileAsync(String filename, int loop, int batch) throws Exception, InterruptedException {
		System.out.println("createFileAsync, filename " + filename  + ", loop " + loop + ", batch " + batch);
		CrailConfiguration conf = new CrailConfiguration();
		CrailFS fs = CrailFS.newInstance(conf);	
		
		//warmup
		ConcurrentLinkedQueue<ByteBuffer> warmupQueue = new ConcurrentLinkedQueue<ByteBuffer>();
		ByteBuffer buf = fs.allocateBuffer();
		warmupQueue.add(buf);
		warmUp(fs, filename, warmup, warmupQueue);		
		fs.freeBuffer(buf);			
		
		//benchmark
		System.out.println("starting benchmark...");
		fs.resetStatistics();
		LinkedBlockingQueue<Future<CrailFile>> futureQueue = new LinkedBlockingQueue<Future<CrailFile>>();
		LinkedBlockingQueue<CrailFile> fileQueue = new LinkedBlockingQueue<CrailFile>();
		LinkedBlockingQueue<String> pathQueue = new LinkedBlockingQueue<String>();
		fs.createDir(filename).get().syncDir();	
		
		for (int i = 0; i < loop; i++){
			String name = "/" + i;
			String f = filename + name;
			pathQueue.add(f);
		}			
		
		long start = System.currentTimeMillis();
		for (int i = 0; i < loop; i += batch){
			//single operation == loop
			for (int j = 0; j < batch; j++) {
				String path = pathQueue.poll();
				Future<CrailFile> future = fs.createFile(path, 0, 0);
				futureQueue.add(future);
			}
			for (int j = 0; j < batch; j++){
				Future<CrailFile> future = futureQueue.poll();
				CrailFile file = future.get();
				fileQueue.add(file);					
			}
			for (int j = 0; j < batch; j++){
				CrailFile file = fileQueue.poll();
				file.syncDir();
			}
		}
		long end = System.currentTimeMillis();
		double executionTime = ((double) (end - start));
		double latency = executionTime*1000.0 / ((double) loop);
		System.out.println("execution time [ms] " + executionTime);
		System.out.println("latency [us] " + latency);

		fs.delete(filename, true).get().syncDir();
		
		
		fs.printStatistics("close");
		fs.close();
		
	}	
	
	void enumerateDir(String filename, int loop) throws Exception {
		System.out.println("reading enumarate dir, path " + filename);
		CrailConfiguration conf = new CrailConfiguration();
		CrailFS fs = CrailFS.newInstance(conf);
		
		//warmup
		ConcurrentLinkedQueue<ByteBuffer> warmupQueue = new ConcurrentLinkedQueue<ByteBuffer>();
		ByteBuffer buf = fs.allocateBuffer();
		warmupQueue.add(buf);
		warmUp(fs, filename, warmup, warmupQueue);		
		fs.freeBuffer(buf);			

		//benchmark
		System.out.println("starting benchmark...");
		fs.resetStatistics();
		long start = System.currentTimeMillis();
		for (int i = 0; i < loop; i++) {
			// single operation == loop
			Iterator<String> iter = fs.listEntries(filename);
			while (iter.hasNext()) {
				iter.next();
			}
		}
		long end = System.currentTimeMillis();
		double executionTime = ((double) (end - start));
		double latency = executionTime * 1000.0 / ((double) loop);
		System.out.println("execution time [ms] " + executionTime);
		System.out.println("latency [us] " + latency);

		fs.printStatistics("close");
		fs.close();
	}
	
	void keyGet(String filename, int size, int loop) throws Exception {
		System.out.println("keyGet, path " + filename + ", size " + size + ", loop " + loop);
		CrailConfiguration conf = new CrailConfiguration();
		CrailFS fs = CrailFS.newInstance(conf);
		
		ByteBuffer buf = ByteBuffer.allocateDirect(size);
		CrailFile file = fs.createFile(filename, 0, 0).get().syncDir();
		CrailBufferedOutputStream outStream = file.getBufferedOutputStream(0);
		outStream.write(buf);
		outStream.close();
		
		//benchmark
		System.out.println("starting benchmark...");
		fs.resetStatistics();
		long start = System.currentTimeMillis();
		for (int i = 0; i < loop; i++){
			CrailBufferedInputStream inputStream = fs.lookupFile(filename, false).get().getBufferedInputStream(0);
			buf.clear();
			inputStream.readAsync(buf).get();
			inputStream.close();
		}
		long end = System.currentTimeMillis();
		double executionTime = ((double) (end - start));
		double latency = executionTime * 1000.0 / ((double) loop);
		System.out.println("execution time [ms] " + executionTime);
		System.out.println("latency [us] " + latency);		
		
		fs.printStatistics("close");
		fs.close();
	}	
	
	private void warmUp(CrailFS fs, String filename, int operations, ConcurrentLinkedQueue<ByteBuffer> bufferList) throws Exception {
		String warmupFilename = filename + ".warmup";
		System.out.println("warmUp, warmupFile " + warmupFilename + ", operations " + operations);
		CrailFile warmupFile = fs.createFile(warmupFilename, 0, 0).get().syncDir();
		CrailBufferedOutputStream warmupStream = warmupFile.getBufferedOutputStream(0);
		for (int i = 0; i < operations; i++){
			ByteBuffer buf = bufferList.poll();
			buf.clear();
			warmupStream.write(buf);
			if (bufferList.isEmpty()){
				bufferList.add(buf);
			}
		}
		warmupStream.flush();
		warmupStream.close();
		fs.delete(warmupFilename, false).get().syncDir();			
	}
	
	public static void main(String[] args) throws Exception {
		String[] _args = args;
		GetOpt go = new GetOpt(_args, "t:f:s:k:b:w:");
		go.optErr = true;
		int ch = -1;
		
		if (args.length < 2){
			usage();
		}
		
		String type = "";
		String filename = "/tmp.dat";
		int size = CrailConstants.BUFFER_SIZE;
		int loop = 1;
		int batch = 1;
		int warmup = 32;
		
		while ((ch = go.getopt()) != GetOpt.optEOF) {
			if ((char) ch == 't') {
				type = go.optArgGet();
			} else if ((char) ch == 'f') {
				filename = go.optArgGet();
			} else if ((char) ch == 's') {
				size = Integer.parseInt(go.optArgGet());
			} else if ((char) ch == 'k') {
				loop = Integer.parseInt(go.optArgGet());
			} else if ((char) ch == 'b') {
				batch = Integer.parseInt(go.optArgGet());
			} else if ((char) ch == 'w') {
				warmup = Integer.parseInt(go.optArgGet());
			} else {
				System.exit(1); // undefined option
			}
		}		
		
		CrailBenchmark benchmark = new CrailBenchmark(warmup);
		if (type.equals("writeClusterHeap")){
			benchmark.writeSequential(filename, size, loop, false, false);
		} else if (type.equals("writeClusterDirect")){
			benchmark.writeSequential(filename, size, loop, false, true);
		} else if (type.equals("writeLocalHeap")){
			benchmark.writeSequential(filename, size, loop, true, false);
		} else if (type.equals("writeLocalDirect")){
			benchmark.writeSequential(filename, size, loop, true, true);
		} else if (type.equalsIgnoreCase("writeAsyncCluster")) {
			benchmark.writeSequentialAsync(filename, size, loop, batch, false, true);
		} else if (type.equalsIgnoreCase("writeAsyncLocal")) {
			benchmark.writeSequentialAsync(filename, size, loop, batch, true, true);
		} else if (type.equalsIgnoreCase("readSequentialDirect")){
			benchmark.readSequential(filename, size, loop, true);
		} else if (type.equals("readSequentialHeap")){
			benchmark.readSequential(filename, size, loop, false);
		} else if (type.equals("readRandomDirect")){
			benchmark.readRandom(filename, size, loop, true);
		} else if (type.equals("readRandomHeap")){
			benchmark.readRandom(filename, size, loop, false);
		} else if (type.equalsIgnoreCase("readAsync")) {
			benchmark.readSequentialAsync(filename, size, loop, batch, true);
		} else if (type.equalsIgnoreCase("readMultiStream")) {
			benchmark.readMultiStream(filename, size, loop, batch);
		} else if (type.equals("getFile")){
			benchmark.getFile(filename, loop);
		} else if (type.equals("getFileAsync")){
			benchmark.getFileAsync(filename, loop, batch);
		} else if (type.equals("createFile")){
			benchmark.createFile(filename, loop);
		} else if (type.equals("createFileAsync")){
			benchmark.createFileAsync(filename, loop, batch);
		} else if (type.equalsIgnoreCase("enumerateDir")) {
			benchmark.enumerateDir(filename, batch);
		} else if (type.equalsIgnoreCase("keyGet")) {
			benchmark.keyGet(filename, size, loop);
		} else {
			usage();
			System.exit(0);
		}		
	}

}

