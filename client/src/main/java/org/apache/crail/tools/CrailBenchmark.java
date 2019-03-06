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

package org.apache.crail.tools;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.crail.CrailBuffer;
import org.apache.crail.CrailBufferedInputStream;
import org.apache.crail.CrailBufferedOutputStream;
import org.apache.crail.CrailStore;
import org.apache.crail.CrailFile;
import org.apache.crail.CrailInputStream;
import org.apache.crail.CrailLocationClass;
import org.apache.crail.CrailNode;
import org.apache.crail.CrailNodeType;
import org.apache.crail.CrailOutputStream;
import org.apache.crail.CrailResult;
import org.apache.crail.CrailStorageClass;
import org.apache.crail.conf.CrailConfiguration;
import org.apache.crail.conf.CrailConstants;
import org.apache.crail.memory.OffHeapBuffer;
import org.apache.crail.utils.CrailUtils;
import org.apache.crail.utils.RingBuffer;

public class CrailBenchmark {
	private int warmup;
	private CrailConfiguration conf;
	private CrailStore fs;
	
	public CrailBenchmark(int warmup) throws Exception {
		this.warmup = warmup;
		this.conf = CrailConfiguration.createConfigurationFromFile();
		this.fs = null;
	}
	
	private void open() throws Exception{
		if (fs == null){
			this.fs = CrailStore.newInstance(conf);
		}
	}
	
	private void close() throws Exception{
		if (fs != null){
			fs.close();
			fs = null;
		}
	}
	
	void write(String filename, int size, int loop, int storageClass, int locationClass, boolean buffered, boolean skipDir) throws Exception {
		System.out.println("write, filename " + filename  + ", size " + size + ", loop " + loop + ", storageClass " + storageClass + ", locationClass " + locationClass + ", buffered " + buffered);
		
		CrailBuffer buf = null;
		if (size == CrailConstants.BUFFER_SIZE){
			buf = fs.allocateBuffer();
		} else if (size < CrailConstants.BUFFER_SIZE){
			CrailBuffer _buf = fs.allocateBuffer();
			_buf.clear().limit(size);
			buf = _buf.slice();
		} else {
			buf = OffHeapBuffer.wrap(ByteBuffer.allocateDirect(size));
		}
		
		//warmup
		ConcurrentLinkedQueue<CrailBuffer> bufferQueue = new ConcurrentLinkedQueue<CrailBuffer>();
		bufferQueue.add(buf);
		warmUp(filename, warmup, bufferQueue);
		
		//benchmark
		System.out.println("starting benchmark...");
		fs.getStatistics().reset();
		long _loop = (long) loop;
		long _bufsize = (long) CrailConstants.BUFFER_SIZE;
		long _capacity = _loop*_bufsize;
		double sumbytes = 0;
		double ops = 0;
		CrailFile file = fs.create(filename, CrailNodeType.DATAFILE, CrailStorageClass.get(storageClass), CrailLocationClass.get(locationClass), !skipDir).get().asFile();
		CrailBufferedOutputStream bufferedStream = buffered ? file.getBufferedOutputStream(_capacity) : null;	
		CrailOutputStream directStream = !buffered ? file.getDirectOutputStream(_capacity) : null;	
		long start = System.currentTimeMillis();
		while (ops < loop) {
			buf.clear();
			if (buffered){
				bufferedStream.write(buf.getByteBuffer());
			} else {
				directStream.write(buf).get();
			}
			sumbytes = sumbytes + buf.capacity();
			ops = ops + 1.0;				
		}
		if (buffered){
			bufferedStream.close();
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
		
		System.out.println("execution time " + executionTime);
		System.out.println("ops " + ops);
		System.out.println("sumbytes " + sumbytes);
		System.out.println("throughput " + throughput);
		System.out.println("latency " + latency);
		
		fs.getStatistics().print("close");
	}
	
	void writeAsync(String filename, int size, int loop, int batch, int storageClass, int locationClass, boolean skipDir) throws Exception {
		System.out.println("writeAsync, filename " + filename  + ", size " + size + ", loop " + loop + ", batch " + batch + ", storageClass " + storageClass + ", locationClass " + locationClass);
		
		ConcurrentLinkedQueue<CrailBuffer> bufferQueue = new ConcurrentLinkedQueue<CrailBuffer>();
		for (int i = 0; i < batch; i++){
			CrailBuffer buf = null;
			if (size == CrailConstants.BUFFER_SIZE){
				buf = fs.allocateBuffer();
			} else if (size < CrailConstants.BUFFER_SIZE){
				CrailBuffer _buf = fs.allocateBuffer();
				_buf.clear().limit(size);
				buf = _buf.slice();
			} else {
				buf = OffHeapBuffer.wrap(ByteBuffer.allocateDirect(size));
			}
			bufferQueue.add(buf);
		}
		
		//warmup
		warmUp(filename, warmup, bufferQueue);				
		
		//benchmark
		System.out.println("starting benchmark...");
		LinkedBlockingQueue<Future<CrailResult>> futureQueue = new LinkedBlockingQueue<Future<CrailResult>>();
		HashMap<Integer, CrailBuffer> futureMap = new HashMap<Integer, CrailBuffer>();
		fs.getStatistics().reset();
		long _loop = (long) loop;
		long _bufsize = (long) CrailConstants.BUFFER_SIZE;
		long _capacity = _loop*_bufsize;
		double sumbytes = 0;
		double ops = 0;
		CrailFile file = fs.create(filename, CrailNodeType.DATAFILE, CrailStorageClass.get(storageClass), CrailLocationClass.get(locationClass), !skipDir).get().asFile();
		CrailOutputStream directStream = file.getDirectOutputStream(_capacity);	
		long start = System.currentTimeMillis();
		for (int i = 0; i < batch - 1 && ops < loop; i++){
			CrailBuffer buf = bufferQueue.poll();
			buf.clear();
			Future<CrailResult> future = directStream.write(buf);
			futureQueue.add(future);
			futureMap.put(future.hashCode(), buf);
			ops = ops + 1.0;
		}
		while (ops < loop) {
			CrailBuffer buf = bufferQueue.poll();
			buf.clear();
			Future<CrailResult> future = directStream.write(buf);
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
			CrailBuffer buf = futureMap.get(future.hashCode());
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
		directStream.close();	
		
		System.out.println("execution time " + executionTime);
		System.out.println("ops " + ops);
		System.out.println("sumbytes " + sumbytes);
		System.out.println("throughput " + throughput);
		System.out.println("latency " + latency);
		
		fs.getStatistics().print("close");
	}

	void readSequential(String filename, int size, int loop, boolean buffered) throws Exception {
		System.out.println("readSequential, filename " + filename  + ", size " + size + ", loop " + loop + ", buffered " + buffered);

		CrailBuffer buf = null;
		if (size == CrailConstants.BUFFER_SIZE){
			buf = fs.allocateBuffer();
		} else if (size < CrailConstants.BUFFER_SIZE){
			CrailBuffer _buf = fs.allocateBuffer();
			_buf.clear().limit(size);
			buf = _buf.slice();
		} else {
			buf = OffHeapBuffer.wrap(ByteBuffer.allocateDirect(size));
		}
		
		//warmup
		ConcurrentLinkedQueue<CrailBuffer> bufferQueue = new ConcurrentLinkedQueue<CrailBuffer>();
		bufferQueue.add(buf);
		warmUp(filename, warmup, bufferQueue);
		
		CrailFile file = fs.lookup(filename).get().asFile();
		CrailBufferedInputStream bufferedStream = file.getBufferedInputStream(file.getCapacity());
		CrailInputStream directStream = file.getDirectInputStream(file.getCapacity());
		
		//benchmark
		System.out.println("starting benchmark...");
		fs.getStatistics().reset();
		double sumbytes = 0;
		double ops = 0;
		long start = System.currentTimeMillis();
		while (ops < loop) {
			if (buffered){
				buf.clear();
				double ret = (double) bufferedStream.read(buf.getByteBuffer());
				if (ret > 0) {
					sumbytes = sumbytes + ret;
					ops = ops + 1.0;
				} else {
					ops = ops + 1.0;
					if (bufferedStream.position() == 0){
						break;
					} else {
						bufferedStream.seek(0);
					}
				}				
			} else {
				buf.clear();
				double ret = (double) directStream.read(buf).get().getLen();
				if (ret > 0) {
					sumbytes = sumbytes + ret;
					ops = ops + 1.0;
				} else {
					ops = ops + 1.0;
					if (directStream.position() == 0){
						break;
					} else {
						directStream.seek(0);
					}
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
		bufferedStream.close();	
		directStream.close();
		
		System.out.println("execution time " + executionTime);
		System.out.println("ops " + ops);
		System.out.println("sumbytes " + sumbytes);
		System.out.println("throughput " + throughput);
		System.out.println("latency " + latency);
		
		fs.getStatistics().print("close");
	}
	
	void readRandom(String filename, int size, int loop, boolean buffered) throws Exception{
		System.out.println("readRandom, filename " + filename  + ", size " + size + ", loop " + loop + ", buffered " + buffered);

		CrailBuffer buf = null;
		if (size == CrailConstants.BUFFER_SIZE){
			buf = fs.allocateBuffer();
		} else if (size < CrailConstants.BUFFER_SIZE){
			CrailBuffer _buf = fs.allocateBuffer();
			_buf.clear().limit(size);
			buf = _buf.slice();
		} else {
			buf = OffHeapBuffer.wrap(ByteBuffer.allocateDirect(size));
		}
		
		//warmup
		ConcurrentLinkedQueue<CrailBuffer> bufferQueue = new ConcurrentLinkedQueue<CrailBuffer>();
		bufferQueue.add(buf);
		warmUp(filename, warmup, bufferQueue);		
		
		//benchmark
		System.out.println("starting benchmark...");
		fs.getStatistics().reset();
		CrailFile file = fs.lookup(filename).get().asFile();
		CrailBufferedInputStream bufferedStream = file.getBufferedInputStream(file.getCapacity());
		CrailInputStream directStream = file.getDirectInputStream(file.getCapacity());		
		
		double sumbytes = 0;
		double ops = 0;
        long _range = file.getCapacity() - ((long)buf.capacity());
        _range = _range / size;
        double range = (double) _range;
		Random random = new Random();
		
		long start = System.currentTimeMillis();
		while (ops < loop) {
			if (buffered){
				buf.clear();
				double _offset = range*random.nextDouble();
				long offset = (long) _offset*size;
				bufferedStream.seek(offset);
				double ret = (double) bufferedStream.read(buf.getByteBuffer());
				if (ret > 0) {
					sumbytes = sumbytes + ret;
					ops = ops + 1.0;
				} else {
					break;
				}

			} else {
				buf.clear();
				double _offset = range*random.nextDouble();
				long offset = (long) _offset*size;
				directStream.seek(offset);
				double ret = (double) directStream.read(buf).get().getLen();
				if (ret > 0) {
					sumbytes = sumbytes + ret;
					ops = ops + 1.0;
				} else {
					break;
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
		bufferedStream.close();
		directStream.close();
		
		System.out.println("execution time " + executionTime);
		System.out.println("ops " + ops);
		System.out.println("sumbytes " + sumbytes);
		System.out.println("throughput " + throughput);
		System.out.println("latency " + latency);
		
		fs.getStatistics().print("close");
	}	
	
	void readSequentialAsync(String filename, int size, int loop, int batch) throws Exception {
		System.out.println("readSequentialAsync, filename " + filename  + ", size " + size + ", loop " + loop + ", batch " + batch);
		
		ConcurrentLinkedQueue<CrailBuffer> bufferQueue = new ConcurrentLinkedQueue<CrailBuffer>();
		for (int i = 0; i < batch; i++){
			CrailBuffer buf = null;
			if (size == CrailConstants.BUFFER_SIZE){
				buf = fs.allocateBuffer();
			} else if (size < CrailConstants.BUFFER_SIZE){
				CrailBuffer _buf = fs.allocateBuffer();
				_buf.clear().limit(size);
				buf = _buf.slice();
			} else {
				buf = OffHeapBuffer.wrap(ByteBuffer.allocateDirect(size));
			}
			bufferQueue.add(buf);
		}

		//warmup
		warmUp(filename, warmup, bufferQueue);	
		
		//benchmark
		System.out.println("starting benchmark...");
		double sumbytes = 0;
		double ops = 0;
		fs.getStatistics().reset();
		CrailFile file = fs.lookup(filename).get().asFile();
		CrailInputStream directStream = file.getDirectInputStream(file.getCapacity());			
		HashMap<Integer, CrailBuffer> futureMap = new HashMap<Integer, CrailBuffer>();
		LinkedBlockingQueue<Future<CrailResult>> futureQueue = new LinkedBlockingQueue<Future<CrailResult>>();
		long start = System.currentTimeMillis();
		for (int i = 0; i < batch - 1 && ops < loop; i++){
			CrailBuffer buf = bufferQueue.poll();
			buf.clear();
			Future<CrailResult> future = directStream.read(buf);
			futureQueue.add(future);
			futureMap.put(future.hashCode(), buf);
			ops = ops + 1.0;
		}
		while (ops < loop) {
			CrailBuffer buf = bufferQueue.poll();
			buf.clear();
			Future<CrailResult> future = directStream.read(buf);
			futureQueue.add(future);
			futureMap.put(future.hashCode(), buf);
			
			future = futureQueue.poll();
			CrailResult result = future.get();
			buf = futureMap.get(future.hashCode());
			bufferQueue.add(buf);
			
			sumbytes = sumbytes + result.getLen();
			ops = ops + 1.0;
		}
		while (!futureQueue.isEmpty()){
			Future<CrailResult> future = futureQueue.poll();
			CrailResult result = future.get();
			futureMap.get(future.hashCode());
			sumbytes = sumbytes + result.getLen();
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
		directStream.close();	
		
		System.out.println("execution time " + executionTime);
		System.out.println("ops " + ops);
		System.out.println("sumbytes " + sumbytes);
		System.out.println("throughput " + throughput);
		System.out.println("latency " + latency);
		
		fs.getStatistics().print("close");
	}

	void readMultiStream(String filename, int size, int loop, int batch) throws Exception {
		System.out.println("readMultiStream, filename " + filename  + ", size " + size + ", loop " + loop + ", batch " + batch);
		
		//warmup
		ConcurrentLinkedQueue<CrailBuffer> bufferQueue = new ConcurrentLinkedQueue<CrailBuffer>();
		for (int i = 0; i < warmup; i++){
			CrailBuffer buf = fs.allocateBuffer().limit(size).slice();
			bufferQueue.add(buf);
		}
		warmUp(filename, warmup, bufferQueue);
		while(!bufferQueue.isEmpty()){
			CrailBuffer buf = bufferQueue.poll();
			fs.freeBuffer(buf);
		}
		
		//benchmark
		System.out.println("starting benchmark...");
		fs.getStatistics().reset();
		CrailBuffer _buf = null;
		if (size == CrailConstants.BUFFER_SIZE){
			_buf = fs.allocateBuffer();
		} else if (size < CrailConstants.BUFFER_SIZE){
			CrailBuffer __buf = fs.allocateBuffer();
			__buf.clear().limit(size);
			_buf = __buf.slice();
		} else {
			_buf = OffHeapBuffer.wrap(ByteBuffer.allocateDirect(size));
		}		
		ByteBuffer buf = _buf.getByteBuffer();
		for (int i = 0; i < loop; i++){
			CrailBufferedInputStream multiStream = fs.lookup(filename).get().asMultiFile().getMultiStream(batch);
			double sumbytes = 0;
			long _sumbytes = 0;
			double ops = 0;
			buf.clear();
			long start = System.currentTimeMillis();
			int ret = multiStream.read(buf);
			while(ret >= 0){
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
			System.out.println("bytes read " + _sumbytes);
			System.out.println("execution time " + executionTime);
			System.out.println("ops " + ops);
			System.out.println("throughput " + throughput);
			System.out.println("latency " + latency);
		}
	
		fs.getStatistics().print("close");
	}
	
	void createFile(String filename, int loop) throws Exception, InterruptedException {
		System.out.println("createFile, filename " + filename  + ", loop " + loop);
		
		//warmup
		ConcurrentLinkedQueue<CrailBuffer> bufferQueue = new ConcurrentLinkedQueue<CrailBuffer>();
		CrailBuffer buf = fs.allocateBuffer();
		bufferQueue.add(buf);
		warmUp(filename, warmup, bufferQueue);		
		fs.freeBuffer(buf);	
		
		//benchmark
		System.out.println("starting benchmark...");
		fs.getStatistics().reset();
		LinkedBlockingQueue<String> pathQueue = new LinkedBlockingQueue<String>();
		fs.create(filename, CrailNodeType.DIRECTORY, CrailStorageClass.DEFAULT, CrailLocationClass.DEFAULT, true).get().syncDir();
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
			fs.create(path, CrailNodeType.DATAFILE, CrailStorageClass.DEFAULT, CrailLocationClass.DEFAULT, true).get().syncDir();
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
		
		fs.getStatistics().print("close");
	}

	void createFileAsync(String filename, int loop, int batch) throws Exception, InterruptedException {
		System.out.println("createFileAsync, filename " + filename  + ", loop " + loop + ", batch " + batch);
		
		//warmup
		ConcurrentLinkedQueue<CrailBuffer> bufferQueue = new ConcurrentLinkedQueue<CrailBuffer>();
		CrailBuffer buf = fs.allocateBuffer();
		bufferQueue.add(buf);
		warmUp(filename, warmup, bufferQueue);		
		fs.freeBuffer(buf);			
		
		//benchmark
		System.out.println("starting benchmark...");
		fs.getStatistics().reset();
		LinkedBlockingQueue<Future<CrailNode>> futureQueue = new LinkedBlockingQueue<Future<CrailNode>>();
		LinkedBlockingQueue<CrailFile> fileQueue = new LinkedBlockingQueue<CrailFile>();
		LinkedBlockingQueue<String> pathQueue = new LinkedBlockingQueue<String>();
		fs.create(filename, CrailNodeType.DIRECTORY, CrailStorageClass.DEFAULT, CrailLocationClass.DEFAULT, true).get().syncDir();	
		
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
				Future<CrailNode> future = fs.create(path, CrailNodeType.DATAFILE, CrailStorageClass.DEFAULT, CrailLocationClass.DEFAULT, true);
				futureQueue.add(future);
			}
			for (int j = 0; j < batch; j++){
				Future<CrailNode> future = futureQueue.poll();
				CrailFile file = future.get().asFile();
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
		
		fs.getStatistics().print("close");
		
	}

	void createMultiFile(String filename, int storageClass) throws Exception, InterruptedException {
		System.out.println("createMultiFile, filename " + filename);
		fs.create(filename, CrailNodeType.MULTIFILE, CrailStorageClass.get(storageClass), CrailLocationClass.DEFAULT, true).get().syncDir();
	}

	void getKey(String filename, int size, int loop) throws Exception {
		System.out.println("getKey, path " + filename + ", size " + size + ", loop " + loop);
		
		CrailBuffer buf = fs.allocateBuffer().clear().limit(size).slice();
		CrailFile file = fs.create(filename, CrailNodeType.DATAFILE, CrailStorageClass.DEFAULT, CrailLocationClass.DEFAULT, true).get().asFile();
		file.syncDir();
		CrailOutputStream directOutputStream = file.getDirectOutputStream(0);
		directOutputStream.write(buf).get();
		directOutputStream.close();
		
		//benchmark
		System.out.println("starting benchmark...");
		fs.getStatistics().reset();
		long start = System.currentTimeMillis();
		for (int i = 0; i < loop; i++){
			CrailInputStream directInputStream = fs.lookup(filename).get().asFile().getDirectInputStream(0);
			buf.clear();
			directInputStream.read(buf).get();
			directInputStream.close();
		}
		long end = System.currentTimeMillis();
		double executionTime = ((double) (end - start));
		double latency = executionTime * 1000.0 / ((double) loop);
		System.out.println("execution time [ms] " + executionTime);
		System.out.println("latency [us] " + latency);		
		
		fs.getStatistics().print("close");
	}

	void getFile(String filename, int loop) throws Exception, InterruptedException {
		System.out.println("getFile, filename " + filename  + ", loop " + loop);
		
		//warmup
		ConcurrentLinkedQueue<CrailBuffer> bufferQueue = new ConcurrentLinkedQueue<CrailBuffer>();
		CrailBuffer buf = fs.allocateBuffer();
		bufferQueue.add(buf);
		warmUp(filename, warmup, bufferQueue);		
		fs.freeBuffer(buf);
		
		//benchmark
		System.out.println("starting benchmark...");
		fs.getStatistics().reset();
		double ops = 0;
		long start = System.currentTimeMillis();
		while (ops < loop) {
			ops = ops + 1.0;
			fs.lookup(filename).get().asFile();
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
		
		fs.getStatistics().print("close");
		fs.close();
	}
	
	void getFileAsync(String filename, int loop, int batch) throws Exception, InterruptedException {
		System.out.println("getFileAsync, filename " + filename  + ", loop " + loop + ", batch " + batch);
		
		//warmup
		ConcurrentLinkedQueue<CrailBuffer> bufferQueue = new ConcurrentLinkedQueue<CrailBuffer>();
		CrailBuffer buf = fs.allocateBuffer();
		bufferQueue.add(buf);
		warmUp(filename, warmup, bufferQueue);		
		fs.freeBuffer(buf);	
		
		//benchmark
		System.out.println("starting benchmark...");
		fs.getStatistics().reset();
		LinkedBlockingQueue<Future<CrailNode>> fileQueue = new LinkedBlockingQueue<Future<CrailNode>>();
		long start = System.currentTimeMillis();
		for (int i = 0; i < loop; i++){
			//single operation == loop
			for (int j = 0; j < batch; j++){
				Future<CrailNode> future = fs.lookup(filename);
				fileQueue.add(future);
			}
			for (int j = 0; j < batch; j++){
				Future<CrailNode> future = fileQueue.poll();
				future.get();
			}
		}
		long end = System.currentTimeMillis();
		double executionTime = ((double) (end - start));
		double latency = executionTime*1000.0 / ((double) batch);
		System.out.println("execution time [ms] " + executionTime);
		System.out.println("latency [us] " + latency);
		
		fs.getStatistics().print("close");
	}
	
	void enumerateDir(String filename, int loop) throws Exception {
		System.out.println("reading enumarate dir, path " + filename);
		
		//warmup
		ConcurrentLinkedQueue<CrailBuffer> bufferQueue = new ConcurrentLinkedQueue<CrailBuffer>();
		CrailBuffer buf = fs.allocateBuffer();
		bufferQueue.add(buf);
		warmUp(filename, warmup, bufferQueue);		
		fs.freeBuffer(buf);			

		//benchmark
		System.out.println("starting benchmark...");
		fs.getStatistics().reset();
		long start = System.currentTimeMillis();
		for (int i = 0; i < loop; i++) {
			// single operation == loop
			Iterator<String> iter = fs.lookup(filename).get().asDirectory().listEntries();
			while (iter.hasNext()) {
				iter.next();
			}
		}
		long end = System.currentTimeMillis();
		double executionTime = ((double) (end - start));
		double latency = executionTime * 1000.0 / ((double) loop);
		System.out.println("execution time [ms] " + executionTime);
		System.out.println("latency [us] " + latency);

		fs.getStatistics().print("close");
	}
	
	void browseDir(String filename) throws Exception {
		System.out.println("reading enumarate dir, path " + filename);
		
		//benchmark
		System.out.println("starting benchmark...");
		fs.getStatistics().reset();
		CrailNode node = fs.lookup(filename).get();
		System.out.println("node type is " + node.getType());
		
		Iterator<String> iter = node.getType() == CrailNodeType.DIRECTORY ? node.asDirectory().listEntries() : node.asMultiFile().listEntries();
		while (iter.hasNext()) {
			String name = iter.next();
			System.out.println(name);
		}
		fs.getStatistics().print("close");
	}	
	
	void early(String filename) throws Exception {
		ByteBuffer buf = ByteBuffer.allocateDirect(32);
		CrailFile file = fs.create(filename, CrailNodeType.DATAFILE, CrailStorageClass.DEFAULT, CrailLocationClass.DEFAULT, true).early().asFile();
		CrailBufferedOutputStream stream = file.getBufferedOutputStream(0);
		System.out.println("buffered stream initialized");
		
		Thread.sleep(1000);
		stream.write(buf);
		System.out.println("buffered stream written");

		Thread.sleep(1000);
		stream.write(buf);
		System.out.println("buffered stream written");		
		
		stream.purge();
		stream.close();
		
		System.out.println("buffered stream closed");
		
		fs.getStatistics().print("close");
	}
	
	void writeInt(String filename, int loop) throws Exception {
		System.out.println("writeInt, filename " + filename  + ", loop " + loop);
		
		//benchmark
		System.out.println("starting benchmark...");
		double ops = 0;
		CrailFile file = fs.create(filename, CrailNodeType.DATAFILE, CrailStorageClass.DEFAULT, CrailLocationClass.DEFAULT, true).get().asFile();
		CrailBufferedOutputStream outputStream = file.getBufferedOutputStream(loop*4);	
		int intValue = 0;
		System.out.println("starting write at position " + outputStream.position());
		while (ops < loop) {
			System.out.println("writing position " + outputStream.position() + ", value " + intValue);
			outputStream.writeInt(intValue);
			intValue++;
			ops++;
		}
		outputStream.purge().get();
		outputStream.sync().get();
		
		fs.getStatistics().print("close");		
	}
	
	void readInt(String filename, int loop) throws Exception {
		System.out.println("seek, filename " + filename  + ", loop " + loop);
		
		//benchmark
		System.out.println("starting benchmark...");
		double ops = 0;
		CrailFile file = fs.lookup(filename).get().asFile();
		CrailBufferedInputStream inputStream = file.getBufferedInputStream(loop*4);	
		System.out.println("starting read at position " + inputStream.position());
		while (ops < loop) {
			System.out.print("reading position " + inputStream.position() + ", expected " + inputStream.position()/4 + " ");
			int intValue = inputStream.readInt();
			System.out.println(", value " + intValue);
			ops++;
		}
		inputStream.close();
		
		fs.getStatistics().print("close");		
	}
	
	void seekInt(String filename, int loop) throws Exception {
		System.out.println("seek, filename " + filename  + ", loop " + loop);
		
		//benchmark
		System.out.println("starting benchmark...");
		double ops = 0;
		CrailFile file = fs.lookup(filename).get().asFile();
		Random random = new Random();
		long nbrOfInts = file.getCapacity() / 4;
		CrailBufferedInputStream seekStream = file.getBufferedInputStream(loop*4);	
		System.out.println("starting seek phase, nbrOfInts " + nbrOfInts + ", position " + seekStream.position());
		long falseMatches = 0;
		while (ops < loop) {
			int intIndex = random.nextInt((int) nbrOfInts);
			int pos = intIndex*4;
			seekStream.seek((long) pos);
			int intValue = seekStream.readInt();
			if (intIndex != intValue){
				falseMatches++;
				System.out.println("reading, position " + pos + ", expected " + pos/4 + ", ########## value " + intValue);
			} else {
				System.out.println("reading, position " + pos + ", expected " + pos/4 + ", value " + intValue);
			}
			ops++;
		}			
		seekStream.close();
		long end = System.currentTimeMillis();
		
		System.out.println("falseMatches " + falseMatches);
		fs.getStatistics().print("close");
	}	
	
	void readMultiStreamInt(String filename, int loop, int batch) throws Exception {
		System.out.println("readMultiStreamInt, filename " + filename  + ", loop " + loop + ", batch " + batch);
		
		System.out.println("starting benchmark...");
		fs.getStatistics().reset();
		CrailBufferedInputStream multiStream = fs.lookup(filename).get().asMultiFile().getMultiStream(batch);
		double ops = 0;
		long falseMatches = 0;
		while (ops < loop) {
			System.out.print("reading position " + multiStream.position() + ", expected " + multiStream.position()/4 + " ");
			long expected = multiStream.position()/4;
			int intValue = multiStream.readInt();
			if (expected != intValue){
				falseMatches++;
			}
			System.out.println(", value " + intValue);
			ops++;
		}
		multiStream.close();	
		
		System.out.println("falseMatches " + falseMatches);
		
		fs.getStatistics().print("close");
	}	
	
	void printLocationClass() throws Exception {
		System.out.println("locationClass " + fs.getLocationClass());
	}

	void locationMap() throws Exception {
		ConcurrentHashMap<String, String> locationMap = new ConcurrentHashMap<String, String>();
		CrailUtils.parseMap(CrailConstants.LOCATION_MAP, locationMap);

		System.out.println("Parsing locationMap " + CrailConstants.LOCATION_MAP);
		for (String key : locationMap.keySet()) {
			System.out.println("key " + key + ", value " + locationMap.get(key));
		}
	}

	void collectionTest(int size, int loop) throws Exception {
		System.out.println("collectionTest, size " + size  + ", loop " + loop);

		RingBuffer<Object> ringBuffer = new RingBuffer<Object>(10);
		ArrayBlockingQueue<Object> arrayQueue = new ArrayBlockingQueue<Object>(10);
		LinkedBlockingQueue<Object> listQueue = new LinkedBlockingQueue<Object>();
		
		Object obj = new Object();
		long start = System.currentTimeMillis();
		for (int i = 0; i < loop; i++){
			for (int j = 0; j < size; j++){
				ringBuffer.add(obj);
				Object tmp = ringBuffer.peek();
				tmp = ringBuffer.poll();
			}
		}
		long end = System.currentTimeMillis();
		double executionTime = ((double) (end - start));
		System.out.println("ringbuffer, execution time [ms] " + executionTime);
		
		start = System.currentTimeMillis();
		for (int i = 0; i < loop; i++){
			for (int j = 0; j < size; j++){
				arrayQueue.add(obj);
				Object tmp = arrayQueue.peek();
				tmp = arrayQueue.poll();
			}
		}
		end = System.currentTimeMillis();
		executionTime = ((double) (end - start));
		System.out.println("arrayQueue, execution time [ms] " + executionTime);		
		
		start = System.currentTimeMillis();
		for (int i = 0; i < loop; i++){
			for (int j = 0; j < size; j++){
				listQueue.add(obj);
				Object tmp = listQueue.peek();
				tmp = listQueue.poll();
			}
		}
		end = System.currentTimeMillis();
		executionTime = ((double) (end - start));
		System.out.println("arrayQueue, execution time [ms] " + executionTime);			
	}	
	
	private void warmUp(String filename, int operations, ConcurrentLinkedQueue<CrailBuffer> bufferList) throws Exception {
		Random random = new Random();
		String warmupFilename = filename + random.nextInt();
		System.out.println("warmUp, warmupFile " + warmupFilename + ", operations " + operations);
		if (operations > 0){
			CrailFile warmupFile = fs.create(warmupFilename, CrailNodeType.DATAFILE, CrailStorageClass.DEFAULT, CrailLocationClass.DEFAULT, true).get().asFile();
			CrailBufferedOutputStream warmupStream = warmupFile.getBufferedOutputStream(0);
			for (int i = 0; i < operations; i++){
				CrailBuffer buf = bufferList.poll();
				buf.clear();
				warmupStream.write(buf.getByteBuffer());
				bufferList.add(buf);
			}
			warmupStream.purge().get();
			warmupStream.close();
			fs.delete(warmupFilename, false).get().syncDir();			
		}
	}
	
	public static void main(String[] args) throws Exception {
		String type = "";
		String filename = "/tmp.dat";
		int size = CrailConstants.BUFFER_SIZE;
		int loop = 1;
		int batch = 1;
		int warmup = 32;
		int experiments = 1;
		boolean keepOpen = false;
		int storageClass = 0;
		int locationClass = 0;
		boolean useBuffered = true;
		boolean skipDir = false;
		
		String benchmarkTypes = "write|writeAsync|readSequential|readRandom|readSequentialAsync|readMultiStream|"
				+ "createFile|createFileAsync|createMultiFile|getKey|getFile|getFileAsync|enumerateDir|browseDir|"
				+ "writeInt|readInt|seekInt|readMultiStreamInt|printLocationclass";
		Option typeOption = Option.builder("t").desc("type of experiment [" + benchmarkTypes + "]").hasArg().build();
		Option fileOption = Option.builder("f").desc("filename").hasArg().build();
		Option sizeOption = Option.builder("s").desc("buffer size [bytes]").hasArg().build();
		Option loopOption = Option.builder("k").desc("loop [1..n]").hasArg().build();
		Option batchOption = Option.builder("b").desc("batch size [1..n]").hasArg().build();
		Option storageOption = Option.builder("c").desc("storageClass for file [1..n]").hasArg().build();
		Option locationOption = Option.builder("p").desc("locationClass for file [1..n]").hasArg().build();
		Option warmupOption = Option.builder("w").desc("number of warmup operations [1..n]").hasArg().build();
		Option experimentOption = Option.builder("e").desc("number of experiments [1..n]").hasArg().build();
		Option openOption = Option.builder("o").desc("whether to keep the file system open [true|false]").hasArg().build();
		Option skipDirOption = Option.builder("d").desc("skip writing the directory record [true|false]").hasArg().build();
		Option bufferedOption = Option.builder("m").desc("use buffer streams [true|false]").hasArg().build();
		
		Options options = new Options();
		options.addOption(typeOption);
		options.addOption(fileOption);
		options.addOption(sizeOption);
		options.addOption(loopOption);
		options.addOption(batchOption);
		options.addOption(storageOption);
		options.addOption(locationOption);
		options.addOption(warmupOption);
		options.addOption(experimentOption);
		options.addOption(openOption);
		options.addOption(bufferedOption);
		options.addOption(skipDirOption);
		
		CommandLineParser parser = new DefaultParser();
		CommandLine line = parser.parse(options, Arrays.copyOfRange(args, 0, args.length));
		if (line.hasOption(typeOption.getOpt())) {
			type = line.getOptionValue(typeOption.getOpt());
		}
		if (line.hasOption(fileOption.getOpt())) {
			filename = line.getOptionValue(fileOption.getOpt());
		}
		if (line.hasOption(sizeOption.getOpt())) {
			size = Integer.parseInt(line.getOptionValue(sizeOption.getOpt()));
		}
		if (line.hasOption(loopOption.getOpt())) {
			loop = Integer.parseInt(line.getOptionValue(loopOption.getOpt()));
		}
		if (line.hasOption(batchOption.getOpt())) {
			batch = Integer.parseInt(line.getOptionValue(batchOption.getOpt()));
		}
		if (line.hasOption(storageOption.getOpt())) {
			storageClass = Integer.parseInt(line.getOptionValue(storageOption.getOpt()));
		}
		if (line.hasOption(locationOption.getOpt())) {
			locationClass = Integer.parseInt(line.getOptionValue(locationOption.getOpt()));
		}		
		if (line.hasOption(warmupOption.getOpt())) {
			warmup = Integer.parseInt(line.getOptionValue(warmupOption.getOpt()));
		}
		if (line.hasOption(experimentOption.getOpt())) {
			experiments = Integer.parseInt(line.getOptionValue(experimentOption.getOpt()));
		}
		if (line.hasOption(openOption.getOpt())) {
			keepOpen = Boolean.parseBoolean(line.getOptionValue(openOption.getOpt()));
		}
		if (line.hasOption(bufferedOption.getOpt())) {
			useBuffered = Boolean.parseBoolean(line.getOptionValue(bufferedOption.getOpt()));
		}	
		if (line.hasOption(skipDirOption.getOpt())) {
			skipDir = Boolean.parseBoolean(line.getOptionValue(skipDirOption.getOpt()));
		}		
		
		CrailBenchmark benchmark = new CrailBenchmark(warmup);
		if (type.equals("write")){
			benchmark.open();
			benchmark.write(filename, size, loop, storageClass, locationClass, useBuffered, skipDir);
			benchmark.close();
		} else if (type.equalsIgnoreCase("writeAsync")) {
			benchmark.open();
			benchmark.writeAsync(filename, size, loop, batch, storageClass, locationClass, skipDir);
			benchmark.close();
		} else if (type.equalsIgnoreCase("readSequential")){
			if (keepOpen) benchmark.open();
			for (int i = 0; i < experiments; i++){
				System.out.println("experiment " + i);
				if (!keepOpen) benchmark.open();
				benchmark.readSequential(filename, size, loop, useBuffered);
				if (!keepOpen) benchmark.close();
			}			
			if (keepOpen) benchmark.close();
		} else if (type.equals("readRandom")){
			if (keepOpen) benchmark.open();
			for (int i = 0; i < experiments; i++){
				System.out.println("experiment " + i);
				if (!keepOpen) benchmark.open();
				benchmark.readRandom(filename, size, loop, useBuffered);
				if (!keepOpen) benchmark.close();
			}
			if (keepOpen) benchmark.close();
		} else if (type.equalsIgnoreCase("readSequentialAsync")) {
			if (keepOpen) benchmark.open();
			for (int i = 0; i < experiments; i++){
				System.out.println("experiment " + i);
				if (!keepOpen) benchmark.open();
				benchmark.readSequentialAsync(filename, size, loop, batch);
				if (!keepOpen) benchmark.close();
			}
			if (keepOpen) benchmark.close();
		} else if (type.equalsIgnoreCase("readMultiStream")) {
			if (keepOpen) benchmark.open();
			for (int i = 0; i < experiments; i++){
				System.out.println("experiment " + i);
				if (!keepOpen) benchmark.open();
				benchmark.readMultiStream(filename, size, loop, batch);
				if (!keepOpen) benchmark.close();
			}
			if (keepOpen) benchmark.close();
		} else if (type.equals("createFile")){
			benchmark.open();
			benchmark.createFile(filename, loop);
			benchmark.close();
		} else if (type.equals("createFileAsync")){
			benchmark.open();
			benchmark.createFileAsync(filename, loop, batch);
			benchmark.close();
		} else if (type.equalsIgnoreCase("createMultiFile")) {
			benchmark.open();
			benchmark.createMultiFile(filename, storageClass);
			benchmark.close();
		} else if (type.equalsIgnoreCase("getKey")) {
			benchmark.open();
			benchmark.getKey(filename, size, loop);
			benchmark.close();
		} else if (type.equals("getFile")){
			benchmark.open();
			benchmark.getFile(filename, loop);
			benchmark.close();
		} else if (type.equals("getFileAsync")){
			benchmark.open();
			benchmark.getFileAsync(filename, loop, batch);
			benchmark.close();
		} else if (type.equalsIgnoreCase("enumerateDir")) {
			benchmark.open();
			benchmark.enumerateDir(filename, batch);
			benchmark.close();
		} else if (type.equalsIgnoreCase("browseDir")) {
			benchmark.open();
			benchmark.browseDir(filename);
			benchmark.close();
		} else if (type.equalsIgnoreCase("early")) {
			benchmark.open();
			benchmark.early(filename);
			benchmark.close();
		} else if (type.equalsIgnoreCase("writeInt")) {
			benchmark.open();
			benchmark.writeInt(filename, loop);
			benchmark.close();
		} else if (type.equalsIgnoreCase("readInt")) {
			benchmark.open();
			benchmark.readInt(filename, loop);
			benchmark.close();
		} else if (type.equalsIgnoreCase("seekInt")) {
			benchmark.open();
			benchmark.seekInt(filename, loop);
			benchmark.close();
		} else if (type.equalsIgnoreCase("readMultiStreamInt")) {
			benchmark.open();
			benchmark.readMultiStreamInt(filename, loop, batch);
			benchmark.close();
		} else if (type.equalsIgnoreCase("printLocationClass")) {
			benchmark.open();
			benchmark.printLocationClass();
			benchmark.close();
		} else if (type.equalsIgnoreCase("collection")) {
			for (int i = 0; i < experiments; i++){
				benchmark.collectionTest(size, loop);
			}
		} else if (type.equalsIgnoreCase("locationMap")) {
			benchmark.locationMap();
		} else {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("crail iobench", options);
			System.exit(-1);
		}
	}

}

