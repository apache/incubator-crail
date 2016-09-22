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
import java.util.LinkedList;
import java.util.Random;
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

public class CrailBenchmark {
	private String mode;
	private int size;
	private int loop;
	private String filename;
	
	public CrailBenchmark(String[] args){
		mode = args[0];
		size = Integer.parseInt(args[1]);
		loop = Integer.parseInt(args[2]);
		filename = args[3];
	}
	
	public void run() throws Exception {
		if (mode.equals("writeClusterHeap")){
			writeSequential(false, false);
		} else if (mode.equals("writeClusterDirect")){
			writeSequential(false, true);
		} else if (mode.equals("writeLocalHeap")){
			writeSequential(true, false);
		} else if (mode.equals("writeLocalDirect")){
			writeSequential(true, true);
		} else if (mode.equalsIgnoreCase("writeAsyncCluster")) {
			writeSequentialAsync(false, true);
		} else if (mode.equalsIgnoreCase("writeAsyncLocal")) {
			writeSequentialAsync(true, true);
		} else if (mode.equalsIgnoreCase("readSequentialDirect")){
			readSequential(true);
		} else if (mode.equals("readSequentialHeap")){
			readSequential(false);
		} else if (mode.equals("readRandomDirect")){
			readRandom(true);
		} else if (mode.equals("readRandomHeap")){
			readRandom(false);
		} else if (mode.equalsIgnoreCase("readAsync")) {
			readSequentialAsync(true);
		} else if (mode.equalsIgnoreCase("readMultiStream")) {
			readMultiStream();
		} else if (mode.equals("getFile")){
			getFile();
		} else if (mode.equals("getFileAsync")){
			getFileAsync();
		} else if (mode.equals("createFile")){
			createFile();
		} else if (mode.equals("createFileAsync")){
			createFileAsync();
		} else if (mode.equalsIgnoreCase("enumerateDir")) {
			enumerateDir();
		} else if (mode.equalsIgnoreCase("keyGet")) {
			keyGet();
		} else {
			usage();
			System.exit(0);
		}
	}
	
	public static void usage() {
		System.out.println("Usage: ");
		System.out.println(
				"iobench <writeClusterHeap|writeClusterDirect|writeLocalHeap|"
				+ "writeLocalDirect|writeAsyncCluster|writeAsyncLocal|readAsync|"
				+ "readSequentialDirect|readSequentialHeap|readRandomDirect|"
				+ "readRandomHeap|readMultiStream"
				+ "enumerateDir|keyGet|getFile"
				+ " <size> <iterations> <path>");
	}

	void writeSequential(boolean affinity, boolean direct) throws Exception {
		System.out.println("writing sequential path " + filename + ", affinity " + affinity + ", direct " + direct + ", size " + size + ", loop " + loop);
		CrailConfiguration conf = new CrailConfiguration();
		CrailFS fs = CrailFS.newInstance(conf);
		
		int hosthash = 0;
		if (affinity){
			hosthash = fs.getHostHash();
		}
		
		CrailFile file = fs.createFile(filename, 0, hosthash).get().syncDir();
		long _loop = (long) loop;
		long _bufsize = (long) CrailConstants.BUFFER_SIZE;
		long _capacity = _loop*_bufsize;
		CrailBufferedOutputStream outstream = file.getBufferedOutputStream(_capacity);			
		
		ByteBuffer buf = null;
		if (direct){
			buf = ByteBuffer.allocateDirect(size);
		} else {
			buf = ByteBuffer.allocate(size);
		}
		buf.clear();
		double sumbytes = 0;
		double ops = 0;
		System.out.println("read size " + size);
		System.out.println("operations " + loop);
		
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
		System.out.println("closing stream");
		fs.printStatistics("close");
		fs.close();		
	}
	
	void writeSequentialAsync(boolean affinity, boolean direct) throws Exception {
		System.out.println("writing sequential async, path " + filename + ", size " + size);
		CrailConfiguration conf = new CrailConfiguration();
		CrailFS fs = CrailFS.newInstance(conf);
		int hosthash = 0;
		if (affinity){
			hosthash = fs.getHostHash();
		}
		
		CrailFile file = fs.createFile(filename, hosthash, 0).get();
		CrailBufferedOutputStream outstream = file.getBufferedOutputStream(0);			
		
		LinkedBlockingQueue<ByteBuffer> bufferQueue = new LinkedBlockingQueue<ByteBuffer>();
		LinkedBlockingQueue<Future<CrailResult>> futureQueue = new LinkedBlockingQueue<Future<CrailResult>>();
		HashMap<Integer, ByteBuffer> futureMap = new HashMap<Integer, ByteBuffer>();
		for (int i = 0; i < size; i++){
			ByteBuffer buf = null;
			if (direct){
				buf = ByteBuffer.allocateDirect((int) CrailConstants.BUFFER_SIZE);
			} else {
				buf = ByteBuffer.allocate((int) CrailConstants.BUFFER_SIZE);
			}
			bufferQueue.add(buf);
			System.out.println("buffer size " + buf.capacity());
		}
		double sumbytes = 0;
		double ops = 0;
		
		long start = System.currentTimeMillis();
		for (int i = 0; i < size - 1 && ops < loop; i++){
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
		fs.close();		
		
		System.out.println("execution time " + executionTime);
		System.out.println("ops " + ops);
		System.out.println("sumbytes " + sumbytes);
		System.out.println("throughput " + throughput);
		System.out.println("latency " + latency);
		fs.printStatistics("close");
		System.out.println("closing stream");
	}

	void readSequential(boolean direct) throws Exception {
		System.out.println("read sequential path " + filename + ", direct " + direct);
		CrailConfiguration conf = new CrailConfiguration();
		CrailFS fs = CrailFS.newInstance(conf);
		
		CrailFile file = fs.lookupFile(filename, false).get();
		CrailBufferedInputStream instream = file.getBufferedInputStream(file.getCapacity());
		ByteBuffer buf = null;
		if (direct){
			buf = ByteBuffer.allocateDirect(size);
		} else {
			buf = ByteBuffer.allocate(size);
		}
		buf.clear();
		double sumbytes = 0;
		double ops = 0;
		System.out.println("file capacity " + file.getCapacity());
		System.out.println("read size " + size);
		System.out.println("operations " + loop);
		
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
		System.out.println("execution time " + executionTime);
		System.out.println("ops " + ops);
		System.out.println("sumbytes " + sumbytes);
		System.out.println("throughput " + throughput);
		System.out.println("latency " + latency);
		System.out.println("closing stream");
		instream.close();	
		fs.printStatistics("close");
		fs.close();
	}
	
	void readRandom(boolean direct) throws Exception{
		System.out.println("read random path " + filename + ", direct " + direct);
		CrailConfiguration conf = new CrailConfiguration();
		CrailFS fs = CrailFS.newInstance(conf);
		
		CrailFile file = fs.lookupFile(filename, false).get();
		CrailBufferedInputStream instream = file.getBufferedInputStream(file.getCapacity());				
		
		ByteBuffer buf = null;
		if (direct){
			buf = ByteBuffer.allocateDirect(size);
		} else {
			buf = ByteBuffer.allocate(size);
		}
		double sumbytes = 0;
		double ops = 0;
        long _range = file.getCapacity() - ((long)buf.capacity());
        double range = (double) _range;
		Random random = new Random();
		
		System.out.println("file capacity " + file.getCapacity());
		System.out.println("read size " + size);
		System.out.println("operations " + loop);
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
		
		System.out.println("execution time " + executionTime);
		System.out.println("ops " + ops);
		System.out.println("sumbytes " + sumbytes);
		System.out.println("throughput " + throughput);
		System.out.println("latency " + latency);
		System.out.println("closing stream");
		instream.close();
		fs.printStatistics("close");
		fs.close();
	}	
	
	void readSequentialAsync(boolean direct) throws Exception {
		System.out.println("reading sequential async, path " + filename + ", size " + size);
		CrailConfiguration conf = new CrailConfiguration();
		CrailFS fs = CrailFS.newInstance(conf);
		
		CrailFile file = fs.lookupFile(filename, false).get();
		CrailBufferedInputStream instream = file.getBufferedInputStream(file.getCapacity());		
		
		LinkedBlockingQueue<ByteBuffer> bufferQueue = new LinkedBlockingQueue<ByteBuffer>();
		LinkedBlockingQueue<Future<CrailResult>> futureQueue = new LinkedBlockingQueue<Future<CrailResult>>();
		HashMap<Integer, ByteBuffer> futureMap = new HashMap<Integer, ByteBuffer>();
		for (int i = 0; i < size; i++){
			ByteBuffer buf = null;
			if (direct){
				buf = ByteBuffer.allocateDirect((int) CrailConstants.BUFFER_SIZE);
			} else {
				buf = ByteBuffer.allocate((int) CrailConstants.BUFFER_SIZE);
			}
			bufferQueue.add(buf);
			System.out.println("buffer size " + buf.capacity());
		}
		double sumbytes = 0;
		double ops = 0;
		
		//fill the meta data cache
		ByteBuffer preBuf = ByteBuffer.allocateDirect(CrailConstants.BUFFER_SIZE);
		while(instream.available() > 0){
			preBuf.clear();
			instream.read(preBuf);
		}
		instream.seek(0);
		instream.setReadahead(file.getCapacity());
		
		long start = System.currentTimeMillis();
		for (int i = 0; i < size - 1 && ops < loop; i++){
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
		System.out.println("closing stream");
		instream.close();	
		
		System.out.println("execution time " + executionTime);
		System.out.println("ops " + ops);
		System.out.println("sumbytes " + sumbytes);
		System.out.println("throughput " + throughput);
		System.out.println("latency " + latency);
		fs.printStatistics("close");
		fs.close();		
	}

	void readMultiStream() throws Exception {
		System.out.println("reading multistream directory, path " + filename + ", size(unused) " + size + ", loop(outstanding) " + loop);
		CrailConfiguration conf = new CrailConfiguration();
		CrailFS fs = CrailFS.newInstance(conf);
		
		Iterator<String> entries = fs.listEntries(filename);
		long totalCap = 0;
		long totalFiles = 0;
		while(entries.hasNext()){
			String path = entries.next();
			CrailFile file = fs.lookupFile(path, false).get();
			totalCap = totalCap + file.getCapacity();
			totalFiles++;
		}
		System.out.println("totalFiles " + totalFiles + ", totalCap " + totalCap);
		
		for (int i = 0; i < 2; i++){
			Iterator<String> files = fs.listEntries(filename);
			CrailInputStream multiStream = fs.getMultiStream(files, loop);
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
			System.out.println("sumbytes " + sumbytes);
			System.out.println("_sumbytes " + _sumbytes);
			System.out.println("throughput " + throughput);
			System.out.println("latency " + latency);
			System.out.println("closing stream");			
		}
	
		fs.close();
	}

	void getFile() throws Exception, InterruptedException {
		System.out.println("get file, path " + filename);
		CrailConfiguration conf = new CrailConfiguration();
		CrailFS fs = CrailFS.newInstance(conf);	
		
		for (int i = 0; i < 1000; i++){
			fs.lookupFile(filename, false).get();
		}
		
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
		System.out.println("closing fs");		
		
		fs.close();
	}
	
	void getFileAsync() throws Exception, InterruptedException {
		System.out.println("get file async, path " + filename + ", outstanding " + size + ", loop " + loop);
		CrailConfiguration conf = new CrailConfiguration();
		CrailFS fs = CrailFS.newInstance(conf);	
		
		for (int i = 0; i < 1000; i++){
			fs.lookupFile(filename, false).get();
		}		
		
		LinkedBlockingQueue<Future<CrailFile>> fileQueue = new LinkedBlockingQueue<Future<CrailFile>>();
		for (int k = 0; k < 20; k++){
			long start = System.currentTimeMillis();
			for (int i = 0; i < size; i++){
				//single operation == loop
				for (int j = 0; j < loop; j++){
					Future<CrailFile> future = fs.lookupFile(filename, false);
					fileQueue.add(future);
				}
				for (int j = 0; j < loop; j++){
					Future<CrailFile> future = fileQueue.poll();
					future.get();
				}
			}
			long end = System.currentTimeMillis();
			double executionTime = ((double) (end - start));
			double latency = executionTime*1000.0 / ((double) size);
			System.out.println("execution time [ms] " + executionTime);
			System.out.println("latency [us] " + latency);
		}
		
		fs.close();
	}
	
	void createFile() throws Exception, InterruptedException {
		System.out.println("create file, path " + filename);
		CrailConfiguration conf = new CrailConfiguration();
		CrailFS fs = CrailFS.newInstance(conf);	
		
		for (int i = 0; i < 1000; i++){
			fs.lookupFile(filename, false).get();
		}
		
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
		System.out.println("closing fs");		
		
		fs.close();
	}	
	
	void createFileAsync() throws Exception, InterruptedException {
		System.out.println("create file async*, path " + filename + ", size " + size + ", loop " + loop);
		CrailConfiguration conf = new CrailConfiguration();
		CrailFS fs = CrailFS.newInstance(conf);	 
		
		for (int i = 0; i < 1000; i++){
			fs.lookupFile(filename, false).get();
		}
		
		LinkedList<ByteBuffer> bufferList = new LinkedList<ByteBuffer>(); 
		System.out.println("allocating buffers");
		for (int i = 0; i < loop; i++){
			ByteBuffer buffer = fs.allocateBuffer();
			bufferList.add(buffer);
		}
		while(!bufferList.isEmpty()){
			ByteBuffer buffer = bufferList.remove();
			fs.freeBuffer(buffer);
		}
		fs.printStatistics("init");
		
		
		for (int k = 0; k < 20; k++){
			LinkedBlockingQueue<Future<CrailFile>> futureQueue = new LinkedBlockingQueue<Future<CrailFile>>();
			LinkedBlockingQueue<CrailFile> fileQueue = new LinkedBlockingQueue<CrailFile>();
			LinkedBlockingQueue<String> pathQueue = new LinkedBlockingQueue<String>();
			fs.createDir(filename).get().syncDir();			
			for (int i = 0; i < loop*size; i++){
				String name = "/" + i;
				String f = filename + name;
				pathQueue.add(f);
			}			
			
			long start = System.currentTimeMillis();
			for (int i = 0; i < size; i++){
				//single operation == loop
				for (int j = 0; j < loop; j++) {
					String path = pathQueue.poll();
					Future<CrailFile> future = fs.createFile(path, 0, 0);
					futureQueue.add(future);
				}
				for (int j = 0; j < loop; j++){
					Future<CrailFile> future = futureQueue.poll();
					CrailFile file = future.get();
					fileQueue.add(file);					
				}
				for (int j = 0; j < loop; j++){
					CrailFile file = fileQueue.poll();
					file.syncDir();
				}
			}
			long end = System.currentTimeMillis();
			double executionTime = ((double) (end - start));
			double latency = executionTime*1000.0 / ((double) size);
			System.out.println("execution time [ms] " + executionTime);
			System.out.println("latency [us] " + latency);

			fs.delete(filename, true).get().syncDir();
			Thread.sleep(2000);
		}		
		
		fs.printStatistics("close");
		fs.close();
		
	}	
	
	void enumerateDir() throws Exception {
		System.out.println("reading enumarate dir, path " + filename);
		CrailConfiguration conf = new CrailConfiguration();
		CrailFS fs = CrailFS.newInstance(conf);

		for (int k = 0; k < 20; k++) {
			long start = System.currentTimeMillis();
			for (int i = 0; i < size; i++) {
				// single operation == loop
				Iterator<String> iter = fs.listEntries(filename);
				while (iter.hasNext()) {
					iter.next();
				}
			}
			long end = System.currentTimeMillis();
			double executionTime = ((double) (end - start));
			double latency = executionTime * 1000.0 / ((double) size);
			System.out.println("execution time [ms] " + executionTime);
			System.out.println("latency [us] " + latency);
		}

		fs.close();
	}
	
	void keyGet() throws Exception {
		System.out.println("keyGet, path " + filename + ", size " + size + ", loop " + loop);
		CrailConfiguration conf = new CrailConfiguration();
		CrailFS fs = CrailFS.newInstance(conf);

		CrailFile file = fs.createFile(filename, 0, 0).get().syncDir();
		CrailBufferedOutputStream outStream = file.getBufferedOutputStream(0);
		ByteBuffer buf = ByteBuffer.allocateDirect(size);
		outStream.write(buf);
		outStream.close();
		
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
		
		fs.close();
	}		
	
	public static void main(String[] args) {
		if (args.length != 4){
			usage();
			System.exit(0);
		}
		try {
			CrailBenchmark benchmark = new CrailBenchmark(args);
			benchmark.run();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}

