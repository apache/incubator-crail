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

package org.apache.crail.hdfs.tools;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.conf.Configuration;

public class HdfsIOBenchmark {
	private String mode;
	private int size;
	private int loop;
	private Path path;
	
	public HdfsIOBenchmark(String[] args){
		mode = args[0];
		size = Integer.parseInt(args[1]);
		loop = Integer.parseInt(args[2]);
		path = new Path(args[3]);
	}
	
	public void run() throws Exception {
		if (mode.equals("writeSequentialHeap")){
			writeSequentialHeap();
		} else if (mode.equalsIgnoreCase("readSequentialDirect")){
			readSequentialDirect();
		} else if (mode.equals("readSequentialHeap")){
			readSequentialHeap();
		} else if (mode.equals("readRandomDirect")){
			readRandomDirect();
		} else if (mode.equals("readRandomHeap")){
			readRandomHeap();
		} else if (mode.equals("getFile")){
			getFile();
		} else if (mode.equals("createFile")){
			createFile();
		} else if (mode.equals("enumerateDir")){
			enumerateDir();
		} else if (mode.equals("keyGet")){
			keyGet();
		} else if (mode.equals("browseDir")){
			browseDir();
		} else {
			usage();
			System.exit(0);
		}
	}

	public static void usage(){
		System.out.println("Usage:");
		System.out.println("hdfsbench <readSequentialDirect|readSequentialHeap|readRandomDirect|readRandomHeap|writeSequentialHeap> <size> <iterations> <path>");
	}	
	
	public void writeSequentialHeap() throws Exception {
			System.out.println("writing sequential file in heap mode " + path);
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);
			FSDataOutputStream instream = fs.create(path);
			byte[] buf = new byte[size];
			double sumbytes = 0;
			double ops = 0;
			System.out.println("read size " + size);
			System.out.println("operations " + loop);
			
			long start = System.currentTimeMillis();
			while (ops < loop) {
	//			System.out.println("writing data, len " + buf.length);
				instream.write(buf, 0, buf.length);
				sumbytes = sumbytes + buf.length;
				ops = ops + 1.0;
			}
			instream.flush();
			long end = System.currentTimeMillis();
			double executionTime = ((double) (end - start)) / 1000.0;
			double throughput = 0.0;
			double latency = 0.0;
			double sumbits = sumbytes * 8.0;
			if (executionTime > 0) {
				throughput = sumbits / executionTime / 1024.0 / 1024.0;
				latency = 1000000.0 * executionTime / ops;
			}
			
			System.out.println("execution time " + executionTime);
			System.out.println("ops " + ops);
			System.out.println("sumbytes " + sumbytes);
			System.out.println("throughput " + throughput);
			System.out.println("latency " + latency);
			System.out.println("closing stream");
			instream.close();	
			fs.close();		
		}

	public void readSequentialDirect() throws Exception {
		System.out.println("reading sequential file in direct mode " + path);
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		FileStatus status = fs.getFileStatus(path);
		FSDataInputStream instream = fs.open(path);
		ByteBuffer buf = ByteBuffer.allocateDirect(size);
		buf.clear();
		double sumbytes = 0;
		double ops = 0;
		System.out.println("file capacity " + status.getLen());
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
			throughput = sumbits / executionTime / 1024.0 / 1024.0;
			latency = 1000000.0 * executionTime / ops;
		}
		System.out.println("execution time " + executionTime);
		System.out.println("ops " + ops);
		System.out.println("sumbytes " + sumbytes);
		System.out.println("throughput " + throughput);
		System.out.println("latency " + latency);
		System.out.println("closing stream");
		instream.close();	
		fs.close();
	}

	public void readSequentialHeap() throws Exception {
		System.out.println("reading sequential file in heap mode " + path);
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		FileStatus status = fs.getFileStatus(path);
		FSDataInputStream instream = fs.open(path);
		byte[] buf = new byte[size];
		double sumbytes = 0;
		double ops = 0;
		System.out.println("file capacity " + status.getLen());
		System.out.println("read size " + size);
		System.out.println("operations " + loop);
		
		long start = System.currentTimeMillis();
		while (ops < loop) {
			double ret = (double) this.read(instream, buf);
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
			throughput = sumbits / executionTime / 1024.0 / 1024.0;
			latency = 1000000.0 * executionTime / ops;
		}
		System.out.println("execution time " + executionTime);
		System.out.println("ops " + ops);
		System.out.println("sumbytes " + sumbytes);
		System.out.println("throughput " + throughput);
		System.out.println("latency " + latency);
		System.out.println("closing stream");
		instream.close();	
		fs.close();
	}

	public void readRandomDirect() throws Exception{
		System.out.println("reading random file in direct mode " + path);
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		FileStatus status = fs.getFileStatus(path);
		FSDataInputStream instream = fs.open(path);
		ByteBuffer buf = ByteBuffer.allocateDirect(size);
		buf.clear();
		double sumbytes = 0;
		double ops = 0;
	    long _range = status.getLen()- ((long)buf.capacity());
	    double range = (double) _range;
		Random random = new Random();
		
		System.out.println("file capacity " + status.getLen());
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
			throughput = sumbits / executionTime / 1024.0 / 1024.0;
			latency = 1000000.0 * executionTime / ops;
		}		
		
		System.out.println("execution time " + executionTime);
		System.out.println("ops " + ops);
		System.out.println("sumbytes " + sumbytes);
		System.out.println("throughput " + throughput);
		System.out.println("latency " + latency);
		System.out.println("closing stream");
		instream.close();
		fs.close();
	}

	public void readRandomHeap() throws Exception{
		System.out.println("reading random file in heap mode " + path);
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		FileStatus status = fs.getFileStatus(path);
		FSDataInputStream instream = fs.open(path);
		byte[] buf = new byte[size];		
		double sumbytes = 0;
		double ops = 0;
	    long _range = status.getLen()- ((long)buf.length);
	    double range = (double) _range;
		Random random = new Random();
		
		System.out.println("file capacity " + status.getLen());
		System.out.println("read size " + size);
		System.out.println("operations " + loop);
		long start = System.currentTimeMillis();
		while (ops < loop) {
	        double _offset = range*random.nextDouble();
	        long offset = (long) _offset;
			instream.seek(offset);
			double ret = (double) this.read(instream, buf);
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
			throughput = sumbits / executionTime / 1024.0 / 1024.0;
			latency = 1000000.0 * executionTime / ops;
		}		
		
		System.out.println("execution time " + executionTime);
		System.out.println("ops " + ops);
		System.out.println("sumbytes " + sumbytes);
		System.out.println("throughput " + throughput);
		System.out.println("latency " + latency);
		System.out.println("closing stream");
		instream.close();
		fs.close();
	}

	void getFile() throws Exception, InterruptedException {
		System.out.println("get file, path " + path + ", outstanding " + size + ", loop " + loop);
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		
		Path paths[] = new Path[loop];
		for (int j = 0; j < loop; j++){
			paths[j] = new Path(path.toString() + "/" + j);
		}
		int repfactor = 4;
		for (int k = 0; k < repfactor; k++){
			long start = System.currentTimeMillis();
			for (int i = 0; i < size; i++){
				//single operation == loop
				for (int j = 0; j < loop; j++){
					fs.listStatus(paths[j]);
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
		System.out.println("create file async hdfs, path " + path + ", size " + size + ", loop " + loop);
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf); 
		
		int repfactor = 4;
		for (int k = 0; k < repfactor; k++){
			LinkedBlockingQueue<Path> pathQueue = new LinkedBlockingQueue<Path>();
			fs.mkdirs(path);
			for (int i = 0; i < loop*size; i++){
				String name = "" + i;
				Path f = new Path(path, name);
				pathQueue.add(f);
			}			
			
			LinkedBlockingQueue<FSDataOutputStream> streamQueue = new LinkedBlockingQueue<FSDataOutputStream>();
			long start = System.currentTimeMillis();
			for (int i = 0; i < size; i++){
				//single operation == loop
				for (int j = 0; j < loop; j++) {
					Path path = pathQueue.poll();
					fs.create(path).close();
				}
			}
			long end = System.currentTimeMillis();
			double executionTime = ((double) (end - start));
			double latency = executionTime*1000.0 / ((double) size);
			System.out.println("execution time [ms] " + executionTime);
			System.out.println("latency [us] " + latency);
			
			while(!streamQueue.isEmpty()){
				FSDataOutputStream stream = streamQueue.poll();
				stream.close();
			}

			if (k < repfactor - 1){
				fs.delete(path, true);
				Thread.sleep(2000);
			}
		}	
		fs.close();
	}
	
	void enumerateDir() throws Exception {
		System.out.println("enumarate dir, path " + path);
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf); 

		int repfactor = 4;
		for (int k = 0; k < repfactor; k++) {
			long start = System.currentTimeMillis();
			for (int i = 0; i < size; i++) {
				// single operation == loop
				RemoteIterator<LocatedFileStatus> iter = fs.listFiles(path, false);
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
		System.out.println("key get, path " + path + ", size " + size + ", loop " + loop);
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf); 
		
		Path[] paths = new Path[loop];
		for (int i = 0; i < loop; i++){
			String child = "" + i;
			paths[i] = new Path(path, child);
			System.out.println("path " + paths[i]);
		}
		
		byte[] outBuf = new byte[size];
		for (Path p : paths){
			FSDataOutputStream outputStream = fs.create(p);
			outputStream.write(outBuf);
			outputStream.close();
		}
		
		long start = System.currentTimeMillis();
		ByteBuffer inBuf = ByteBuffer.allocateDirect(size);
		for (int i = 0; i < loop; i++){
			Path p = paths[i];
			FSDataInputStream inputStream = fs.open(p);
			inBuf.clear();
			while(inBuf.remaining() > 0){
				inputStream.read(inBuf);
			}
			inputStream.close();
		}
		long end = System.currentTimeMillis();
		double executionTime = ((double) (end - start));
		double latency = executionTime * 1000.0 / ((double) loop);
		System.out.println("execution time [ms] " + executionTime);
		System.out.println("latency [us] " + latency);		
		
		fs.close();
	}		
	
	private int read(FSDataInputStream stream, byte[] buf) throws IOException{
		int off = 0;
		int len = buf.length;
		int ret = stream.read(buf, off, len);
		while(ret > 0 && len - ret > 0){
			len -= ret;
			off += ret;
			ret = stream.read(buf, off, len);
		}
		return off > 0 || ret > 0 ? ret : -1;
	}
	
	void browseDir() throws Exception {
		System.out.println("reading enumarate dir, path " + path);
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf); 
		
		//benchmark
		System.out.println("starting benchmark...");
		RemoteIterator<LocatedFileStatus> iter = fs.listFiles(path, false);
		while (iter.hasNext()) {
			LocatedFileStatus status = iter.next();
			System.out.println(status.getPath());
		}		
		fs.close();
	}	
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		if (args.length != 4){
			usage();
			System.exit(0);
		}
		
		try {
			HdfsIOBenchmark benchmark = new HdfsIOBenchmark(args);
			benchmark.run();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
