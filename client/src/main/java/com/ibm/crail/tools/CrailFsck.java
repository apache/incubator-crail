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

import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

import com.ibm.crail.CrailBlockLocation;
import com.ibm.crail.CrailDirectory;
import com.ibm.crail.CrailFS;
import com.ibm.crail.CrailFile;
import com.ibm.crail.conf.CrailConfiguration;
import com.ibm.crail.conf.CrailConstants;
import com.ibm.crail.core.CoreFileSystem;
import com.ibm.crail.core.DirectoryInputStream;
import com.ibm.crail.core.DirectoryRecord;
import com.ibm.crail.namenode.protocol.FileName;
import com.ibm.crail.utils.GetOpt;
import com.ibm.crail.utils.CrailUtils;

public class CrailFsck {
	
	public CrailFsck(){
		
	}
	
	public static void usage() {
		System.out.println("Usage: ");
		System.out.println("fsck -t <getLocations|directoryDump|namenodeDump|blockStatistics|ping> " + 
		"-f <file/dir> -y <offset> -l <length> -r <true/false>");
		System.exit(1);
	}		
	
	public void getLocations(String filename, int offset, int length) throws Exception {
		System.out.println("getLocations, filename " + filename + ", offset " + offset + ", len " + length);
		CrailConfiguration conf = new CrailConfiguration();
		CrailFS fs = CrailFS.newInstance(conf);
		CrailBlockLocation locations[] = fs.lookup(filename).get().asFile().getBlockLocations(offset, length);
		for (int i = 0; i < locations.length; i++){
			System.out.println("location " + i + " : " + locations[i].toString());
		}	
		fs.close();
	}
	
	public void blockStatistics(String filename) throws Exception {
		HashMap<String, AtomicInteger> stats = new HashMap<String, AtomicInteger>();
		CrailConfiguration conf = new CrailConfiguration();
		CrailFS fs = CrailFS.newInstance(conf);
		CrailDirectory directory = fs.lookup(filename).get().asDirectory();
		
		Iterator<String> iter = directory.listEntries();
		while (iter.hasNext()) {
			String path = iter.next();
			CrailFile child = fs.lookup(path).get().asFile();
			printPath(stats, fs, child.getPath(), 0, child.getCapacity());
		}
		printStats(stats);	
		fs.close();
	}

	public void namenodeDump()  throws Exception {
		CrailConfiguration conf = new CrailConfiguration();
		CoreFileSystem fs = new CoreFileSystem(conf);
		fs.dumpNameNode();
		fs.close();
	}

	public void directoryDump(String filename, boolean randomize) throws Exception {
		CrailConfiguration conf = new CrailConfiguration();
		CrailConstants.updateConstants(conf);
		CoreFileSystem fs = new CoreFileSystem(conf);		
		DirectoryInputStream iter = fs._listEntries(filename, randomize);
		System.out.println("#hash   \t\tname\t\tfilecomponent");
		int i = 0;
		while(iter.hasRecord()){
			DirectoryRecord record = iter.nextRecord();
			String path = CrailUtils.combinePath(record.getParent(), record.getFile());
			FileName hash = new FileName(path);
			System.out.format(i + ": " + "%08d\t\t%s\t%d\n", record.isValid() ? 1 : 0, padRight(record.getFile(), 8), hash.getFileComponent());
			i++;
		}
		iter.close();
		fs.closeFileSystem();
	}
	
	private void ping() throws Exception {
		CrailConfiguration conf = new CrailConfiguration();
		CrailConstants.updateConstants(conf);
		CoreFileSystem fs = new CoreFileSystem(conf);
		fs.ping();
		fs.closeFileSystem();		
	}
	
	//-----------------

	private String padRight(String s, int n) {
		return String.format("%1$-" + n + "s", s);
	}
	
	private void printStats(HashMap<String, AtomicInteger> stats) {
		for (Iterator<String> iter = stats.keySet().iterator(); iter.hasNext(); ){
			String key = iter.next();
			System.out.println(key + "\t" + stats.get(key));
		}
	}

	private void printPath(HashMap<String, AtomicInteger> stats, CrailFS fs, String filePath, long offset, long len) throws Exception {
		System.out.println("printing locations for path " + filePath);
		CrailBlockLocation locations[] = fs.lookup(filePath).get().asFile().getBlockLocations(offset, len);
		for (int i = 0; i < locations.length; i++){
			for (int j = 0; j < locations[i].getNames().length; j++){
				String name = locations[i].getNames()[j];
				String host = name.split(":")[0];
				System.out.println("..........names " + host);
				incStats(stats, host);
			}
		}
	}
	
	private void incStats(HashMap<String, AtomicInteger> stats, String host) {
		if (!stats.containsKey(host)){
			stats.put(host, new AtomicInteger(0));
		}
		stats.get(host).incrementAndGet();
	}	

	
	public static void main(String[] args) throws Exception {
		String[] _args = args;
		GetOpt go = new GetOpt(_args, "t:f:y:l:r:");
		go.optErr = true;
		int ch = -1;
		
		if (args.length < 2){
			usage();
		}
		
		String type = "";
		String filename = "/tmp.dat";
		int offset = 0;
		int length = 1;
		boolean randomize = false;
		
		while ((ch = go.getopt()) != GetOpt.optEOF) {
			if ((char) ch == 't') {
				type = go.optArgGet();
			} else if ((char) ch == 'f') {
				filename = go.optArgGet();
			} else if ((char) ch == 'y') {
				offset = Integer.parseInt(go.optArgGet());
			} else if ((char) ch == 'l') {
				length = Integer.parseInt(go.optArgGet());
			} else if ((char) ch == 'r') {
				randomize = Boolean.parseBoolean(go.optArgGet());
			} else {
				System.exit(1); // undefined option
			}
		}		
		
		CrailFsck fsck = new CrailFsck();
		if (type.equals("getLocations")){
			fsck.getLocations(filename, offset, length);
		} else if (type.equals("directoryDump")){
			fsck.directoryDump(filename, randomize);
		} else if (type.equals("namenodeDump")){
			fsck.namenodeDump();
		} else if (type.equals("blockStatistics")){
			fsck.blockStatistics(filename);
		} else if (type.equals("ping")){
			fsck.ping();
		} else {
			usage();
			System.exit(0);			
		}
	}
}
