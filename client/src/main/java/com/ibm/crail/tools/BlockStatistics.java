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
import java.util.concurrent.LinkedBlockingQueue;
import org.slf4j.Logger;
import com.ibm.crail.CrailBlockLocation;
import com.ibm.crail.CrailFile;
import com.ibm.crail.CrailFS;
import com.ibm.crail.conf.CrailConfiguration;
import com.ibm.crail.utils.CrailUtils;

import java.util.concurrent.atomic.AtomicInteger;

public class BlockStatistics implements Runnable {
	private static final Logger LOG = CrailUtils.getLogger();
	
	private String path;
	private HashMap<String, AtomicInteger> stats;
	
	public BlockStatistics(CrailFsck crailTest) throws Exception {
		this.path = crailTest.getPath();
		this.stats = new HashMap<String, AtomicInteger>();
	}	

	public void run() {
		try {
			CrailConfiguration conf = new CrailConfiguration();
			CrailFS dfs = CrailFS.newInstance(conf);
			LinkedBlockingQueue<CrailFile> fileQueue = new LinkedBlockingQueue<CrailFile>();
			CrailFile directFile = dfs.lookupFile(path, false).get();
			fileQueue.add(directFile);
			
			
			while (!fileQueue.isEmpty()) {
				CrailFile file = fileQueue.poll();
				Iterator<String> iter = dfs.listEntries(file.getPath());
				while (iter.hasNext()) {
					String path = iter.next();
					CrailFile child = dfs.lookupFile(path, false).get();
					fileQueue.add(child);
				}
				printPath(dfs, file.getPath(), 0, file.getCapacity());
			}
			printStats();			
		} catch(Exception e){
			LOG.error(e.getMessage());
		}
	}	
	
	private void printStats() {
		for (Iterator<String> iter = stats.keySet().iterator(); iter.hasNext(); ){
			String key = iter.next();
			System.out.println(key + "\t" + stats.get(key));
		}
	}

	public void printPath(CrailFS fs, String filePath, long offset, long len) throws Exception {
		System.out.println("printing locations for path " + filePath);
		CrailBlockLocation locations[] = fs.getBlockLocations(filePath, offset, len);
		for (int i = 0; i < locations.length; i++){
			for (int j = 0; j < locations[i].getNames().length; j++){
				String name = locations[i].getNames()[j];
				String host = name.split(":")[0];
				System.out.println("..........names " + host);
				incStats(host);
			}
		}
	}
	
	private void incStats(String host) {
		if (!stats.containsKey(host)){
			stats.put(host, new AtomicInteger(0));
		}
		stats.get(host).incrementAndGet();
	}	
}
