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

import org.slf4j.Logger;
import com.ibm.crail.utils.GetOpt;
import com.ibm.crail.utils.CrailUtils;

public class CrailFsck {
	public static final Logger LOG = CrailUtils.getLogger();
	
	private String ipAddress = "127.0.0.1"; // of 
	private int loop = 1;
	private int size = 128;
	private String path;
	private long len;
	private long offset;
	
	public void launchBenchmark(String[] args) throws Exception {
		String _bechmarkType = "";
		Runnable benchmarkTask = null;
		
		String[] _args = args;
		GetOpt go = new GetOpt(_args, "t:a:s:k:o:p:n:m:b:y:j:c");
		go.optErr = true;
		int ch = -1;
		while ((ch = go.getopt()) != GetOpt.optEOF) {
			if ((char) ch == 't') {
				_bechmarkType = go.optArgGet();
			} else if ((char) ch == 'a') {
				ipAddress = go.optArgGet();
			} else if ((char) ch == 's') {
				size = Integer.parseInt(go.optArgGet());
			} else if ((char) ch == 'k') {
				loop = Integer.parseInt(go.optArgGet());
			} else if ((char) ch == 'p') {
				path = go.optArgGet();
			} else if ((char) ch == 'b') {
				len = Long.parseLong(go.optArgGet());
			} else if ((char) ch == 'y') {
				offset = Long.parseLong(go.optArgGet());
			} else {
				System.exit(1); // undefined option
			}
		}
		
		if (_bechmarkType.startsWith("namenode-dump")) {
			LOG.info("starting namenode-dump");
			benchmarkTask = new NameNodeDump();				
		} else if (_bechmarkType.startsWith("directory-dump")) {
			LOG.info("starting directory-dump");
			benchmarkTask = new DirectoryDump(this);				
		} else if (_bechmarkType.startsWith("ping")) {
			benchmarkTask = new RpcPing();
		} else if (_bechmarkType.startsWith("location")) {
			benchmarkTask = new LocationTest(this);
		} else if (_bechmarkType.startsWith("statistics")) {
			benchmarkTask = new BlockStatistics(this);
		} else {
			System.out.println("No valid apptype, type " + _bechmarkType);
		}		
		
		benchmarkTask.run();
		System.exit(0);
	}
	
	public static void main(String[] args) throws Exception { 
		try {
			CrailFsck crailTest = new CrailFsck();
			crailTest.launchBenchmark(args);
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println(e.getCause().getMessage());
		}
	}	
	
	public String getIpAddress() {
		return ipAddress;
	}

	public int getSize() {
		return size;
	}

	public int getLoop() {
		return loop;
	}

	public String getPath() {
		return path;
	}

	public long getLen() {
		return len;
	}

	public long getOffset() {
		return offset;
	}	
}
