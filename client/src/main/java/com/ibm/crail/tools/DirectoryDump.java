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
import com.ibm.crail.conf.CrailConfiguration;
import com.ibm.crail.conf.CrailConstants;
import com.ibm.crail.core.CoreFileSystem;
import com.ibm.crail.core.DirectoryRecord;
import com.ibm.crail.core.DirectoryRecordIterator;
import com.ibm.crail.namenode.protocol.FileName;
import com.ibm.crail.utils.CrailUtils;

public class DirectoryDump implements Runnable {
	private static final Logger LOG = CrailUtils.getLogger();
	private String path;
	
	public DirectoryDump(CrailFsck crailTest) throws Exception {
		this.path = crailTest.getPath();
	}	

	@Override
	public void run() {
		try {
			CrailConfiguration conf = new CrailConfiguration();
			CrailConstants.updateConstants(conf);
			CoreFileSystem fs = new CoreFileSystem(conf);		
			DirectoryRecordIterator iter = fs._listEntries(path);
			System.out.println("#hash   \t\tname\t\tfilecomponent");
			int i = 0;
			while(iter.hasNext()){
				DirectoryRecord record = iter.next();
				String path = CrailUtils.combinePath(record.getParent(), record.getFile());
				FileName filename = new FileName(path);
				System.out.format(i + ": " + "%08d\t\t%s\t%d\n", record.isValid() ? 1 : 0, padRight(record.getFile(), 8), filename.getFileComponent());
				i++;
			}
			fs.close();
		} catch(Exception e){
			LOG.error(e.getMessage());
		}
	}

	private String padRight(String s, int n) {
		return String.format("%1$-" + n + "s", s);
	}
}
