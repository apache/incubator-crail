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
import com.ibm.crail.CrailFS;
import com.ibm.crail.conf.CrailConfiguration;
import com.ibm.crail.utils.CrailUtils;

public class NameNodeDump implements Runnable {
	public static final Logger LOG = CrailUtils.getLogger();
	
	public NameNodeDump() throws Exception {
	}
	
	@Override
	public void run() {
		try {
			CrailConfiguration conf = new CrailConfiguration();
			CrailFS dfs = CrailFS.newInstance(conf);
			dfs.dumpNameNode();
			dfs.close();
		} catch(Exception e){
			LOG.error(e.getMessage());
		}
	}
}
