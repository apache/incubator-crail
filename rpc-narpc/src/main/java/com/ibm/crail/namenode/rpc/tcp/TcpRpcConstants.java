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

package com.ibm.crail.namenode.rpc.tcp;

import java.io.IOException;

import org.slf4j.Logger;

import com.ibm.crail.conf.CrailConfiguration;
import com.ibm.crail.utils.CrailUtils;

public class TcpRpcConstants {
	private static final Logger LOG = CrailUtils.getLogger();
	
	public static final String NAMENODE_TCP_QUEUEDEPTH_KEY = "crail.namenode.tcp.queueDepth";
	public static int NAMENODE_TCP_QUEUEDEPTH = 32;
	
	public static final String NAMENODE_TCP_MESSAGESIZE_KEY = "crail.namenode.tcp.messageSize";
	public static int NAMENODE_TCP_MESSAGESIZE = 512;	
	
	public static void updateConstants(CrailConfiguration conf){
		if (conf.get(NAMENODE_TCP_QUEUEDEPTH_KEY) != null) {
			NAMENODE_TCP_QUEUEDEPTH = Integer.parseInt(conf.get(NAMENODE_TCP_QUEUEDEPTH_KEY));
		}
		if (conf.get(NAMENODE_TCP_MESSAGESIZE_KEY) != null) {
			NAMENODE_TCP_MESSAGESIZE = Integer.parseInt(conf.get(NAMENODE_TCP_MESSAGESIZE_KEY));
		}		
	}
	
	public static void verify() throws IOException {
	}

	public static void printConf(Logger logger) {
		LOG.info(NAMENODE_TCP_QUEUEDEPTH_KEY + " " + NAMENODE_TCP_QUEUEDEPTH);
		LOG.info(NAMENODE_TCP_MESSAGESIZE_KEY + " " + NAMENODE_TCP_MESSAGESIZE);
	}	
}
