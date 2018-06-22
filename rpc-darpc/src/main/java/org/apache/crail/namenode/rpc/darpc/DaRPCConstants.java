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

package org.apache.crail.namenode.rpc.darpc;

import java.io.IOException;

import org.apache.crail.conf.CrailConfiguration;
import org.apache.crail.utils.CrailUtils;
import org.slf4j.Logger;

public class DaRPCConstants {
	private static final Logger LOG = CrailUtils.getLogger();
	
	public static final String NAMENODE_DARPC_POLLING_KEY = "crail.namenode.darpc.polling";
	public static boolean NAMENODE_DARPC_POLLING = false;
	
	public static final String NAMENODE_DARPC_TYPE_KEY = "crail.namenode.darpc.type";
	public static String NAMENODE_DARPC_TYPE = "passive";	
	
	public static final String NAMENODE_DARPC_AFFINITY_KEY = "crail.namenode.darpc.affinity";
	public static String NAMENODE_DARPC_AFFINITY = "1";	
	
	public static final String NAMENODE_DARPC_MAXINLINE_KEY = "crail.namenode.darpc.maxinline";
	public static int NAMENODE_DARPC_MAXINLINE = 0;

	public static final String NAMENODE_DARPC_RECVQUEUE_KEY = "crail.namenode.darpc.recvQueue";
	public static int NAMENODE_DARPC_RECVQUEUE = 32;
	
	public static final String NAMENODE_DARPC_SENDQUEUE_KEY = "crail.namenode.darpc.sendQueue";
	public static int NAMENODE_DARPC_SENDQUEUE = 32;		
	
	public static final String NAMENODE_DARPC_POLLSIZE_KEY = "crail.namenode.darpc.pollsize";
	public static int NAMENODE_DARPC_POLLSIZE = NAMENODE_DARPC_RECVQUEUE;		
	
	public static final String NAMENODE_DARPC_CLUSTERSIZE_KEY = "crail.namenode.darpc.clustersize";
	public static int NAMENODE_DARPC_CLUSTERSIZE = 128;	
	
	public static final String NAMENODE_DARPC_BACKLOG_KEY = "crail.namenode.darpc.backlog";
	public static int NAMENODE_DARPC_BACKLOG = 100;	
	
	public static final String NAMENODE_DARPC_CONNECTTIMEOUT_KEY = "crail.namenode.darpc.connecttimeout";
	public static int NAMENODE_DARPC_CONNECTTIMEOUT = 1000;		
	
	public static void updateConstants(CrailConfiguration conf){
		if (conf.get(NAMENODE_DARPC_POLLING_KEY) != null) {
			NAMENODE_DARPC_POLLING = conf.getBoolean(NAMENODE_DARPC_POLLING_KEY, false);
		}			
		if (conf.get(NAMENODE_DARPC_TYPE_KEY) != null) {
			NAMENODE_DARPC_TYPE = conf.get(NAMENODE_DARPC_TYPE_KEY);
		}	
		if (conf.get(NAMENODE_DARPC_AFFINITY_KEY) != null) {
			NAMENODE_DARPC_AFFINITY = conf.get(NAMENODE_DARPC_AFFINITY_KEY);
		}	
		if (conf.get(NAMENODE_DARPC_MAXINLINE_KEY) != null) {
			NAMENODE_DARPC_MAXINLINE = Integer.parseInt(conf.get(NAMENODE_DARPC_MAXINLINE_KEY));
		}	
		if (conf.get(NAMENODE_DARPC_RECVQUEUE_KEY) != null) {
			NAMENODE_DARPC_RECVQUEUE = Integer.parseInt(conf.get(NAMENODE_DARPC_RECVQUEUE_KEY));
		}
		if (conf.get(NAMENODE_DARPC_SENDQUEUE_KEY) != null) {
			NAMENODE_DARPC_SENDQUEUE = Integer.parseInt(conf.get(NAMENODE_DARPC_SENDQUEUE_KEY));
		}		
		if (conf.get(NAMENODE_DARPC_POLLSIZE_KEY) != null) {
			NAMENODE_DARPC_POLLSIZE = Integer.parseInt(conf.get(NAMENODE_DARPC_POLLSIZE_KEY));
		}	
		if (conf.get(NAMENODE_DARPC_CLUSTERSIZE_KEY) != null) {
			NAMENODE_DARPC_CLUSTERSIZE = Integer.parseInt(conf.get(NAMENODE_DARPC_CLUSTERSIZE_KEY));
		}	
		if (conf.get(NAMENODE_DARPC_BACKLOG_KEY) != null) {
			NAMENODE_DARPC_BACKLOG = Integer.parseInt(conf.get(NAMENODE_DARPC_BACKLOG_KEY));
		}	
		if (conf.get(NAMENODE_DARPC_CONNECTTIMEOUT_KEY) != null) {
			NAMENODE_DARPC_CONNECTTIMEOUT = Integer.parseInt(conf.get(NAMENODE_DARPC_CONNECTTIMEOUT_KEY));
		}			
	}
	
	public static void verify() throws IOException {
		if (!DaRPCConstants.NAMENODE_DARPC_TYPE.equalsIgnoreCase("passive") && !DaRPCConstants.NAMENODE_DARPC_TYPE.equalsIgnoreCase("active")){
			throw new IOException("crail.namenode.darpc.type must be either <active> or <passive>, found " + DaRPCConstants.NAMENODE_DARPC_TYPE);
		}		
	}

	public static void printConf(Logger logger) {
		LOG.info(NAMENODE_DARPC_POLLING_KEY + " " + NAMENODE_DARPC_POLLING);
		LOG.info(NAMENODE_DARPC_TYPE_KEY + " " + NAMENODE_DARPC_TYPE);
		LOG.info(NAMENODE_DARPC_AFFINITY_KEY + " " + NAMENODE_DARPC_AFFINITY);
		LOG.info(NAMENODE_DARPC_MAXINLINE_KEY + " " + NAMENODE_DARPC_MAXINLINE);
		LOG.info(NAMENODE_DARPC_RECVQUEUE_KEY + " " + NAMENODE_DARPC_RECVQUEUE);
		LOG.info(NAMENODE_DARPC_SENDQUEUE_KEY + " " + NAMENODE_DARPC_SENDQUEUE);
		LOG.info(NAMENODE_DARPC_POLLSIZE_KEY + " " + NAMENODE_DARPC_POLLSIZE);
		LOG.info(NAMENODE_DARPC_CLUSTERSIZE_KEY + " " + NAMENODE_DARPC_CLUSTERSIZE);
		LOG.info(NAMENODE_DARPC_BACKLOG_KEY + " " + NAMENODE_DARPC_BACKLOG);
		LOG.info(NAMENODE_DARPC_CONNECTTIMEOUT_KEY + " " + NAMENODE_DARPC_CONNECTTIMEOUT);
	}	
}
