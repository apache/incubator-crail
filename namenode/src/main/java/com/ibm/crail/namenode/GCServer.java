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

package com.ibm.crail.namenode;

import java.util.Iterator;
import java.util.concurrent.DelayQueue;

import org.slf4j.Logger;

import com.ibm.crail.utils.CrailUtils;

public class GCServer implements Runnable {
	private static final Logger LOG = CrailUtils.getLogger();
	
	private NameNodeService rpcService;
	private DelayQueue<AbstractNode> deleteQueue;
	
	public GCServer(NameNodeService service, DelayQueue<AbstractNode> deleteQueue){
		this.rpcService = service;
		this.deleteQueue = deleteQueue;
	}

	@Override
	public void run() {
		while(true){
			try{
				AbstractNode file = deleteQueue.take();
//				LOG.info("GC: removing deleted file to queue, fd " + file.getFd() + ", queue size " + deleteQueue.size());
				Iterator<AbstractNode> iter = file.childIterator();
				while(iter.hasNext()){
					AbstractNode child = iter.next();
					deleteQueue.add(child);
//					LOG.info("GC: adding deleted child to queue, fd " + child.getFd() + ", queue size " + deleteQueue.size());
				}
				rpcService.freeFile(file);
			} catch(Exception e){
				LOG.info("Exception during GC: " + e.getMessage());
			}
		}
	}

}
