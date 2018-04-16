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

package org.apache.crail.namenode;

import java.util.concurrent.DelayQueue;

import org.apache.crail.utils.CrailUtils;
import org.slf4j.Logger;

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
				if (file.getType().isContainer()){
					file.clearChildren(deleteQueue);
				}
				rpcService.freeFile(file);
			} catch(Exception e){
				LOG.info("Exception during GC: " + e.getMessage());
			}
		}
	}

}
