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

package com.ibm.crail.rpc;

import java.net.InetSocketAddress;
import org.slf4j.Logger;
import com.ibm.crail.conf.CrailConfiguration;

public interface RpcClient {
	public abstract void init(CrailConfiguration conf, String[] args) throws Exception;
	public abstract void printConf(Logger log);
	public RpcConnection connect(InetSocketAddress address)  throws Exception ;
	public void close();
	
	@SuppressWarnings("unchecked")
	public static RpcClient createInstance(String name) throws Exception {
		Class<?> nodeClass = Class.forName(name);
		if (RpcClient.class.isAssignableFrom(nodeClass)){
			Class<? extends RpcClient> clientClass = (Class<? extends RpcClient>) nodeClass;
			RpcClient clientInstance = clientClass.newInstance();
			return clientInstance;
		} else {
			throw new Exception("Cannot instantiate rpc client of type " + name);
		}
	}	
}
