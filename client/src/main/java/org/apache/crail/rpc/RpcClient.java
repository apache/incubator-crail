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

package org.apache.crail.rpc;

import java.net.InetSocketAddress;

import org.apache.crail.conf.Configurable;

public interface RpcClient extends Configurable {
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
