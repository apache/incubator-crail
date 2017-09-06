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


public interface RpcBinding extends RpcClient {
	public RpcServer launchServer(RpcNameNodeService service);
	
	@SuppressWarnings("unchecked")
	public static RpcBinding createInstance(String name) throws Exception {
		Class<?> nodeClass = Class.forName(name);
		if (RpcBinding.class.isAssignableFrom(nodeClass)){
			Class<? extends RpcBinding> bindingClass = (Class<? extends RpcBinding>) nodeClass;
			RpcBinding bindingInstance = bindingClass.newInstance();
			return bindingInstance;
		} else {
			throw new Exception("Cannot instantiate datanode of type " + name);
		}
	}		
}
