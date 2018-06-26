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

package org.apache.crail.storage;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.util.List;

public class StorageUtils {
	public static InetSocketAddress getDataNodeAddress(String ifname, int port) throws IOException {
		NetworkInterface netif = NetworkInterface.getByName(ifname);
		if (netif == null){
			throw new IOException("Cannot find network interface with name " + ifname);
		}
		List<InterfaceAddress> addresses = netif.getInterfaceAddresses();
		InetAddress addr = null;
		for (InterfaceAddress address: addresses){
			if (address.getBroadcast() != null){
				InetAddress _addr = address.getAddress();
				addr = _addr;
			}
		}

		if (addr == null){
			throw new IOException("Network interface with name " + ifname + " has no valid IP address");
		}
		InetSocketAddress inetAddr = new InetSocketAddress(addr, port);
		return inetAddr;
	}
	
	public static void clean(String base, String path) throws IOException {
		try {
			File dataDir = new File(path);
			if (!dataDir.exists()){
				if (!dataDir.mkdirs()) {
					throw new IOException("crail.datapath " + base + " either does not exist or has no write permissions");
				}
			}
			for (File child : dataDir.listFiles()) {
				child.delete();
			}
		} catch(SecurityException e) {
			throw new IOException("Error when trying to access " + base, e);
		}
	}
	
	public static String getDatanodeDirectory(String datapath, InetSocketAddress address) throws IllegalArgumentException {
		if (address == null) {
			throw new IllegalArgumentException("Address paramater cannot be null!");
		}
		return datapath + address.getAddress() + "-"  + address.getPort();
	}	
}
