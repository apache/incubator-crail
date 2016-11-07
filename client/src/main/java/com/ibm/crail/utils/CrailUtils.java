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

package com.ibm.crail.utils;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.URI;
import java.net.UnknownHostException;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import com.ibm.crail.conf.CrailConstants;
import com.ibm.crail.namenode.protocol.DataNodeInfo;


public class CrailUtils {
	private static Logger LOG = CrailUtils.getLogger();
	
	public static synchronized Logger getLogger(){
		if (LOG == null){
			LOG = LoggerFactory.getLogger("com.ibm.crail");
		}
		return LOG;
	}
	
	public static InetSocketAddress getNameNodeAddress() {
		URI uri = URI.create(CrailConstants.NAMENODE_ADDRESS);
		InetSocketAddress nnAddr = createSocketAddrForHost(uri.getHost(), uri.getPort());
		return nnAddr;
	}
	
	public static long blockStartAddress(long offset) {
		long blockCount = offset / CrailConstants.BLOCK_SIZE;
		return blockCount*CrailConstants.BLOCK_SIZE;
	}
	
	public static long nextBlockAddress(long offset){
		if ((offset % CrailConstants.BLOCK_SIZE) == 0){
			return offset;
		} else {
			return blockStartAddress(offset) + CrailConstants.BLOCK_SIZE;
		}
	}

	public static int minFileBuf(long fileSize, int bufSize) {
		int fileLeftOver = Integer.MAX_VALUE;
		
		long _maxInt = (long) Integer.MAX_VALUE;
		if (fileSize < _maxInt){
			fileLeftOver = (int) fileSize;
		}		
		
		return Math.min(bufSize, fileLeftOver);
	}
	
	public static String getCacheDirectory(String id){
		return CrailConstants.CACHE_PATH + "/" + id;
	}	
	
	public static void printStackTrace(){
		StackTraceElement[] elements = Thread.currentThread().getStackTrace();
		for (StackTraceElement e : elements) {
			LOG.info(e.toString());
		}		
	}
	
	public static String getName(String path){
		if (path.equalsIgnoreCase("/")){
			return "";
		}
		if (path.endsWith("/")){
			int termSlash = path.lastIndexOf('/');
			path = path.substring(0, termSlash);
		}
		
		int lastSlash = path.lastIndexOf('/');
		if (lastSlash == -1) {
			return path;
		} else if (lastSlash == 0) {
			String name = path.substring(1);
			return name;
		} else {
			String name = path.substring(lastSlash+1);
			return name;
		}		
	}
	
	public static String getParent(String path){
		if (path.equalsIgnoreCase("/")){
			return null;
		}	
		if (path.endsWith("/")){
			int termSlash = path.lastIndexOf('/');
			path = path.substring(0, termSlash);
		}		
		
		int lastSlash = path.lastIndexOf('/');
		if (lastSlash == -1) {
			return path;
		} else if (lastSlash == 0) {
			return "/";
		} else {
			String parent = path.substring(0, lastSlash);
			return parent;
		}
	}
	
	public static String combinePath(String parent, String name){
		if (parent.endsWith("/")){
			return parent + name;
		} else {
			return parent + "/" + name;
		}
	}

	public static int computeIndex(long offset) {
		long index = offset / CrailConstants.BLOCK_SIZE;
		return (int) index;
	}
	
	public static boolean isLocalAddress(InetAddress addr) {
	    // Check if the address is a valid special local or loop back
	    if (addr.isAnyLocalAddress() || addr.isLoopbackAddress())
	        return true;

	    // Check if the address is defined on any interface
	    try {
	        return NetworkInterface.getByInetAddress(addr) != null;
	    } catch (SocketException e) {
	        return false;
	    }
	}
	
	public static InetSocketAddress createSocketAddrForHost(String host, int port) {
		return new InetSocketAddress(host, port);
	}
	
	public static InetSocketAddress datanodeInfo2SocketAddr(DataNodeInfo dnInfo) throws UnknownHostException{
		return new InetSocketAddress(InetAddress.getByAddress(dnInfo.getIpAddress()), dnInfo.getPort());
	}	

}
