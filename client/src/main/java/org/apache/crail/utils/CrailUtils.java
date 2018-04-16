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

package org.apache.crail.utils;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.URI;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.crail.CrailLocationClass;
import org.apache.crail.conf.CrailConstants;
import org.apache.crail.metadata.DataNodeInfo;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;


public class CrailUtils {
	private static Logger LOG = CrailUtils.getLogger();
	
	public static synchronized Logger getLogger(){
		if (LOG == null){
			LOG = LoggerFactory.getLogger("org.apache.crail");
		}
		return LOG;
	}
	
	public static long getAddress(ByteBuffer buffer) {
		return ((sun.nio.ch.DirectBuffer) buffer).address();
	}	
	
	public static InetSocketAddress getNameNodeAddress() {
		StringTokenizer tupleTokenizer = new StringTokenizer(CrailConstants.NAMENODE_ADDRESS, ",");
		LinkedBlockingQueue<URI> namenodes = new LinkedBlockingQueue<URI>();
		while(tupleTokenizer.hasMoreTokens()){
			String address = tupleTokenizer.nextToken();
			URI uri = URI.create(address);
			namenodes.add(uri);
		}
		
		URI master = namenodes.poll();
		InetSocketAddress nnAddr = createSocketAddrForHost(master.getHost(), master.getPort());
		return nnAddr;
	}
	
	public static URI getPrimaryNameNode() {
		StringTokenizer tupleTokenizer = new StringTokenizer(CrailConstants.NAMENODE_ADDRESS, ",");
		LinkedBlockingQueue<URI> namenodes = new LinkedBlockingQueue<URI>();
		while(tupleTokenizer.hasMoreTokens()){
			String address = tupleTokenizer.nextToken();
			URI uri = URI.create(address);
			namenodes.add(uri);
		}
		
		URI master = namenodes.poll();
		return master;
	}	
	
	public static boolean verifyNamenode(String namenode) {
		StringTokenizer tupleTokenizer = new StringTokenizer(CrailConstants.NAMENODE_ADDRESS, ",");
		ConcurrentHashMap<String, Object> namenodes = new ConcurrentHashMap<String, Object>();
		while(tupleTokenizer.hasMoreTokens()){
			String address = tupleTokenizer.nextToken();
			URI uri = URI.create(address);
			String node = uri.getHost() + ":" + uri.getPort();
			namenodes.put(node, node);
		}		
		
		URI uri = URI.create(namenode);
		String node = uri.getHost() + ":" + uri.getPort();
		return namenodes.containsKey(node);
	}

	public static ConcurrentLinkedQueue<InetSocketAddress> getNameNodeList() {
		StringTokenizer tupleTokenizer = new StringTokenizer(CrailConstants.NAMENODE_ADDRESS, ",");
		ConcurrentLinkedQueue<InetSocketAddress> namenodes = new ConcurrentLinkedQueue<InetSocketAddress>();
		while(tupleTokenizer.hasMoreTokens()){
			String token = tupleTokenizer.nextToken();
			URI uri = URI.create(token);
			InetSocketAddress address = createSocketAddrForHost(uri.getHost(), uri.getPort());
			namenodes.add(address);
		}
		return namenodes;
	}
	
	public static long getServiceId(String namenode) {
		StringTokenizer tupleTokenizer = new StringTokenizer(CrailConstants.NAMENODE_ADDRESS, ",");
		ConcurrentHashMap<String, Long> namenodes = new ConcurrentHashMap<String, Long>();
		long serviceId = 0;
		while(tupleTokenizer.hasMoreTokens()){
			String address = tupleTokenizer.nextToken();
			URI uri = URI.create(address);
			String node = uri.getHost() + ":" + uri.getPort();
			namenodes.put(node, serviceId++);
		}	
		
		URI uri = URI.create(namenode);
		String node = uri.getHost() + ":" + uri.getPort();		
		long id = namenodes.get(node);
		return id;
	}

	public static long getServiceSize() {
		StringTokenizer tupleTokenizer = new StringTokenizer(CrailConstants.NAMENODE_ADDRESS, ",");
		return tupleTokenizer.countTokens();
	}	

	public static final long blockStartAddress(long offset) {
		long blockCount = offset / CrailConstants.BLOCK_SIZE;
		return blockCount*CrailConstants.BLOCK_SIZE;
	}
	
	public static final long bufferStartAddress(long position, long sliceSize) {
		long blockCount = position / sliceSize;
		return blockCount*sliceSize;
	}	
	
	public static final long nextBlockAddress(long offset){
		if ((offset % CrailConstants.BLOCK_SIZE) == 0){
			return offset;
		} else {
			return blockStartAddress(offset) + CrailConstants.BLOCK_SIZE;
		}
	}

	public static final int minFileBuf(long fileSize, int bufSize) {
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
	
	public static CrailLocationClass getLocationClass() throws UnknownHostException{
		return CrailLocationClass.get(InetAddress.getLocalHost().getCanonicalHostName().hashCode());
	}
	
	public static void parseMap(String config, ConcurrentHashMap<String, String> map) throws Exception {
		StringTokenizer tupleTokenizer = new StringTokenizer(config, "/");
		while(tupleTokenizer.hasMoreTokens()){
			String tuple = tupleTokenizer.nextToken();
			StringTokenizer commaTokenizer = new StringTokenizer(tuple, ",");
			if (commaTokenizer.countTokens() != 2){
				throw new Exception("parsing Map, wrong format!");
			}
			String key = commaTokenizer.nextToken();
			String value = commaTokenizer.nextToken();
			map.put(key, value);
		}
	}

	public static int getStorageClasses(String storageTypes) {
		StringTokenizer tokenizer = new StringTokenizer(storageTypes, ",");
		return tokenizer.countTokens();
	}
	
	public static String getIPAddressFromBytes(byte[] bytes){
		String address = "/unresolved";
		try {
			address = InetAddress.getByAddress(bytes).toString();
		} catch(Exception e){
		}
		return address;
	}

}
