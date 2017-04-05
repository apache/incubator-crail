package com.ibm.crail.storage;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.slf4j.Logger;

import com.ibm.crail.conf.CrailConfiguration;

public interface StorageClient {
	public abstract void init(CrailConfiguration conf, String[] args) throws IOException;
	public abstract StorageEndpoint createEndpoint(InetSocketAddress inetAddress) throws IOException;	
	public abstract void close() throws Exception;
	public abstract void printConf(Logger log);
	
	@SuppressWarnings("unchecked")
	public static StorageClient createInstance(String name) throws Exception {
		Class<?> nodeClass = Class.forName(name);
		if (StorageClient.class.isAssignableFrom(nodeClass)){
			Class<? extends StorageClient> storageClientClass = (Class<? extends StorageClient>) nodeClass;
			StorageClient client = storageClientClass.newInstance();
			return client;
		} else {
			throw new Exception("Cannot instantiate storage client of type " + name);
		}
		
	}	
}
