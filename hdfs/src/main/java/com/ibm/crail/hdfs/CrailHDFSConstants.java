package com.ibm.crail.hdfs;

import java.io.IOException;

import org.slf4j.Logger;

import com.ibm.crail.conf.CrailConfiguration;

public class CrailHDFSConstants {
	public static final String STORAGE_AFFINITY_KEY = "crail.hdfs.storageaffinity";
	public static int STORAGE_AFFINITY = 1;
	
	public static final String LOCAL_AFFINITY_KEY = "crail.hdfs.localaffinity";
	public static boolean LOCAL_AFFINITY = false;

	public static void updateConstants(CrailConfiguration conf){
		if (conf.get(STORAGE_AFFINITY_KEY) != null) {
			STORAGE_AFFINITY = Integer.parseInt(conf.get(STORAGE_AFFINITY_KEY));
		}
		if (conf.get(LOCAL_AFFINITY_KEY) != null) {
			LOCAL_AFFINITY = Boolean.parseBoolean(conf.get(LOCAL_AFFINITY_KEY));
		}			
	}
	
	public static void verify() throws IOException {
	}

	public static void printConf(Logger logger) {
		logger.info(STORAGE_AFFINITY_KEY + " " + STORAGE_AFFINITY);
		logger.info(LOCAL_AFFINITY_KEY + " " + LOCAL_AFFINITY);
	}	
}
