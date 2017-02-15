package com.ibm.crail;

import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;

import com.ibm.crail.utils.CrailUtils;

public class CrailStatistics {
	private static final Logger LOG = CrailUtils.getLogger();
	private ConcurrentHashMap<String, StatisticsProvider> statistics;
	
	public CrailStatistics(){
		statistics = new ConcurrentHashMap<String, StatisticsProvider>();
	}
	
	public void addProvider(StatisticsProvider provider){
		StatisticsProvider old = statistics.putIfAbsent(provider.providerName(), provider);
	}
	
	public void printStatistics(String message){
		LOG.info("CoreFileSystem statistics, " + message);
		for (StatisticsProvider provider : statistics.values()){
			provider.printStatistics();
		}
	}
	
	public void resetStatistics(){
		for (StatisticsProvider provider : statistics.values()){
			provider.reset();
		}
	}	
	
	public static interface StatisticsProvider {
		public String providerName();
		public String printStatistics();
		public void reset();
	}
}
