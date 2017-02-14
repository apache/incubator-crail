package com.ibm.crail.utils;

import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;

public class Statistics {
	private static final Logger LOG = CrailUtils.getLogger();
	private ConcurrentHashMap<String, StatisticsProvider> statistics;
	
	public Statistics(){
		statistics = new ConcurrentHashMap<String, StatisticsProvider>();
	}
	
	public void addProvider(StatisticsProvider provider){
		StatisticsProvider old = statistics.putIfAbsent(provider.providerName(), provider);
		if (old != null){
			old.merge(provider);
		}
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
		public void merge(StatisticsProvider provider);
		public void printStatistics();
		public void reset();
	}
}
