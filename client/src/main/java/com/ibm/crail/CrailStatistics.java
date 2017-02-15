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
		StatisticsProvider existing = statistics.putIfAbsent(provider.providerName(), provider);
		if (existing != null){
			existing.mergeStatistics(provider);
		}
	}
	
	public void print(String tag){
		LOG.info("CrailStatistics, tag=" + tag);
		for (StatisticsProvider provider : statistics.values()){
			LOG.info("provider=" + provider.providerName() + " [" + provider.printStatistics() + "]");
		}
	}
	
	public void reset(){
		for (StatisticsProvider provider : statistics.values()){
			provider.resetStatistics();
		}
	}	
	
	public static interface StatisticsProvider {
		public String providerName();
		public String printStatistics();
		public void mergeStatistics(StatisticsProvider provider);
		public void resetStatistics();
	}
}
