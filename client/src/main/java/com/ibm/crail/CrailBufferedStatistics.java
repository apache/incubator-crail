package com.ibm.crail;

import java.util.concurrent.atomic.AtomicLong;
import com.ibm.crail.CrailStatistics.StatisticsProvider;

class CrailBufferedStatistics implements StatisticsProvider {
	private String type;
	private AtomicLong totalOps;
	private AtomicLong blockingOps;
	private AtomicLong nonBlockingOps;
	
	public CrailBufferedStatistics(String type){
		this.type = type;
		this.totalOps = new AtomicLong(0);
		this.blockingOps = new AtomicLong(0);
		this.nonBlockingOps = new AtomicLong(0);
	}
	
	public void mergeStatistics(StatisticsProvider provider){
		if (provider instanceof CrailBufferedStatistics){
			CrailBufferedStatistics newProvider = (CrailBufferedStatistics) provider;
			this.totalOps.addAndGet(newProvider.getTotalOps());
			this.blockingOps.addAndGet(newProvider.getBlockingOps());
			this.nonBlockingOps.addAndGet(newProvider.getNonBlockingOps());
		}
	}
	
	@Override
	public String providerName() {
		return type;
	}

	@Override
	public String printStatistics() {
		return "totalOps " + getTotalOps() + ", blockingOps " + getBlockingOps() + ", nonBlockingOps " + getNonBlockingOps();
	}

	@Override
	public void resetStatistics() {
		this.totalOps.set(0);
		this.blockingOps.set(0);
		this.nonBlockingOps.set(0);
	}
	
	public void incTotalOps(){
		this.totalOps.incrementAndGet();
	}
	
	public void incBlockingOps(){
		this.blockingOps.incrementAndGet();
	}
	
	public void incNonBlockingOps(){
		this.nonBlockingOps.incrementAndGet();
	}
	
	public long getTotalOps(){
		return totalOps.get();
	}
	
	public long getBlockingOps(){
		return blockingOps.get();
	}
	
	public long getNonBlockingOps(){
		return nonBlockingOps.get();
	}
}
