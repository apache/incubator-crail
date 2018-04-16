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

package org.apache.crail;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.crail.CrailStatistics.StatisticsProvider;

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
