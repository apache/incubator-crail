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

import java.util.concurrent.ConcurrentHashMap;

import org.apache.crail.utils.CrailUtils;
import org.slf4j.Logger;

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
