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

package org.apache.crail.core;

import org.apache.crail.CrailBlockLocation;

class CoreBlockLocation implements CrailBlockLocation {
	private String[] names;
	private String[] hosts;
	private String[] topology;
	private int[] storageTypes;
	private int[] storageClasses;
	private int[] locationAffinities;
	private long offset; 
	private long length;

	public CoreBlockLocation() {
	}

	public long getOffset() {
		return offset;
	}

	public void setOffset(long offset) {
		this.offset = offset;
	}
	
	public void setLength(long length) {
		this.length = length;
	}
	
	public long getLength(){
		return length;
	}

	public String[] getNames() {
		return names;
	}

	public void setNames(String[] names) {
		this.names = names;
	}

	public String[] getHosts() {
		return hosts;
	}

	public void setHosts(String[] hosts) {
		this.hosts = hosts;
	}

	public String[] getTopology() {
		return topology;
	}

	public void setTopologyPaths(String[] topology) {
		this.topology = topology;
	}

	public int[] getStorageTypes() {
		return storageTypes;
	}

	public void setStorageTypes(int[] storageTypes) {
		this.storageTypes = storageTypes;
	}
	
	public int[] getStorageClasses() {
		return storageClasses;
	}

	public void setStorageClasses(int[] storageClasses) {
		this.storageClasses = storageClasses;
	}	

	public int[] getLocationAffinities() {
		return locationAffinities;
	}

	public void setLocationAffinities(int[] locationTiers) {
		this.locationAffinities = locationTiers;
	}
	
	  public String toString() {
		    StringBuilder result = new StringBuilder();
		    result.append(offset);
		    result.append(',');
		    result.append(length);
		    for(String h: hosts) {
		      result.append(',');
		      result.append(h);
		    }
		    for (int i = 0; i < storageTypes.length; i++){
		    	result.append(',');
		    	result.append(storageTypes[i]);
		    }
		    for (int i = 0; i < storageClasses.length; i++){
		    	result.append(',');
		    	result.append(storageClasses[i]);
		    }		    
		    for (int i = 0; i < locationAffinities.length; i++){
		    	result.append(',');
		    	result.append(locationAffinities[i]);
		    }		    
		    return result.toString();
		  }	

}