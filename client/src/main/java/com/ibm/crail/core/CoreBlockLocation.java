package com.ibm.crail.core;

import com.ibm.crail.CrailBlockLocation;

public class CoreBlockLocation implements CrailBlockLocation {
	private String[] names;
	private String[] hosts;
	private String[] topology;
	private int[] storageTiers;
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

	public int[] getStorageTiers() {
		return storageTiers;
	}

	public void setStorageTiers(int[] storageTiers) {
		this.storageTiers = storageTiers;
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
		    for (int i = 0; i < storageTiers.length; i++){
		    	result.append(',');
		    	result.append(storageTiers[i]);
		    }
		    for (int i = 0; i < locationAffinities.length; i++){
		    	result.append(',');
		    	result.append(locationAffinities[i]);
		    }		    
		    return result.toString();
		  }	

}