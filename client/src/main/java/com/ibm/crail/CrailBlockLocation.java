package com.ibm.crail;

public interface CrailBlockLocation {
	public long getOffset();
	public long getLength();
	public String[] getNames();
	public String[] getHosts();
	public String[] getTopology();
	public int[] getStorageTiers();
	public int[] getLocationAffinities();
}
