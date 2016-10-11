package com.ibm.crail;

public interface CrailNode {
	public CrailFS getFileSystem();
	public String getPath(); 
	public abstract CrailNode syncDir() throws Exception;
	public abstract long getModificationTime();
	public abstract long getCapacity();
	public abstract boolean isDir();
}
