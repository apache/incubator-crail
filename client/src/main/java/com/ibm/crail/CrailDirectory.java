package com.ibm.crail;

import java.util.Iterator;

public abstract class CrailDirectory implements CrailNode {
	public abstract int files();
	public abstract Iterator<String> listEntries(String name) throws Exception;
	public abstract CrailDirectory syncDir() throws Exception;
	public abstract void close() throws Exception;
	
	public CrailMultiStream getMultiStream(int outstanding) throws Exception{
		return new CrailMultiStream(this.getFileSystem(), listEntries(this.getPath()), outstanding);
	}	
}