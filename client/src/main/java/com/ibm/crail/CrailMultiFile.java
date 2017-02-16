package com.ibm.crail;

import java.util.Iterator;

public interface CrailMultiFile extends CrailNode {
	public abstract int files();
	public abstract Iterator<String> listEntries() throws Exception;
	
	default CrailMultiStream getMultiStream(int outstanding) throws Exception{
		return new CrailMultiStream(this.getFileSystem(), listEntries(), outstanding, files());
	}
}
