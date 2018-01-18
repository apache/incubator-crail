package org.apache.crail;

import java.util.Iterator;

public interface CrailContainer extends CrailNode {
	public abstract int files();
	public abstract Iterator<String> listEntries() throws Exception;
}
