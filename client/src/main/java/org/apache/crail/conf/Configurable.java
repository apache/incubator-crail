package org.apache.crail.conf;

import org.slf4j.Logger;

public interface Configurable {
	public void init(CrailConfiguration conf, String[] args) throws Exception;
	public void printConf(Logger log);
}
