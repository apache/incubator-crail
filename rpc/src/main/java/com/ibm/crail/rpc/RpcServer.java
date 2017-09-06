package com.ibm.crail.rpc;

import com.ibm.crail.conf.Configurable;

public abstract class RpcServer implements Configurable {
	public abstract void run();
}
