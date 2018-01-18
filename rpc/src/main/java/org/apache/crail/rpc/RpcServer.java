package org.apache.crail.rpc;

import org.apache.crail.conf.Configurable;

public abstract class RpcServer implements Configurable {
	public abstract void run();
}
