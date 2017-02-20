package com.ibm.crail.core;

import java.util.concurrent.Future;
import com.ibm.crail.CrailResult;

public class CoreSyncOperation {
	private DirectoryOutputStream stream;
	private Future<CrailResult> future;	
	
	public CoreSyncOperation(DirectoryOutputStream stream, Future<CrailResult> future) {
		this.stream = stream;
		this.future = future;
	}

	public void close() throws Exception {
		future.get();
		stream.close();
	}
}
