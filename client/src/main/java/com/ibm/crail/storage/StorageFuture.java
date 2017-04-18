package com.ibm.crail.storage;

import java.util.concurrent.Future;

public interface StorageFuture extends Future<StorageResult> {
	public boolean isSynchronous();
}
