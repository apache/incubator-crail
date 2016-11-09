package com.ibm.crail;

import java.util.concurrent.Future;

public interface Upcoming<T> extends Future<T> {
	T early() throws Exception;
}
