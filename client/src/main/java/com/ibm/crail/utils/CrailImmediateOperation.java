package com.ibm.crail.utils;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.ibm.crail.CrailResult;

public class CrailImmediateOperation implements Future<CrailResult>, CrailResult{
	private long len;
	
	public CrailImmediateOperation(int len){
		this.len = len;
	}

	@Override
	public boolean cancel(boolean mayInterruptIfRunning) {
		return false;
	}

	@Override
	public boolean isCancelled() {
		return false;
	}

	@Override
	public boolean isDone() {
		return true;
	}

	@Override
	public CrailResult get() throws InterruptedException, ExecutionException {
		return this;
	}

	@Override
	public CrailResult get(long timeout, TimeUnit unit)
			throws InterruptedException, ExecutionException, TimeoutException {
		return this;
	}

	@Override
	public long getLen() {
		return len;
	}
}

