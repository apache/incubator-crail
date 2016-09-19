/*
 * Crail: A Multi-tiered Distributed Direct Access File System
 *
 * Author: Patrick Stuedi <stu@zurich.ibm.com>
 *
 * Copyright (C) 2016, IBM Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.ibm.crail.core;

import java.util.concurrent.atomic.AtomicLong;

public class CoreStreamStatistics {
	private AtomicLong open;
	private AtomicLong openInput;
	private AtomicLong openOutput;
	private AtomicLong openInputDir;
	private AtomicLong openOutputDir;
	private AtomicLong close;
	private AtomicLong closeInput;
	private AtomicLong closeOutput;
	private AtomicLong closeInputDir;
	private AtomicLong closeOutputDir;	
	private AtomicLong currentInput;
	private AtomicLong currentOutput;
	private AtomicLong maxInput;
	private AtomicLong maxOutput;
	
	public CoreStreamStatistics(){
		reset();
	}
	
	public void reset(){
		this.open = new AtomicLong(0);
		this.openInput = new AtomicLong(0);
		this.openOutput = new AtomicLong(0);
		this.openInputDir = new AtomicLong(0);
		this.openOutputDir = new AtomicLong(0);
		this.close = new AtomicLong(0);
		this.closeInput = new AtomicLong(0);
		this.closeOutput = new AtomicLong(0);
		this.closeInputDir = new AtomicLong(0);
		this.closeOutputDir = new AtomicLong(0);
		this.currentInput = new AtomicLong(0);
		this.currentOutput = new AtomicLong(0);
		this.maxInput = new AtomicLong(0);
		this.maxOutput = new AtomicLong(0);
	}

	public void incOpen() {
		this.open.incrementAndGet();
	}

	public void incOpenInput() {
		this.openInput.incrementAndGet();
	}

	public void incOpenOutput() {
		this.openOutput.incrementAndGet();
	}

	public void incOpenInputDir() {
		this.openInputDir.incrementAndGet();
	}

	public void incOpenOutputDir() {
		this.openOutputDir.incrementAndGet();
	}

	public void incClose() {
		this.close.incrementAndGet();
	}

	public void incCloseInput() {
		this.closeInput.incrementAndGet();
	}

	public void incCloseOutput() {
		this.closeOutput.incrementAndGet();
	}

	public void incCloseInputDir() {
		this.closeInputDir.incrementAndGet();
	}

	public void incCloseOutputDir() {
		this.closeOutputDir.incrementAndGet();
	}

	public void incCurrentInput() {
		this.currentInput.incrementAndGet();
	}
	
	public void incCurrentOutput() {
		this.currentOutput.incrementAndGet();
	}	
	
	public void decCurrentInput() {
		this.currentInput.decrementAndGet();
	}	
	
	public void decCurrentOutput() {
		this.currentOutput.decrementAndGet();
	}		

	public void incMaxInput() {
		maxInput.updateAndGet(x -> Math.max(x, currentInput.get()));
	}
	
	public void incMaxOutput() {
		maxOutput.updateAndGet(x -> Math.max(x, currentOutput.get()));
	}	

	public long getOpen() {
		return open.get();
	}

	public long getOpenInput() {
		return openInput.get();
	}

	public long getOpenOutput() {
		return openOutput.get();
	}

	public long getOpenInputDir() {
		return openInputDir.get();
	}

	public long getOpenOutputDir() {
		return openOutputDir.get();
	}

	public long getClose() {
		return close.get();
	}

	public long getCloseInput() {
		return closeInput.get();
	}

	public long getCloseOutput() {
		return closeOutput.get();
	}

	public long getCloseInputDir() {
		return closeInputDir.get();
	}

	public long getCloseOutputDir() {
		return closeOutputDir.get();
	}

	public long getCurrentInput() {
		return currentInput.get();
	}
	
	public long getCurrentOutput() {
		return currentOutput.get();
	}	

	public long getMaxInput() {
		return maxInput.get();
	}
	
	public long getMaxOutput() {
		return maxOutput.get();
	}	
	
	
}
