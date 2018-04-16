/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.crail.core;

import java.io.IOException;
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.crail.CrailBuffer;
import org.apache.crail.CrailResult;
import org.apache.crail.conf.CrailConstants;
import org.apache.crail.utils.CrailUtils;
import org.slf4j.Logger;


public class DirectoryInputStream implements Iterator<String> {
	private static final Logger LOG = CrailUtils.getLogger();
	
	private CoreInputStream stream;
	private CrailBuffer internalBuf;	
	private CoreDataStore fs;
	private String parent;
	private String currentFile;
	private int consumedRecords;
	private int availableRecords;
	private int[] blockTickets;
	private Random random;
	private boolean randomize;
	private boolean open;
	
	public DirectoryInputStream(CoreInputStream stream, boolean randomize) throws Exception {
		this.stream = stream;
		this.randomize = randomize;
		this.fs = stream.getFile().getFileSystem();
		this.parent = stream.getFile().getPath();
		this.internalBuf = fs.allocateBuffer();
		this.internalBuf.clear();
		this.internalBuf.position(this.internalBuf.capacity());
		this.currentFile = null;
		this.availableRecords = 0;
		this.consumedRecords = 0;
		this.random = new Random();
		this.blockTickets = new int[CrailConstants.BUFFER_SIZE/CrailConstants.DIRECTORY_RECORD];
		for (int i = 0; i < blockTickets.length; i++){
			blockTickets[i] = i;
		}
		this.open = true;
	}
	
	public boolean hasNext() {
		if (!open) { 
			return false;
		} 		
		if (currentFile != null){
			return true;
		}
		while(hasRecord()){
			DirectoryRecord record = nextRecord();
			if (record.isValid()){
				currentFile = CrailUtils.combinePath(record.getParent(), record.getFile());
				break;
			}
		}
		return currentFile != null;
	}
	
	public String next(){
		if (!open) { 
			return null;
		} 		
		
		String ret = currentFile;
		currentFile = null;
		return ret;
	}
	
	public boolean hasRecord() {
		if (!open) { 
			return false;
		} 		
		if (fetchIfEmpty() > 0){
			return true;
		}
		try {
			close();
		} catch(Exception e){
			LOG.info("error when closing directory stream " + e.getMessage());
		}
		return false;
	}

	public DirectoryRecord nextRecord() {
		if (!open) { 
			return null;
		} 		
		
		DirectoryRecord record = new DirectoryRecord(parent);
		int offset = blockTickets[consumedRecords]*CrailConstants.DIRECTORY_RECORD;
		internalBuf.position(offset);
		record.update(internalBuf);
		consumedRecords++;
		return record;
	}	
	
	private int fetchIfEmpty() {
		try {
			if (consumedRecords == availableRecords){
				internalBuf.clear();
				Future<CrailResult> future = stream.read(internalBuf);
				if (future != null){
					long ret = future.get(CrailConstants.DATA_TIMEOUT, TimeUnit.MILLISECONDS).getLen();
					if (ret > 0){
						internalBuf.flip();
						availableRecords = internalBuf.remaining() / CrailConstants.DIRECTORY_RECORD;
						consumedRecords = 0;
						if (randomize){
							shuffleTickets(blockTickets, availableRecords);
						}
					} 		
				}
			}
			
			return availableRecords - consumedRecords;
		} catch(Exception e){
			return 0;
		}
	}	
	
	public void close() throws IOException {
		try {
			if (!open) { 
				return;
			} 			
			
			stream.close();
			fs.freeBuffer(internalBuf);
			internalBuf = null;
			open = false;
		} catch (Exception e) {
			throw new IOException(e);
		}
	}	
	
	void shuffleTickets(int[] tickets, int length) {
		for (int i = 0; i < availableRecords; i++){
			blockTickets[i] = i;
		}		
		for (int i = length - 1; i > 0; i--) {
			int index = random.nextInt(i + 1);
			int tmp = tickets[index];
			tickets[index] = tickets[i];
			tickets[i] = tmp;
		}
	}
}
