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

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class DirectoryRecordIterator implements Iterator<DirectoryRecord> {
	private String parent;
	private DirectoryInputStream inputStream;
	private long size;
	private long index;
	private DirectoryRecord current;

	public DirectoryRecordIterator(String parent, DirectoryInputStream inputStream) throws IOException {
		this.parent = parent;
		this.inputStream = inputStream;
		this.size = inputStream.available() / DirectoryRecord.MaxSize;
		this.index = 0;		
		this.current = null;
	}

	@Override
	public synchronized boolean hasNext() {
		if (current == null){
			try {
				current = search();
			} catch(Exception e) {
				current = null;
			}
		}
		return current != null;
	}

	@Override
	public synchronized DirectoryRecord next() throws NoSuchElementException {
		DirectoryRecord record = current;
		if (record == null){
			try {
				record = search();
			} catch(Exception e) {
				record = null;
			}
		} else {
			current = null;
		}
		return record;
	}
	
	private DirectoryRecord search() throws Exception{
		DirectoryRecord record = inputStream.readRecord(parent);
		return record;
	}
	
	public int recordsRemaining(){
		return (int) inputStream.available() / DirectoryRecord.MaxSize;
	}
	
	public void close() throws Exception{
		if (inputStream.isOpen()){
			inputStream.close();
		}
	}
	
	public long getFileOffset(){
		return inputStream.position();
	}
	
	public long available(){
		return inputStream.available();
	}
	
	public String getDirName(){
		return parent;
	}
	
	public int getBufCapacity(){
		return inputStream.getBufCapacity();
	}
	
	public int getBufPosition(){
		return inputStream.getBufPosition();
	}
	
	public int getBufLimit(){
		return inputStream.getBufLimit();
	}

	public long getSize() {
		return size;
	}

	public long getIndex() {
		return index;
	}	
}
