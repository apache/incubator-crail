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

import java.util.Iterator;
import java.util.NoSuchElementException;
import org.slf4j.Logger;
import com.ibm.crail.utils.CrailUtils;

class CoreFileIterator implements Iterator<String> {
	private static final Logger LOG = CrailUtils.getLogger();
	private DirectoryRecordIterator iter;
	private String current;
	private boolean isOpen;
	
	public CoreFileIterator(DirectoryRecordIterator iter) {
		this.iter = iter;
		this.current = null;
		this.isOpen = true;
	}

	@Override
	public boolean hasNext() {
		if (!isOpen){
			return false;
		}
		
		if (current == null){
			try {
				current = search();
			} catch(Exception e) {
				current = null;
			}
		}
		
		if (current == null){
			try {
				this.close();
			} catch (Exception e) {
				LOG.info("closing file iterator failed " + e.getMessage());
			} finally {
				isOpen = false;
			}
		}
		
		return current != null;
	}

	@Override
	public String next() throws NoSuchElementException {
		String file = current;
		if (file == null){
			try {
				file = search();
			} catch(Exception e) {
				file = null;
			}
		} else {
			current = null;
		}
		
		return file;
	}
	
	public String search() throws Exception {
		String file = null;
		DirectoryRecord record = iter.next();
		while(record != null && !record.isValid()){
			record = iter.next();
		}
		if (record != null && record.isValid()) {
			try {
				file = CrailUtils.combinePath(record.getParent(), record.getFile());
			} catch (Exception e) {
				file = null;
				throw new NoSuchElementException(e.getMessage());
			}
		}
		return file;
	}	
	
	public void close() throws Exception{
		iter.close();
	}
	
	//---debug
	
	public long getFileOffset(){
		return iter.getFileOffset();
	}
	
	public long available(){
		return iter.available();
	}
	
	public String getDirName(){
		return iter.getDirName();
	}

	public long getSize() {
		return iter.getSize();
	}

	public long getIndex() {
		return iter.getIndex();
	}
	
	public int getBufCapacity(){
		return iter.getBufCapacity();
	}
	
	public int getBufPosition(){
		return iter.getBufPosition();
	}
	
	public int getBufLimit(){
		return iter.getBufLimit();
	}	
}

