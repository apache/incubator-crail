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
import java.util.concurrent.Future;

import org.apache.crail.CrailBuffer;
import org.apache.crail.CrailResult;

public class DirectoryOutputStream {
	private CoreOutputStream stream;
	private CrailBuffer internalBuf;	
	private CoreDataStore fs;
	private boolean open;

	public DirectoryOutputStream(CoreOutputStream stream)
			throws Exception {
		this.fs = stream.getFile().getFileSystem();
		this.stream = stream;
		this.internalBuf = fs.allocateBuffer();
		this.open = true;
	}
	
	Future<CrailResult> writeRecord(DirectoryRecord record, long offset) throws Exception {
		if (!open) {
			throw new IOException("stream closed");
		} 		
		
		internalBuf.clear();
		record.write(internalBuf);
		internalBuf.flip();
		stream.seek(offset);
		Future<CrailResult> future = stream.write(internalBuf);
		return future;
	}	
	
	public void close() throws IOException {
		try {
			if (!open){
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
	
	//debug
	
	public int getBufCapacity(){
		return internalBuf.capacity();
	}
	
	public int getBufPosition(){
		return internalBuf.position();
	}
	
	public int getBufLimit(){
		return internalBuf.limit();
	}	

}
