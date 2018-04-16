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

package org.apache.crail;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;

import org.apache.crail.utils.RingBuffer;

class MultiFileBufferedInputStream extends CrailBufferedInputStream {
	private CrailStore fs;
	private Iterator<String> paths;
	private RingBuffer<CrailInputStream> readyStreams;
	private LinkedList<CrailInputStream> finalStreams;
	
	MultiFileBufferedInputStream(CrailStore fs, Iterator<String> paths, int outstanding, int files) throws Exception {
		super(fs, outstanding, 0);
		this.fs = fs;
		this.paths = paths;
		this.readyStreams = new RingBuffer<CrailInputStream>(1);
		this.finalStreams = new LinkedList<CrailInputStream>();
	}

	@Override
	public CrailInputStream getStream() throws Exception {
		while(readyStreams.isEmpty() && paths.hasNext()){
			String path = paths.next();
			CrailNode node = fs.lookup(path).get();
			if (node != null){
				CrailFile file = node.asFile();
				if (file.getCapacity() > 0){
					CrailInputStream stream = file.getDirectInputStream(file.getCapacity());
					readyStreams.add(stream);
				}
			}
		}
		return readyStreams.peek();
	}
	
	
	public void putStream() throws Exception {
		CrailInputStream stream = readyStreams.peek();
		if (stream.position() >= stream.getFile().getCapacity()) {
			stream = readyStreams.poll();
			finalStreams.add(stream);
		}
	}

	@Override
	public void close() throws IOException {
		super.close();
		try {
			while(!readyStreams.isEmpty()){
				finalStreams.add(readyStreams.poll());
			}
			while(paths.hasNext()){
				String path = paths.next();
				CrailFile file = fs.lookup(path).get().asFile();
				if (file != null){
					CrailInputStream stream = file.getDirectInputStream(file.getCapacity());
					finalStreams.add(stream);
				}
			}
			while(!finalStreams.isEmpty()){
				CrailInputStream stream = finalStreams.poll();
				stream.close();
			}
		} catch(Exception e){
			throw new IOException(e);
		}
	}

	@Override
	public void seek(long pos) throws IOException {
		throw new IOException("Seek not supported on multistream");
	}	
	
}

