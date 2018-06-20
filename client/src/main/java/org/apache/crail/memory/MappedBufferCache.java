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

package org.apache.crail.memory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

import org.apache.crail.CrailBuffer;
import org.apache.crail.conf.CrailConstants;
import org.apache.crail.utils.CrailUtils;
import org.slf4j.Logger;

public class MappedBufferCache extends BufferCache {
	private static final Logger LOG = CrailUtils.getLogger();

	private String id;
	private String directory;
	private File dir;
	private long allocationCount;
	private long bufferCount;
	private long currentRegion;

	public MappedBufferCache() throws IOException {
		super();

		this.allocationCount = CrailConstants.CACHE_LIMIT / CrailConstants.REGION_SIZE;
		long _bufferSize = (long) CrailConstants.BUFFER_SIZE;
		this.bufferCount = CrailConstants.REGION_SIZE / _bufferSize;
		this.currentRegion = 0;
		LOG.info("buffer cache, allocationCount " + allocationCount + ", bufferCount " + bufferCount);

		if (allocationCount > 0){
			id = "" + System.currentTimeMillis();
			directory = CrailUtils.getCacheDirectory(id);
			dir = new File(directory);
			try {
				if (dir.exists()) {
					throw new IOException("A cache directory with the same id " + id + " already exists!");
				}
				if (!dir.mkdirs()) {
					throw new IOException("Cannot create cache directory [crail.cachepath] set to path " + directory + ", check if crail.cachepath exists and has write permissions");
				}
				for (File child : dir.listFiles()) {
					child.delete();
				}
			} catch(SecurityException e) {
				throw new IOException("Security exception when trying to access " + directory + ", please check the directory permissions", e);
			}
		}
	}

	@Override
	public void close() {
		super.close();

		if (allocationCount > 0 && dir.exists()){
			for (File child : dir.listFiles()) {
				child.delete();
			}
			dir.delete();
		}
		LOG.info("mapped client cache closed");
	}

	public CrailBuffer allocateRegion() throws IOException {
		if (currentRegion >= allocationCount){
			return null;
		}

		String path = directory + "/" + currentRegion++;
		RandomAccessFile randomFile = new RandomAccessFile(path, "rw");
		randomFile.setLength(CrailConstants.REGION_SIZE);
		FileChannel channel = randomFile.getChannel();
		MappedByteBuffer _mappedBuffer = channel.map(MapMode.READ_WRITE, 0,
				CrailConstants.REGION_SIZE);
		CrailBuffer mappedBuffer = OffHeapBuffer.wrap(_mappedBuffer);
		randomFile.close();
		channel.close();

		CrailBuffer firstBuffer = slice(mappedBuffer, 0);

		for (int j = 1; j < bufferCount; j++) {
			int position = j * CrailConstants.BUFFER_SIZE;
			CrailBuffer sliceBuffer = slice(mappedBuffer, position);
			this.putBufferInternal(sliceBuffer);
		}
		mappedBuffer.clear();

		return firstBuffer;
	}

	private CrailBuffer slice(CrailBuffer mappedBuffer, int position){
		int limit = position + CrailConstants.BUFFER_SIZE;
		mappedBuffer.clear();
		mappedBuffer.position(position);
		mappedBuffer.limit(limit);
		CrailBuffer buffer = mappedBuffer.slice();
		return buffer;
	}
}
