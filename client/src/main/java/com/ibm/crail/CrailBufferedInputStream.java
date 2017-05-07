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

package com.ibm.crail;

import java.io.IOException;

import com.ibm.crail.conf.CrailConstants;

public class CrailBufferedInputStream extends BufferedInputStream {
	private CrailInputStream inputStream;
	
	CrailBufferedInputStream(CrailFile file, long readHint) throws Exception {
		super(file.getFileSystem(), Math.max(CrailConstants.BUFFER_SIZE, CrailConstants.SLICE_SIZE)/Math.min(CrailConstants.BUFFER_SIZE, CrailConstants.SLICE_SIZE), file.getCapacity());
		this.inputStream = file.getDirectInputStream(readHint);
	}

	@Override
	public CrailInputStream getStream() throws Exception {
		return inputStream;
	}

	@Override
	public void putStream() throws Exception {
		
	}

	@Override
	public void close() throws IOException {
		super.close();
		try {
			inputStream.close();
		} catch(Exception e){
			throw new IOException(e);
		}
	}
	
}

