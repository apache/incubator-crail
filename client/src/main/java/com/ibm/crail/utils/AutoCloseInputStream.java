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

package com.ibm.crail.utils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.Future;

import com.ibm.crail.CrailInputStream;
import com.ibm.crail.CrailResult;

public class AutoCloseInputStream implements CrailInputStream {
	private CrailInputStream innerStream;
	
	public AutoCloseInputStream(CrailInputStream innerStream){
		this.innerStream = innerStream;
	}
	

	@Override
	public int read(ByteBuffer dataBuf) throws Exception {
		int ret = innerStream.read(dataBuf);
		if (ret < 0){
			close();
		}	
		return ret;
	}


	@Override
	public Future<CrailResult> readAsync(ByteBuffer dataBuf) throws Exception {
		throw new Exception("method not supported");
	}	

	@Override
	public void close() throws IOException {
		if (innerStream.available() != 0){
			throw new IOException("closing while inner stream available " + innerStream.available());
		}
		innerStream.close();
	}


	@Override
	public int available() {
		return innerStream.available();
	}

	@Override
	public void seek(long pos) throws IOException {
		innerStream.seek(pos);
	}


	@Override
	public boolean isOpen() {
		return innerStream.isOpen();
	}


	@Override
	public long position() {
		return innerStream.position();
	}


	public long getReadHint() {
		return innerStream.getReadHint();
	}
}
