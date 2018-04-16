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

import org.apache.crail.CrailBuffer;
import org.apache.crail.conf.CrailConstants;
import org.apache.crail.utils.CrailUtils;

public class DirectoryRecord {
	private int valid;
	private String parent;
	private String filename;
	
	public DirectoryRecord(boolean valid, String path){
		this.valid = valid == true ? 1 : 0;
		this.parent = CrailUtils.getParent(path);
		this.filename = CrailUtils.getName(path);
	}	
	
	public DirectoryRecord(String parent) {
		this.valid = 0;
		this.parent = parent;
		this.filename = null;
		
	}
	
	public void write(CrailBuffer buffer) throws Exception {
		int oldposition = buffer.position();
		buffer.putInt(valid);
		byte barray[] = filename.getBytes();
		buffer.putInt(barray.length);
		buffer.put(barray);
		buffer.position(oldposition + CrailConstants.DIRECTORY_RECORD);
	}
	
	public void update(CrailBuffer buffer) {
		int oldlimit = buffer.limit();
		int tmplimit = buffer.position() + CrailConstants.DIRECTORY_RECORD;
		buffer.limit(tmplimit);
		valid = buffer.getInt();
		int length = buffer.getInt();
		byte barray[] = new byte[length];
		buffer.get(barray);
		filename = new String(barray);
		buffer.position(tmplimit);
		buffer.limit(oldlimit);
	}

	public boolean isValid() {
		return valid == 1;
	}

	public String toString() {
		return valid + "\t\t" + filename;
	}

	public String getFile() {
		return filename;
	}

	public String getParent() {
		return parent;
	}
}
