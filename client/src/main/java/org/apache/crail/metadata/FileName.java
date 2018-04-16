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

package org.apache.crail.metadata;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.StringTokenizer;

import org.apache.crail.conf.CrailConstants;

public class FileName {
	public static int CSIZE = 4 + CrailConstants.DIRECTORY_DEPTH*4;
	
	private int length;
	private int[] components;
	
	public FileName(){
		this.length = 0;
		this.components = new int[CrailConstants.DIRECTORY_DEPTH];
		for (int i = 0; i < components.length; i++){
			components[i] = 0;
		}
	}
	
	public FileName(String name) throws IOException {
		this();
		StringTokenizer tokenizer = new StringTokenizer(name, "/");
		if (tokenizer.countTokens() > components.length){
			throw new IOException("filename with too many tokens, filename " + name + ", tokens " + components.length);
		}
		
		this.length = tokenizer.countTokens();
		int i = 0;
		while (tokenizer.hasMoreElements()) {
			String token = tokenizer.nextToken();
			components[i] = token.hashCode();
			i++;
		}	
	}
	
	public FileName(FileName name){
		this();
		this.length = name.length;
		for (int i = 0; i < components.length; i++){
			this.components[i] = name.components[i];
		}	
	}
	
	public int write(ByteBuffer buffer) {
		buffer.putInt(length);
		int written = 4;
		for (int i = 0; i < components.length; i++){
			buffer.putInt(components[i]);
			written += 4;
		}		
		return written;
	}		

	public void update(ByteBuffer buffer) {
		this.length = buffer.getInt();
		for (int i = 0; i < components.length; i++){
			components[i] = buffer.getInt();
		}
	}	

	public int getFileComponent(){
		return getComponent(length - 1);
	}	
	
	public int getComponent(int index){
		if (index >= 0 && index < components.length){
			return components[index];
		} else {
			return 0;
		}
	}
	
	public int getLength(){
		return length;
	}
	
	
	public int size(){
		return CSIZE;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		FileName other = (FileName) obj;
		if (hashCode() != other.hashCode())
			return false;
		return true;
	}

	@Override
	public String toString() {
		String tmp = "components=/";
		for (int i = 0; i < components.length; i++){
			tmp = tmp + components[i] + "/";
		}
		tmp = tmp + ", length=" + length;
		return tmp;
	}
}
