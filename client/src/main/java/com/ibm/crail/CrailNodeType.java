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

public enum CrailNodeType {
	DATAFILE(0), DIRECTORY(1), MULTIFILE(2), STREAMFILE(3);
	
	private int label;
	
	CrailNodeType(int label){
		this.label = label;
	}
	
	public int getLabel(){
		return this.label;
	}
	
	public boolean isDirectory(){
		return this == DIRECTORY;
	}
	
	public boolean isDataFile(){
		return this == DATAFILE;
	}	
	
	public boolean isMultiFile(){
		return this == MULTIFILE;
	}
	
	public boolean isStreamFile(){
		return this == STREAMFILE;
	}
	
	public boolean isContainer(){
		return this == DIRECTORY || this == MULTIFILE;
	}	
	
	public static CrailNodeType parse(int label) {
		for (CrailNodeType val : CrailNodeType.values()) {
			if (val.getLabel() == label) {
				return val;
			}
		}
		throw new IllegalArgumentException();
	}	
}
