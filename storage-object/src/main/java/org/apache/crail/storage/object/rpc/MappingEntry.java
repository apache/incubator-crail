/*
 * Copyright (C) 2015-2018, IBM Corporation
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
package org.apache.crail.storage.object.rpc;

import static java.lang.Math.max;

import java.util.Comparator;

public class MappingEntry implements Comparable<MappingEntry>, Comparator<MappingEntry> {
	private final String key; // object key, cannot be modifies
	private long blockOffset;// the blockOffset in the Crail address space
	private long length; // the length of the object
	private long objectOffset; // the offset in the original object

	public MappingEntry(String key, long startOffset, long length) {
		assert length > 0;
		this.key = key;
		this.blockOffset = startOffset;
		this.length = length;
		this.objectOffset = 0;
	}

	public MappingEntry(String key, long start, long length, long objectOffset) {
		assert length > 0;
		this.key = key;
		this.blockOffset = start;
		this.length = length;
		this.objectOffset = objectOffset;
	}

	public String getKey() {
		return key;
	}

	public long getSize() {
		return length;
	}

	public long getStartOffset() {
		return blockOffset;
	}

	public long getObjectOffset() {
		return objectOffset;
	}

	public String toString() {
		return "MappingEntry: Block range (" + blockOffset + ", " + getEndOffset() + ") maps to Object (key= "
				+ key + ", offset=" + objectOffset + ")";
	}

	public long getEndOffset() {
		return blockOffset + length;
	}

	public boolean equals(MappingEntry o) {
		if (this.key.equals(o.key) && this.blockOffset == o.blockOffset && this.length == o.length &&
				this.objectOffset == o.objectOffset)
			return true;
		return false;
	}

	public void setStartAddr(long newStart) {
		length = getEndOffset() - newStart;
		blockOffset = newStart;
	}

	public void setEndAddr(long newEnd) {
		length = max(0, newEnd - blockOffset);
	}

	public boolean isValid() {
		return length > 0;
	}

	public int compareTo(MappingEntry e) {
		return (int) (blockOffset - e.blockOffset);
	}

	public int compare(MappingEntry e1, MappingEntry e2) {
		return (int) (e1.blockOffset - e2.blockOffset);
	}
}
