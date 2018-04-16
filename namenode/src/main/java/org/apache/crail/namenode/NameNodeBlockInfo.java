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

package org.apache.crail.namenode;

import org.apache.crail.metadata.BlockInfo;
import org.apache.crail.metadata.DataNodeInfo;

public class NameNodeBlockInfo extends BlockInfo {
	private BlockInfo region;
	private long offset;
	
	public NameNodeBlockInfo(BlockInfo region, long offset, int length){
		this.region = region;
		this.offset = offset;
		this.length = length;
		
		this.dnInfo = this.getDnInfo();
		this.lba = this.getLba();
		this.addr = this.getAddr();
		this.lkey = this.getLkey();		
	}	

	@Override
	public long getLba() {
		return region.getLba() + offset;
	}

	@Override
	public long getAddr() {
		return region.getAddr() + offset;
	}

	@Override
	public int getLkey() {
		return region.getLkey();
	}

	@Override
	public DataNodeInfo getDnInfo() {
		return region.getDnInfo();
	}

	public BlockInfo getRegion() {
		return region;
	}
	
}
