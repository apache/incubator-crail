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

import com.ibm.crail.utils.MultiFuture;

class CrailPurgeOperation extends MultiFuture<CrailResult, CrailResult> implements CrailResult {
	private long completedLen;
	
	public CrailPurgeOperation() {
		this.completedLen = 0;
	}

	@Override
	public void aggregate(CrailResult obj) {
		this.completedLen += obj.getLen();
	}

	@Override
	public CrailResult getAggregate() {
		return this;
	}

	@Override
	public long getLen() {
		return completedLen;
	}

}
