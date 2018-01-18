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

package org.apache.crail.storage.nvmf.client;

import org.apache.crail.metadata.BlockInfo;

/**
 * Created by jpf on 14.02.17.
 */
public class NvmfStorageUtils {

	public static long linearBlockAddress(BlockInfo remoteMr, long remoteOffset, int sectorSize) {
		return (remoteMr.getAddr() + remoteOffset) / (long)sectorSize;
	}

	public static long namespaceSectorOffset(int sectorSize, long fileOffset) {
		return fileOffset % (long)sectorSize;
	}

	public static long alignLength(int sectorSize, long remoteOffset, long len) {
		long alignedSize = len + namespaceSectorOffset(sectorSize, remoteOffset);
		if (namespaceSectorOffset(sectorSize, alignedSize) != 0) {
			alignedSize += (long)sectorSize - namespaceSectorOffset(sectorSize, alignedSize);
		}
		return alignedSize;
	}

	public static long alignOffset(int sectorSize, long fileOffset) {
		return fileOffset - namespaceSectorOffset(sectorSize, fileOffset);
	}
}
