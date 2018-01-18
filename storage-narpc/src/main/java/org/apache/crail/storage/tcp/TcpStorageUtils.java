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

package org.apache.crail.storage.tcp;

import java.nio.ByteBuffer;

import org.apache.crail.utils.CrailUtils;
import org.slf4j.Logger;

public class TcpStorageUtils {
	private static final Logger LOG = CrailUtils.getLogger();
	public static void printBuffer(String name, ByteBuffer buffer){
		String state = buffer.toString();
		String data = "";
		for (int i = 0; i < buffer.remaining(); i++){
			data += buffer.get(buffer.position() + i) + ",";
		}
		LOG.info("buffer " + name + ", value " + state + ": " + data);
	}
}
