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

import org.apache.crail.CrailNodeType;
import org.apache.crail.conf.CrailConstants;

public class TableBlocks extends DirectoryBlocks {

	TableBlocks(long fd, int fileComponent, CrailNodeType type,
			int storageClass, int locationClass, boolean enumerable) {
		super(fd, fileComponent, type, storageClass, locationClass, enumerable);
	}

	@Override
	public AbstractNode putChild(AbstractNode child) throws Exception {
		if (!child.getType().isKeyValue()){
			throw new Exception("Attempt to create key/value pair in container other than a table");
		}
		
		AbstractNode oldNode = children.put(child.getComponent(), child);
		if (child.isEnumerable()) {
			child.setDirOffset(dirOffsetCounter.getAndAdd(CrailConstants.DIRECTORY_RECORD));
		}		
		return oldNode;
	}
}
