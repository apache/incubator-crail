package com.ibm.crail.namenode;

import com.ibm.crail.CrailNodeType;

public class TableBlocks extends DirectoryBlocks {

	TableBlocks(long fd, int fileComponent, CrailNodeType type,
			int storageClass, int locationClass) {
		super(fd, fileComponent, type, storageClass, locationClass);
	}

	@Override
	public AbstractNode putChild(AbstractNode child) throws Exception {
		if (!child.getType().isKeyValue()){
			throw new Exception("Attempt to create key/value pair in container other than a table");
		}
		
		return children.put(child.getComponent(), child);
	}
}
