package com.ibm.crail.namenode;

import com.ibm.crail.CrailNodeType;

public class MultiFileBlocks extends DirectoryBlocks {

	MultiFileBlocks(long fd, int fileComponent, CrailNodeType type,
			int storageClass, int locationClass) {
		super(fd, fileComponent, type, storageClass, locationClass);
	}

}
