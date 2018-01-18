package org.apache.crail.namenode;

import org.apache.crail.CrailNodeType;

public class KeyValueBlocks extends FileBlocks {
	public KeyValueBlocks(long fd, int fileComponent, CrailNodeType type,
			int storageClass, int locationClass) {
		super(fd, fileComponent, type, storageClass, locationClass);
	}

}
