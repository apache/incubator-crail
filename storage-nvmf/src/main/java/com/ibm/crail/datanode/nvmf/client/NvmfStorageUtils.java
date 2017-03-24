package com.ibm.crail.datanode.nvmf.client;

import com.ibm.crail.namenode.protocol.BlockInfo;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;

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

	public static long getAddress(ByteBuffer buffer) {
		return ((DirectBuffer)buffer).address();
	}
}
