package org.apache.crail;

public interface CrailBufferCache {
	CrailBuffer allocateBuffer() throws Exception;
	void freeBuffer(CrailBuffer buffer) throws Exception;
}
