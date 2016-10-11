package com.ibm.crail.core;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.Future;

import com.ibm.crail.CrailResult;

public class DirectoryOutputStream extends CoreOutputStream {
	private ByteBuffer internalBuf;	
	private CoreFileSystem fs;

	public DirectoryOutputStream(CoreDirectory directory, long streamId)
			throws Exception {
		super(directory, streamId, 0);
		this.fs = directory.getFileSystem();
		this.internalBuf = fs.allocateBuffer();
	}
	
	Future<CrailResult> writeRecord(DirectoryRecord record, long offset) throws Exception {
		internalBuf.clear();
		record.write(internalBuf);
		internalBuf.flip();
		seek(offset);
		Future<CrailResult> future = write(internalBuf);
		return future;
	}	
	
	public void close() throws IOException {
		if (!this.isOpen()){
			return;
		}
		super.close();
		fs.freeBuffer(internalBuf);
	}	
	
	//debug
	
	public int getBufCapacity(){
		return internalBuf.capacity();
	}
	
	public int getBufPosition(){
		return internalBuf.position();
	}
	
	public int getBufLimit(){
		return internalBuf.limit();
	}	

}
