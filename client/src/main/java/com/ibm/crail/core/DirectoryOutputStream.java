package com.ibm.crail.core;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.Future;

import com.ibm.crail.CrailResult;

public class DirectoryOutputStream {
	private CoreOutputStream stream;
	private ByteBuffer internalBuf;	
	private CoreFileSystem fs;

	public DirectoryOutputStream(CoreOutputStream stream)
			throws Exception {
		this.fs = stream.getFile().getFileSystem();
		this.stream = stream;
		this.internalBuf = fs.allocateBuffer();
	}
	
	Future<CrailResult> writeRecord(DirectoryRecord record, long offset) throws Exception {
		internalBuf.clear();
		record.write(internalBuf);
		internalBuf.flip();
		stream.seek(offset);
		Future<CrailResult> future = stream.write(internalBuf);
		return future;
	}	
	
	public void close() throws IOException {
		try {
			if (!stream.isOpen()){
				return;
			}
			stream.close();
			fs.freeBuffer(internalBuf);
		} catch (Exception e) {
			throw new IOException(e);
		} 		
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
