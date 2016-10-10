package com.ibm.crail.core;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.concurrent.Future;

import com.ibm.crail.CrailDirectory;
import com.ibm.crail.CrailFS;
import com.ibm.crail.CrailFile;
import com.ibm.crail.CrailResult;
import com.ibm.crail.namenode.protocol.FileInfo;

public class CoreDirectory extends CrailDirectory {
	private CoreFileSystem fs;
	private FileInfo fileInfo;
	private String path;
	
	protected CoreDirectory(CoreFileSystem fs, FileInfo fileInfo, String path){
		this.fs = fs;
		this.fileInfo = fileInfo;
		this.path = path;
	}	

	@Override
	public CrailFS getFileSystem() {
		return fs;
	}

	@Override
	public String getPath() {
		return path;
	}
	
	FileInfo getFileInfo(){
		return this.fileInfo;
	}	
	

	@Override
	public CrailDirectory syncDir() throws Exception {
		return this;
	}

	@Override
	public void close() throws Exception {
	}

	@Override
	public int files() {
		return (int) fileInfo.getCapacity()/DirectoryRecord.MaxSize;
	}

	@Override
	public Iterator<String> listEntries() throws Exception {
		return fs.listEntries(path);
	}

}

class CoreMakeDirectory extends CoreDirectory {
	private Future<?> dirFuture;
	private ByteBuffer dirBuffer;
	private CoreOutputStream dirStream;			

	protected CoreMakeDirectory(CoreFileSystem fs, FileInfo fileInfo, String path, Future<?> dirFuture, ByteBuffer dirBuffer, CoreOutputStream dirStream) {
		super(fs, fileInfo, path);
		this.dirFuture = dirFuture;
		this.dirBuffer = dirBuffer;		
		this.dirStream = dirStream;				
	}
	
	@Override
	public CrailDirectory syncDir() throws Exception {
		if (dirFuture != null) {
			dirFuture.get();
			dirFuture = null;
		}
		if (dirBuffer != null) {
			this.getFileSystem().freeBuffer(dirBuffer);
			dirBuffer = null;
		}
		if (dirStream != null){
			dirStream.close();
			dirStream = null;
		}
		return this;
	}	
}
