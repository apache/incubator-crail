package com.ibm.crail.core;

import java.util.Iterator;
import java.util.concurrent.Future;

import com.ibm.crail.CrailDirectory;
import com.ibm.crail.conf.CrailConstants;
import com.ibm.crail.namenode.protocol.FileInfo;

class CoreDirectory extends CoreNode implements CrailDirectory {
	
	protected CoreDirectory(CoreFileSystem fs, FileInfo fileInfo, String path){
		super(fs, fileInfo, path, 0, 0);
		this.fs = fs;
		this.fileInfo = fileInfo;
		this.path = path;
	}	

	@Override
	public int files() {
		return (int) fileInfo.getCapacity()/CrailConstants.DIRECTORY_RECORD;
	}

	@Override
	public Iterator<String> listEntries() throws Exception {
		return fs.listEntries(path);
	}
}

class CoreMakeDirectory extends CoreDirectory {
	private Future<?> dirFuture;
	private DirectoryOutputStream dirStream;			

	protected CoreMakeDirectory(CoreFileSystem fs, FileInfo fileInfo, String path, Future<?> dirFuture, DirectoryOutputStream dirStream) {
		super(fs, fileInfo, path);
		this.dirFuture = dirFuture;
		this.dirStream = dirStream;				
	}
	
	@Override
	public CoreNode syncDir() throws Exception {
		if (dirFuture != null) {
			dirFuture.get();
			dirFuture = null;
		}
		if (dirStream != null){
			dirStream.close();
			dirStream = null;
		}
		return this;
	}	
}
