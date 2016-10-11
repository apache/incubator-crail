package com.ibm.crail.core;

import java.util.concurrent.Future;
import com.ibm.crail.CrailNode;
import com.ibm.crail.namenode.protocol.FileInfo;

public class CoreNode implements CrailNode {
	protected CoreFileSystem fs;
	protected FileInfo fileInfo;
	protected String path;
	protected int storageAffinity;
	protected int locationAffinity;	
	
	protected CoreNode(CoreFileSystem fs, FileInfo fileInfo, String path, int storageAffinity, int locationAffinity){
		this.fs = fs;
		this.fileInfo = fileInfo;
		this.path = path;
		this.storageAffinity = storageAffinity;
		this.locationAffinity = locationAffinity;		
	}	

	@Override
	public CoreFileSystem getFileSystem() {
		return fs;
	}

	@Override
	public String getPath() {
		return path;
	}
	
	public long getModificationTime() {
		return fileInfo.getModificationTime();
	}	
	
	public long getCapacity() {
		return fileInfo.getCapacity();
	}
	
	public boolean isDir() {
		return fileInfo.isDir();
	}
	
	public int storageAffinity(){
		return storageAffinity;
	}	

	public int locationAffinity() {
		return locationAffinity;
	}
	
	public long getFd() {
		return fileInfo.getFd();
	}	

	@Override
	public CoreNode syncDir() throws Exception {
		return this;
	}
	
	FileInfo getFileInfo(){
		return fileInfo;
	}
}

class CoreRenamedNode extends CoreFile {
	private Future<?> srcDirFuture;
	private Future<?> dstDirFuture;
	private DirectoryOutputStream srcStream;
	private DirectoryOutputStream dstStream;
	

	protected CoreRenamedNode(CoreFileSystem fs, FileInfo fileInfo, String path, Future<?> srcDirFuture, Future<?> dstDirFuture, DirectoryOutputStream srcStream, DirectoryOutputStream dstStream){
		super(fs, fileInfo, path, 0, 0);
		this.srcDirFuture = srcDirFuture;
		this.dstDirFuture = dstDirFuture;
		this.srcStream = srcStream;
		this.dstStream = dstStream;
	}

	@Override
	public CoreNode syncDir() throws Exception {
		if (srcDirFuture != null) {
			srcDirFuture.get();
			srcDirFuture = null;
		}
		if (dstDirFuture != null) {
			dstDirFuture.get();
			dstDirFuture = null;
		}		
		if (srcStream != null){
			srcStream.close();
			srcStream = null;
		}
		if (dstStream != null){
			dstStream.close();
			dstStream = null;
		}
		return this;
	}
}

class CoreDeleteNode extends CoreFile {
	private Future<?> dirFuture;
	private DirectoryOutputStream dirStream;	
	
	public CoreDeleteNode(CoreFileSystem fs, FileInfo fileInfo, String path, Future<?> dirFuture, DirectoryOutputStream dirStream){
		super(fs, fileInfo, path, 0, 0);
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
