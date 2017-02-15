package com.ibm.crail.namenode.protocol;

public enum FileType {
	DATAFILE(0), DIRECTORY(1), MULTIFILE(2), STREAMFILE(3);
	
	private int value;
	
	FileType(int value){
		this.value = value;
	}
	
	public int value(){
		return this.value;
	}
	
	public boolean isDir(){
		return this == DIRECTORY;
	}
	
	public static FileType parse(int value){
		switch(value){
		case 0:
				return DATAFILE;
		case 1:
				return DIRECTORY;
		case 2:
				return MULTIFILE;
		case 3:
				return STREAMFILE;
		}
		return null;
	}
}
