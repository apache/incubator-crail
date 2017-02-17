package com.ibm.crail;

public enum CrailNodeType {
	DATAFILE(0), DIRECTORY(1), MULTIFILE(2), STREAMFILE(3);
	
	private int value;
	
	CrailNodeType(int value){
		this.value = value;
	}
	
	public int value(){
		return this.value;
	}
	
	public boolean isDir(){
		return this == DIRECTORY;
	}
	
	public boolean isDataFile(){
		return this == DATAFILE;
	}	
	
	public boolean isMultiFile(){
		return this == MULTIFILE;
	}
	
	public boolean isStreamFile(){
		return this == STREAMFILE;
	}
	
	public static CrailNodeType parse(int value){
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
