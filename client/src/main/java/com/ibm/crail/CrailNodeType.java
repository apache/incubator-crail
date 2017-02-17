package com.ibm.crail;

public enum CrailNodeType {
	DATAFILE(0), DIRECTORY(1), MULTIFILE(2), STREAMFILE(3);
	
	private int label;
	
	CrailNodeType(int label){
		this.label = label;
	}
	
	public int getLabel(){
		return this.label;
	}
	
	public boolean isDirectory(){
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
	
	public static CrailNodeType parse(int label) {
		for (CrailNodeType val : CrailNodeType.values()) {
			if (val.getLabel() == label) {
				return val;
			}
		}
		throw new IllegalArgumentException();
	}	
}
