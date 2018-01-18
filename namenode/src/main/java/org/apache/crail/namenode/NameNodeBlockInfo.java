package org.apache.crail.namenode;

import org.apache.crail.metadata.BlockInfo;
import org.apache.crail.metadata.DataNodeInfo;

public class NameNodeBlockInfo extends BlockInfo {
	private BlockInfo region;
	private long offset;
	
	public NameNodeBlockInfo(BlockInfo region, long offset, int length){
		this.region = region;
		this.offset = offset;
		this.length = length;
		
		this.dnInfo = this.getDnInfo();
		this.lba = this.getLba();
		this.addr = this.getAddr();
		this.lkey = this.getLkey();		
	}	

	@Override
	public long getLba() {
		return region.getLba() + offset;
	}

	@Override
	public long getAddr() {
		return region.getAddr() + offset;
	}

	@Override
	public int getLkey() {
		return region.getLkey();
	}

	@Override
	public DataNodeInfo getDnInfo() {
		return region.getDnInfo();
	}

	public BlockInfo getRegion() {
		return region;
	}
	
}
