package com.ibm.crail.storage.nvmf.client;

import com.ibm.crail.CrailBuffer;
import com.ibm.disni.nvmef.spdk.NvmeStatusCodeType;

import java.io.IOException;

/**
 * Created by jpf on 23.05.17.
 */
public class NvmfStorageUnalignedWriteFuture extends NvmfStorageFuture {

	private CrailBuffer stagingBuffer;

	public NvmfStorageUnalignedWriteFuture(NvmfStorageEndpoint endpoint, int len, CrailBuffer stagingBuffer) {
		super(endpoint, len);
		this.stagingBuffer = stagingBuffer;
	}

	@Override
	void signal(NvmeStatusCodeType statusCodeType, int statusCode) {
		try {
			endpoint.putBuffer(stagingBuffer);
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(-1);
		}
		super.signal(statusCodeType, statusCode);
	}
}
