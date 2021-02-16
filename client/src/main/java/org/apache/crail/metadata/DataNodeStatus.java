package org.apache.crail.metadata;

import java.nio.ByteBuffer;

public class DataNodeStatus {
    public static final int CSIZE = 2;

    private short status;

    public static final short STATUS_DATANODE_STOP = 1;

    public DataNodeStatus() {
        this.status = 0;
    }

    public int write(ByteBuffer buffer) {
        buffer.putShort(status);
        return CSIZE;
    }

    public void update(ByteBuffer buffer) {
        this.status = buffer.getShort();
    }

    public short getStatus() {
        return this.status;
    }

    public void setStatus(short status) {
        this.status = status;
    }
}
