/*
 * Crail: A Multi-tiered Distributed Direct Access File System
 *
 * Author: Patrick Stuedi <stu@zurich.ibm.com>
 *
 * Copyright (C) 2016, IBM Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.ibm.crail.storage.rdma;

import java.io.IOException;

import com.ibm.disni.rdma.verbs.*;
import com.ibm.disni.rdma.*;

public class RdmaStorageServerEndpoint extends RdmaEndpoint {
	private RdmaStorageServer closer;

	public RdmaStorageServerEndpoint(RdmaPassiveEndpointGroup<RdmaStorageServerEndpoint> endpointGroup, RdmaCmId idPriv, RdmaStorageServer closer, boolean serverSide) throws IOException {	
		super(endpointGroup, idPriv, serverSide);
		this.closer = closer;
	}	
	
	public synchronized void dispatchCmEvent(RdmaCmEvent cmEvent)
			throws IOException {
		super.dispatchCmEvent(cmEvent);
		int eventType = cmEvent.getEvent();
		if (eventType == RdmaCmEvent.EventType.RDMA_CM_EVENT_DISCONNECTED
				.ordinal()) {
			closer.close(this);
		}
	}
}
