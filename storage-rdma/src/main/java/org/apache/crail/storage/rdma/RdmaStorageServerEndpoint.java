/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.crail.storage.rdma;

import java.io.IOException;

import com.ibm.disni.verbs.*;
import com.ibm.disni.*;

public class RdmaStorageServerEndpoint extends RdmaActiveEndpoint {
	private RdmaStorageServer closer;

	public RdmaStorageServerEndpoint(RdmaActiveEndpointGroup<RdmaStorageServerEndpoint> endpointGroup, RdmaCmId idPriv, RdmaStorageServer closer, boolean serverSide) throws IOException {
		super(endpointGroup, idPriv, serverSide);
		this.closer = closer;
	}	

	public void dispatchCqEvent(IbvWC wc) throws IOException {

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
