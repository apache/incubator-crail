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

package com.ibm.crail.namenode;

import java.io.IOException;

import com.ibm.crail.conf.CrailConstants;
import com.ibm.crail.namenode.protocol.FileName;
import com.ibm.crail.namenode.rpc.NameNodeProtocol;
import com.ibm.crail.namenode.rpc.RpcNameNodeState;

public class FileStore {
	private AbstractNode root;
	
	public FileStore() throws IOException { 
		this.root = DirectoryBlocks.createRoot();
	}
	
	public AbstractNode retrieveFile(FileName filename, RpcNameNodeState error) throws Exception{
		return retrieveFileInternal(filename, filename.getLength(), error);
	}
	
	public AbstractNode retrieveParent(FileName filename, RpcNameNodeState error) throws Exception{
		return retrieveFileInternal(filename, filename.getLength()-1, error);
	}	
	
	public AbstractNode getRoot() {
		return root;
	}	
	
	public void dump(){
		root.dump();
	}
	
	private AbstractNode retrieveFileInternal(FileName filename, int length, RpcNameNodeState error) throws Exception {
		if (length >= CrailConstants.DIRECTORY_DEPTH){
			error.setError(NameNodeProtocol.ERR_FILE_COMPONENTS_EXCEEDED);
			return null;
		}
		
		AbstractNode current = root;
		for (int i = 0; i < length; i++){
			int component = filename.getComponent(i);
			current = current.getChild(component);
			if (current == null){
				break;
			}
		}
		
		return current;
	}
}
