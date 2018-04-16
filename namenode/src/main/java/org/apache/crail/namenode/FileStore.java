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

package org.apache.crail.namenode;

import java.io.IOException;

import org.apache.crail.CrailNodeType;
import org.apache.crail.conf.CrailConstants;
import org.apache.crail.metadata.FileName;
import org.apache.crail.rpc.RpcErrors;
import org.apache.crail.rpc.RpcNameNodeState;

public class FileStore {
	private Sequencer sequencer;
	private AbstractNode root;
	
	public FileStore(Sequencer sequencer) throws IOException { 
		this.sequencer = sequencer;
		this.root = createNode(new FileName("/").getFileComponent(), CrailNodeType.DIRECTORY, CrailConstants.STORAGE_ROOTCLASS, 0, false);
	}
	
	public AbstractNode createNode(int fileComponent, CrailNodeType type, int storageClass, int locationClass, boolean enumerable) throws IOException {
		if (type == CrailNodeType.DIRECTORY){
			return new DirectoryBlocks(sequencer.getNextId(), fileComponent, type, storageClass, locationClass, enumerable);
		} else if (type == CrailNodeType.MULTIFILE){
			return new MultiFileBlocks(sequencer.getNextId(), fileComponent, type, storageClass, locationClass, enumerable);
		} else if (type == CrailNodeType.TABLE){
			return new TableBlocks(sequencer.getNextId(), fileComponent, type, storageClass, locationClass, enumerable);
		} else if (type == CrailNodeType.KEYVALUE){
			return new KeyValueBlocks(sequencer.getNextId(), fileComponent, type, storageClass, locationClass, enumerable);
		} else if (type == CrailNodeType.DATAFILE){
			return new FileBlocks(sequencer.getNextId(), fileComponent, type, storageClass, locationClass, enumerable);
		} else {
			throw new IOException("File type unkown: " + type);
		}
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
			error.setError(RpcErrors.ERR_FILE_COMPONENTS_EXCEEDED);
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
