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

package org.apache.crail;


public interface CrailNode {
	public CrailStore getFileSystem();
	public String getPath(); 
	public abstract CrailNode syncDir() throws Exception;
	public abstract long getModificationTime();
	public abstract long getCapacity();
	public abstract CrailNodeType getType();
	public abstract CrailFile asFile() throws Exception;
	public abstract CrailContainer asContainer() throws Exception;
	public abstract CrailDirectory asDirectory() throws Exception;
	public abstract CrailMultiFile asMultiFile() throws Exception;
	public abstract CrailTable asTable() throws Exception;
	public abstract CrailKeyValue asKeyValue() throws Exception;
	public abstract CrailBlockLocation[] getBlockLocations(long start, long len) throws Exception;
}
