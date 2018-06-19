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

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.crail.conf.CrailConfiguration;
import org.apache.crail.conf.CrailConstants;
import org.apache.crail.core.CoreDataStore;
import org.apache.crail.utils.CrailUtils;
import org.slf4j.Logger;

public abstract class CrailStore {
	private static final Logger LOG = CrailUtils.getLogger();
	private static AtomicLong referenceCounter = new AtomicLong(0);
	private static CrailStore instance = null;

	public abstract Upcoming<CrailNode> create(String path, CrailNodeType type, CrailStorageClass storageClass, CrailLocationClass locationClass, boolean enumerable) throws Exception;
	public abstract Upcoming<CrailNode> lookup(String path) throws Exception;
	public abstract Upcoming<CrailNode> rename(String srcPath, String dstPath) throws Exception;
	public abstract Upcoming<CrailNode> delete(String path, boolean recursive) throws Exception;
	public abstract CrailBuffer allocateBuffer() throws Exception;
	public abstract void freeBuffer(CrailBuffer buffer) throws Exception;
	public abstract CrailStatistics getStatistics();
	public abstract CrailLocationClass getLocationClass();
	protected abstract void closeFileSystem() throws Exception;

	public void close() throws Exception {
		synchronized(referenceCounter){
			if (CrailConstants.SINGLETON){
				long counter = referenceCounter.decrementAndGet();
				if (counter == 0){
					LOG.info("Closing CrailFS singleton");
					try {
						closeFileSystem();
						instance = null;
					} catch (Exception e){
						throw new IOException(e);
					}
				}
			} else {
				LOG.info("Closing CrailFS non-singleton");
				try {
					closeFileSystem();
				} catch (Exception e){
					throw new IOException(e);
				}
			}

		}
	}

	public static CrailStore newInstance(CrailConfiguration conf) throws Exception {
		synchronized(referenceCounter){
			boolean isSingleton = conf.getBoolean(CrailConstants.SINGLETON_KEY, CrailConstants.SINGLETON);
			if (isSingleton) {
				referenceCounter.incrementAndGet();
				if (instance == null) {
					LOG.info("creating singleton crail file system");
					instance = new CoreDataStore(conf);
					return instance;
				} else {
					LOG.info("returning singleton crail file system");
					return instance;
				}
			} else {
				LOG.info("creating non-singleton crail file system");
				return new CoreDataStore(conf);
			}
		}
	}
}

