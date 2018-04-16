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

package org.apache.crail.hdfs;

import java.net.URI;

import org.apache.crail.hdfs.CrailHadoopFileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemContractBaseTest;

public class TestCrailHDFSContract extends FileSystemContractBaseTest {

	@Override
	protected void setUp() throws Exception {
		Configuration conf = new Configuration();
		fs = new CrailHadoopFileSystem();
		fs.initialize(URI.create(conf.get("fs.defaultFS")), conf);
	}

	// --------------------

	protected final static String TEST_UMASK = "062";
	protected FileSystem fs;
	protected byte[] data = dataset(getBlockSize() * 2, 0, 255);

	@Override
	protected void tearDown() throws Exception {
		fs.delete(path("/test"), true);
		fs.close();
	}

	/**
	 * Create a dataset for use in the tests; all data is in the range base to
	 * (base+modulo-1) inclusive
	 * 
	 * @param len
	 *            length of data
	 * @param base
	 *            base of the data
	 * @param modulo
	 *            the modulo
	 * @return the newly generated dataset
	 */
	protected byte[] dataset(int len, int base, int modulo) {
		byte[] dataset = new byte[len];
		for (int i = 0; i < len; i++) {
			dataset[i] = (byte) (base + (i % modulo));
		}
		return dataset;
	}

}
