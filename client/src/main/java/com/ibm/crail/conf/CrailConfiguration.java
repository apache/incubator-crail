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

package com.ibm.crail.conf;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;

import com.ibm.crail.utils.CrailUtils;

public class CrailConfiguration {
	private static final Logger LOG = CrailUtils.getLogger();
	private ConcurrentHashMap<String, String> conf;
	
	
	public CrailConfiguration() throws IOException{
		conf = new ConcurrentHashMap<>();
		Properties properties = loadProperties("crail-site.conf");
		mergeProperties(properties);
	}

	public String get(String key) {
		return conf.get(key);
	}

	public void set(String key, String value) {
		conf.put(key, value);
	}
	
	public boolean getBoolean(String key, boolean fallback) {
		if (conf.containsKey(key)){
			return Boolean.parseBoolean(conf.get(key));
		} else {
			return fallback;
		}
	}

	public void setInt(String key, int level) {
		String value = Integer.toString(level);
		conf.put(key, value);
	}
	
	private void mergeProperties(Properties properties) {
		if (properties == null){
			return;
		}
		for (String key : properties.stringPropertyNames()) {
			conf.put(key.trim(), properties.getProperty(key).trim());
		}
	}
	
	private static Properties loadProperties(String resourceName) throws IOException {
		Properties properties = new Properties();

		String base = System.getenv("CRAIL_HOME");
		FileInputStream inputStream = new FileInputStream(new File(base + "/conf/" + resourceName));

		try {
			properties.load(inputStream);
		} finally {
			inputStream.close();
		}
		return properties;
	}
}
