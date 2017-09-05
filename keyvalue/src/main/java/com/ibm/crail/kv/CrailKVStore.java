package com.ibm.crail.kv;

import java.nio.ByteBuffer;
import java.util.StringTokenizer;

import org.slf4j.Logger;

import com.ibm.crail.CrailBuffer;
import com.ibm.crail.CrailFS;
import com.ibm.crail.CrailFile;
import com.ibm.crail.CrailInputStream;
import com.ibm.crail.CrailLocationClass;
import com.ibm.crail.CrailNodeType;
import com.ibm.crail.CrailOutputStream;
import com.ibm.crail.CrailStorageClass;
import com.ibm.crail.conf.CrailConfiguration;
import com.ibm.crail.memory.OffHeapBuffer;
import com.ibm.crail.utils.CrailUtils;

public class CrailKVStore {
	private static final Logger LOG = CrailUtils.getLogger();
	private CrailFS crail;
	
	public CrailKVStore() throws Exception {
		CrailConfiguration crailConf = new CrailConfiguration();
		this.crail = CrailFS.newInstance(crailConf);
		LOG.info("CrailKVStore* initialization done..");
	}
	
	public void createTable(String table, CrailStorageClass storageClass, CrailLocationClass locationClass) throws Exception{
		table = setTable(table);
		LOG.info("create table, name " + table);
		crail.create(table, CrailNodeType.DIRECTORY, storageClass, locationClass).get().syncDir();
	}
	
	public void deleteTable(String table) throws Exception{
		table = setTable(table);
		LOG.info("delete table, name " + table);
		crail.delete(table, true);
	}
	
	public void writeKey(String table, String key, ByteBuffer value) throws Exception{
		table = setTable(table);
		String path = table + "/" + key;
		LOG.info("write key, name " + path);
		CrailFile file = crail.create(path, CrailNodeType.DATAFILE, CrailStorageClass.PARENT, CrailLocationClass.DEFAULT).get().asFile();
		CrailOutputStream stream = file.getDirectOutputStream(value.capacity());
		CrailBuffer crailBuffer = OffHeapBuffer.wrap(value);
		stream.write(crailBuffer).get();
		stream.close();
	}
	
	public CrailOutputStream writeKey(String table, String key) throws Exception{
		table = setTable(table);
		String path = table + "/" + key;
		LOG.info("read key, name " + path);
		CrailFile file = crail.create(path, CrailNodeType.DATAFILE, CrailStorageClass.PARENT, CrailLocationClass.DEFAULT).get().asFile();
		CrailOutputStream stream = file.getDirectOutputStream(0);
		return stream;
	}	
	
	public void readKey(String table, String key, ByteBuffer value) throws Exception {
		table = setTable(table);
		String path = table + "/" + key;
		LOG.info("write key, name " + path);
		CrailFile file = crail.create(path, CrailNodeType.DATAFILE, CrailStorageClass.PARENT, CrailLocationClass.DEFAULT).get().asFile();
		CrailInputStream stream = file.getDirectInputStream(file.getCapacity());
		CrailBuffer crailBuffer = OffHeapBuffer.wrap(value);
		stream.read(crailBuffer).get();
		stream.close();		
	}
	
	public CrailInputStream readKey(String table, String key) throws Exception {
		table = setTable(table);
		String path = table + "/" + key;
		LOG.info("read key, name " + path);
		CrailFile file = crail.create(path, CrailNodeType.DATAFILE, CrailStorageClass.PARENT, CrailLocationClass.DEFAULT).get().asFile();
		CrailInputStream stream = file.getDirectInputStream(file.getCapacity());
		return stream;
	}	
	
	public void close() throws Exception {
		crail.close();
	}
	
	private String setTable(String name) throws Exception{
		if (name == null || name.isEmpty()){
			name = "/";
		}
		if (!name.startsWith("/")){
			name = "/" + name;
		}
		StringTokenizer tokenizer = new StringTokenizer(name, "/");
		if (tokenizer.countTokens() != 1){
			throw new Exception("table name should not include '/' characters");
		} else {
			LOG.info("setTable " + name);
		}
		return name;
	}
}
