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

package com.ibm.crail.hdfs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;

import org.slf4j.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import com.ibm.crail.CrailBufferedInputStream;
import com.ibm.crail.CrailBufferedOutputStream;
import com.ibm.crail.CrailBlockLocation;
import com.ibm.crail.CrailDirectory;
import com.ibm.crail.CrailFile;
import com.ibm.crail.CrailFS;
import com.ibm.crail.CrailNode;
import com.ibm.crail.CrailNodeType;
import com.ibm.crail.conf.CrailConfiguration;
import com.ibm.crail.conf.CrailConstants;
import com.ibm.crail.namenode.rpc.NameNodeProtocol;
import com.ibm.crail.utils.CrailUtils;

public class CrailHadoopFileSystem extends FileSystem {
	private static final Logger LOG = CrailUtils.getLogger();
	private CrailFS dfs;
	private Path workingDir;
	private URI uri;
	private int localAffinity;
	
	public CrailHadoopFileSystem() throws IOException {
		LOG.info("CrailHadoopFileSystem construction");
		dfs = null;
	}
	
	@Override
	public void initialize(URI uri, Configuration conf) throws IOException {
		super.initialize(uri, conf);
		setConf(conf);
		
		try {
			CrailConfiguration crailConf = new CrailConfiguration();
			CrailHDFSConstants.updateConstants(crailConf);
			CrailHDFSConstants.printConf(LOG);			
			this.dfs = CrailFS.newInstance(crailConf);
			this.localAffinity = 0;
			if (CrailHDFSConstants.LOCAL_AFFINITY){
				localAffinity = dfs.getHostHash();
			}			
			Path _workingDir = new Path("/user/" + CrailConstants.USER);
			this.workingDir = new Path("/user/" + CrailConstants.USER).makeQualified(uri, _workingDir);	
			this.uri = URI.create(CrailConstants.NAMENODE_ADDRESS);
			LOG.info("CrailHadoopFileSystem fs initialization done..");
		} catch(Exception e){
			throw new IOException(e);
		}
	}
	
	public String getScheme() {
		return "crail";
	}

	public URI getUri() {
		return uri;
	}	

	public FSDataInputStream open(Path path, int bufferSize) throws IOException {
		CrailFile fileInfo = null;
		try {
			fileInfo = dfs.lookup(path.toUri().getRawPath()).get().asFile();
			CrailBufferedInputStream inputStream = fileInfo.getBufferedInputStream(fileInfo.getCapacity());
			return new CrailHDFSInputStream(inputStream);
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	@Override
	public FSDataOutputStream create(Path path, FsPermission permission,
			boolean overwrite, int bufferSize, short replication,
			long blockSize, Progressable progress) throws IOException {
		CrailFile fileInfo = null;
		try {
			fileInfo = dfs.create(path.toUri().getRawPath(), CrailNodeType.DATAFILE, CrailHDFSConstants.STORAGE_AFFINITY, localAffinity).get().asFile();
		} catch (Exception e) {
			if (e.getMessage().contains(NameNodeProtocol.messages[NameNodeProtocol.ERR_PARENT_MISSING])) {
				fileInfo = null;
			} else {
				throw new IOException(e);
			}
		}
		
		if (fileInfo == null) {
			Path parent = path.getParent();
			this.mkdirs(parent, FsPermission.getDirDefault());
			try {
				fileInfo = dfs.create(path.toUri().getRawPath(), CrailNodeType.DATAFILE, CrailHDFSConstants.STORAGE_AFFINITY, localAffinity).get().asFile();
			} catch (Exception e) {
				throw new IOException(e);
			}
		}
		
		CrailBufferedOutputStream outputStream = null;
		if (fileInfo != null){
			try {
				fileInfo.syncDir();
				outputStream = fileInfo.getBufferedOutputStream(CrailConstants.HDFS_WRITE_AHEAD);
			} catch (Exception e) {
				throw new IOException(e);
			}
		}
		
		if (outputStream != null){
			return new CrailHDFSOutputStream(outputStream, statistics);					
		} else {
			throw new IOException("Failed to create file, path " + path.toString());
		}
	}

	@Override
	public FSDataOutputStream append(Path path, int bufferSize, Progressable progress) throws IOException {
		throw new IOException("Append not supported");
	}

	@Override
	public boolean rename(Path src, Path dst) throws IOException {
		try {
			CrailNode file = dfs.rename(src.toUri().getRawPath(), dst.toUri().getRawPath()).get();
			if (file != null){
				file.syncDir();
			}
			return file != null;
		} catch(Exception e){
			throw new IOException(e);
		}
	}

	@Override
	public boolean delete(Path path, boolean recursive) throws IOException {
		try {
			CrailNode file = dfs.delete(path.toUri().getRawPath(), recursive).get();
			if (file != null){
				file.syncDir();
			}
			return file != null;
		} catch(Exception e){
			throw new IOException(e);
		}
	}

	@Override
	public FileStatus[] listStatus(Path path) throws FileNotFoundException, IOException {
		try {
			CrailNode node = dfs.lookup(path.toUri().getRawPath()).get();
			Iterator<String> iter = node.getType() == CrailNodeType.DIRECTORY ? node.asDirectory().listEntries() : node.asMultiFile().listEntries(); 
			ArrayList<FileStatus> statusList = new ArrayList<FileStatus>();
			while(iter.hasNext()){
				String filepath = iter.next();
				CrailNode directFile = dfs.lookup(filepath).get();
				if (directFile != null){
					FsPermission permission = FsPermission.getFileDefault();
					if (directFile.getType().isDirectory()) {
						permission = FsPermission.getDirDefault();
					}
					FileStatus status = new FileStatus(directFile.getCapacity(), directFile.getType().isContainer(), CrailConstants.SHADOW_REPLICATION, CrailConstants.BLOCK_SIZE, directFile.getModificationTime(), directFile.getModificationTime(), permission, CrailConstants.USER, CrailConstants.USER, new Path(filepath).makeQualified(this.getUri(), this.workingDir));	
					statusList.add(status);
				}
			}
			FileStatus[] list = new FileStatus[statusList.size()];
			statusList.toArray(list);
			return list;
		} catch(Exception e){
			throw new FileNotFoundException(path.toUri().getRawPath());
		}
	}

	@Override
	public void setWorkingDirectory(Path new_dir) {
		this.workingDir = new_dir;
	}

	@Override
	public Path getWorkingDirectory() {
		return this.workingDir;
	}

	@Override
	public boolean mkdirs(Path path, FsPermission permission) throws IOException {
		try {
			CrailDirectory file = dfs.create(path.toUri().getRawPath(), CrailNodeType.DIRECTORY, 0, 0).get().asDirectory();
			file.syncDir();
			return true;
		} catch(Exception e){
			if (e.getMessage().contains(NameNodeProtocol.messages[NameNodeProtocol.ERR_PARENT_MISSING])){
				Path parent = path.getParent();
				mkdirs(parent);
				return mkdirs(path);
			} else if (e.getMessage().contains(NameNodeProtocol.messages[NameNodeProtocol.ERR_FILE_EXISTS])){
				return true;
			} else {
				throw new IOException(e);
			}
		}
	}

	@Override
	public FileStatus getFileStatus(Path path) throws IOException {
		CrailNode directFile = null;
		try {
			directFile = dfs.lookup(path.toUri().getRawPath()).get();
		} catch (Exception e) {
			throw new IOException(e);
		}
		if (directFile == null) {
			throw new FileNotFoundException("File does not exist: " + path);
		}
		FsPermission permission = FsPermission.getFileDefault();
		if (directFile.getType().isDirectory()) {
			permission = FsPermission.getDirDefault();
		}
		FileStatus status = new FileStatus(directFile.getCapacity(), directFile.getType().isContainer(), CrailConstants.SHADOW_REPLICATION, CrailConstants.BLOCK_SIZE, directFile.getModificationTime(), directFile.getModificationTime(), permission, CrailConstants.USER, CrailConstants.USER, path.makeQualified(this.getUri(), this.workingDir));
		return status;
	}

	@Override
	public BlockLocation[] getFileBlockLocations(FileStatus file, long start, long len) throws IOException {
		try {
			CrailBlockLocation[] _locations = dfs.lookup(file.getPath().toUri().getRawPath()).get().asFile().getBlockLocations(start, len);
			BlockLocation[] locations = new BlockLocation[_locations.length];
			for (int i = 0; i < locations.length; i++){
				locations[i] = new BlockLocation();
				locations[i].setOffset(_locations[i].getOffset());
				locations[i].setLength(_locations[i].getLength());
				locations[i].setNames(_locations[i].getNames());
				locations[i].setHosts(_locations[i].getHosts());
				locations[i].setTopologyPaths(_locations[i].getTopology());
				
			}			
			return locations;
		} catch(Exception e){
			throw new IOException(e);
		}
	}

	@Override
	public BlockLocation[] getFileBlockLocations(Path path, long start, long len) throws IOException {
		try {
			CrailBlockLocation[] _locations = dfs.lookup(path.toUri().getRawPath()).get().asFile().getBlockLocations(start, len);
			BlockLocation[] locations = new BlockLocation[_locations.length];
			for (int i = 0; i < locations.length; i++){
				locations[i] = new BlockLocation();
				locations[i].setOffset(_locations[i].getOffset());
				locations[i].setLength(_locations[i].getLength());
				locations[i].setNames(_locations[i].getNames());
				locations[i].setHosts(_locations[i].getHosts());
				locations[i].setTopologyPaths(_locations[i].getTopology());
				
			}			
			return locations;
		} catch(Exception e){
			throw new IOException(e);
		}
	}
	
	@Override
	public FsStatus getStatus(Path p) throws IOException {
		statistics.incrementReadOps(1);
		return new FsStatus(Long.MAX_VALUE, 0, Long.MAX_VALUE);
	}
	
	@Override
	public void close() throws IOException {
		try {
			LOG.info("Closing CrailHadoopFileSystem");
			super.processDeleteOnExit();
			dfs.close();
		} catch (Exception e) {
			e.printStackTrace();
			throw new IOException(e);
		}
	}
}

