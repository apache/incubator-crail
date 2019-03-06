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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;

import org.slf4j.Logger;
import org.apache.crail.CrailBlockLocation;
import org.apache.crail.CrailBufferedInputStream;
import org.apache.crail.CrailBufferedOutputStream;
import org.apache.crail.CrailDirectory;
import org.apache.crail.CrailStore;
import org.apache.crail.CrailFile;
import org.apache.crail.CrailLocationClass;
import org.apache.crail.CrailNode;
import org.apache.crail.CrailNodeType;
import org.apache.crail.CrailStorageClass;
import org.apache.crail.conf.CrailConfiguration;
import org.apache.crail.conf.CrailConstants;
import org.apache.crail.rpc.RpcErrors;
import org.apache.crail.utils.CrailUtils;
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

public class CrailHadoopFileSystem extends FileSystem {
	private static final Logger LOG = CrailUtils.getLogger();
	private CrailStore dfs;
	private Path workingDir;
	private URI uri;
	
	public CrailHadoopFileSystem() throws IOException {
		LOG.info("CrailHadoopFileSystem construction");
		dfs = null;
	}
	
	@Override
	public void initialize(URI uri, Configuration conf) throws IOException {
		super.initialize(uri, conf);
		setConf(conf);
		
		try {
			CrailConfiguration crailConf = CrailConfiguration.createConfigurationFromFile();
			this.dfs = CrailStore.newInstance(crailConf);
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
		statistics.incrementReadOps(1);
		CrailFile fileInfo = null;
		try {
			fileInfo = dfs.lookup(path.toUri().getRawPath()).get().asFile();
			CrailBufferedInputStream inputStream = fileInfo.getBufferedInputStream(fileInfo.getCapacity());
			return new CrailHDFSInputStream(inputStream, statistics);
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	@Override
	public FSDataOutputStream create(Path path, FsPermission permission,
			boolean overwrite, int bufferSize, short replication,
			long blockSize, Progressable progress) throws IOException {
		statistics.incrementWriteOps(1);
		CrailFile fileInfo = null;
		try {
			fileInfo = dfs.create(path.toUri().getRawPath(), CrailNodeType.DATAFILE, CrailStorageClass.PARENT, CrailLocationClass.PARENT, true).get().asFile();
		} catch (Exception e) {
			if (e.getMessage().contains(RpcErrors.messages[RpcErrors.ERR_PARENT_MISSING])) {
				fileInfo = null;
			} else {
				throw new IOException(e);
			}
		}
		
		if (fileInfo == null) {
			Path parent = path.getParent();
			this.mkdirs(parent, FsPermission.getDirDefault());
			try {
				fileInfo = dfs.create(path.toUri().getRawPath(), CrailNodeType.DATAFILE, CrailStorageClass.PARENT, CrailLocationClass.PARENT, true).get().asFile();
			} catch (Exception e) {
				throw new IOException(e);
			}
		}
		
		CrailBufferedOutputStream outputStream = null;
		if (fileInfo != null){
			try {
				fileInfo.syncDir();
				outputStream = fileInfo.getBufferedOutputStream(Integer.MAX_VALUE);
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
			statistics.incrementWriteOps(1);
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
			statistics.incrementWriteOps(1);
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
			Iterator<String> iter = node.asContainer().listEntries();
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
			statistics.incrementWriteOps(1);
			CrailDirectory file = dfs.create(path.toUri().getRawPath(), CrailNodeType.DIRECTORY, CrailStorageClass.PARENT, CrailLocationClass.DEFAULT, true).get().asDirectory();
			file.syncDir();
			return true;
		} catch(Exception e){
			if (e.getMessage().contains(RpcErrors.messages[RpcErrors.ERR_PARENT_MISSING])){
				Path parent = path.getParent();
				mkdirs(parent);
				return mkdirs(path);
			} else if (e.getMessage().contains(RpcErrors.messages[RpcErrors.ERR_FILE_EXISTS])){
				return true;
			} else {
				throw new IOException(e);
			}
		}
	}

	@Override
	public FileStatus getFileStatus(Path path) throws IOException {
		statistics.incrementReadOps(1);
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
		return this.getFileBlockLocations(file.getPath(), start, len);
	}

	@Override
	public BlockLocation[] getFileBlockLocations(Path path, long start, long len) throws IOException {
		try {
			statistics.incrementReadOps(1);
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

