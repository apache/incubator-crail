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
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Iterator;

import org.slf4j.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.AbstractFileSystem;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.Options.ChecksumOpt;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.util.DataChecksum;
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

public class CrailHDFS extends AbstractFileSystem {
	private static final Logger LOG = CrailUtils.getLogger();
	private CrailFS dfs;
	private Path workingDir;
	private int localAffinity;
	
	public CrailHDFS(final URI uri, final Configuration conf) throws IOException, URISyntaxException {
		super(uri, "crail", true, 9000);
		
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
			LOG.info("CrailHDFS initialization done..");
		} catch(Exception e){
			throw new IOException(e);
		}
	}

	@Override
	public int getUriDefaultPort() {
		return 9000;
	}

	@Override
	public FsServerDefaults getServerDefaults() throws IOException {
		return new FsServerDefaults(CrailConstants.BLOCK_SIZE, 512, 64*1024, (short) 1, 4096, false, (long) 0, DataChecksum.Type.CRC32);
	}

	@Override
	public FSDataOutputStream createInternal(Path path, EnumSet<CreateFlag> flag, FsPermission absolutePermission, int bufferSize, short replication, long blockSize, Progressable progress, ChecksumOpt checksumOpt, boolean createParent) throws AccessControlException, FileAlreadyExistsException, FileNotFoundException, ParentNotDirectoryException, UnsupportedFileSystemException, UnresolvedLinkException, IOException {
		CrailFile fileInfo = null;
		try {
			fileInfo = dfs.create(path.toUri().getRawPath(), CrailNodeType.DATAFILE, CrailHDFSConstants.STORAGE_AFFINITY, localAffinity).get().asFile();
		} catch(Exception e){
			if (e.getMessage().contains(NameNodeProtocol.messages[NameNodeProtocol.ERR_PARENT_MISSING])){
				fileInfo = null;
			} else {
				throw new IOException(e);
			}
		}
		
		if (fileInfo == null){
			Path parent = path.getParent();
			this.mkdir(parent, FsPermission.getDirDefault(), true);
			try {
				fileInfo = dfs.create(path.toUri().getRawPath(), CrailNodeType.DATAFILE, CrailHDFSConstants.STORAGE_AFFINITY, localAffinity).get().asFile();
			} catch(Exception e){
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
		} else {
			throw new IOException("Failed to create file, path " + path.toString());
		}
		
		if (outputStream != null){
			return new CrailHDFSOutputStream(outputStream, statistics);					
		} else {
			throw new IOException("Failed to create file, path " + path.toString());
		}		
	}

	@Override
	public void mkdir(Path path, FsPermission permission, boolean createParent) throws AccessControlException, FileAlreadyExistsException, FileNotFoundException, UnresolvedLinkException, IOException {
		try {
			CrailDirectory file = dfs.create(path.toUri().getRawPath(), CrailNodeType.DIRECTORY, 0, 0).get().asDirectory();
			file.syncDir();
		} catch(Exception e){
			if (e.getMessage().contains(NameNodeProtocol.messages[NameNodeProtocol.ERR_PARENT_MISSING])){
				Path parent = path.getParent();
				mkdir(parent, permission, createParent);
				mkdir(path, permission, createParent);
			} else if (e.getMessage().contains(NameNodeProtocol.messages[NameNodeProtocol.ERR_FILE_EXISTS])){
			} else {
				throw new IOException(e);
			}			
		}
	}

	@Override
	public boolean delete(Path path, boolean recursive) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
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
	public FSDataInputStream open(Path path, int bufferSize) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
		CrailFile fileInfo = null;
		try {
			fileInfo = dfs.lookup(path.toUri().getRawPath()).get().asFile();
		} catch(Exception e){
			throw new IOException(e);
		}
		
		CrailBufferedInputStream inputStream = null;
		if (fileInfo != null){
			try {
				inputStream = fileInfo.getBufferedInputStream(fileInfo.getCapacity());
			} catch(Exception e){
				throw new IOException(e);
			}
		}
		
		if (inputStream != null){
			return new CrailHDFSInputStream(inputStream);
		} else {
			throw new IOException("Failed to open file, path " + path.toString());
		}
	}

	@Override
	public boolean setReplication(Path f, short replication) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
		return true;
	}

	@Override
	public void renameInternal(Path src, Path dst) throws AccessControlException, FileAlreadyExistsException, FileNotFoundException, ParentNotDirectoryException, UnresolvedLinkException, IOException {
		try {
			CrailNode file = dfs.rename(src.toUri().getRawPath(), dst.toUri().getRawPath()).get();
			if (file != null){
				file.syncDir();
			}
		} catch(Exception e){
			throw new IOException(e);
		}
	}

	@Override
	public void setPermission(Path f, FsPermission permission) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
	}

	@Override
	public void setOwner(Path f, String username, String groupname) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
	}

	@Override
	public void setTimes(Path f, long mtime, long atime) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
	}

	@Override
	public FileChecksum getFileChecksum(Path f) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
		return null;
	}

	@Override
	public FileStatus getFileStatus(Path path) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
		CrailNode directFile = null;
		try {
			directFile = dfs.lookup(path.toUri().getRawPath()).get();
		} catch(Exception e){
			throw new IOException(e);
		}
		if (directFile == null){
			throw new FileNotFoundException("filename " + path);
		}
		
		FsPermission permission = FsPermission.getFileDefault();
		if (directFile.getType().isDirectory()) {
			permission = FsPermission.getDirDefault();
		}		
		FileStatus status = new FileStatus(directFile.getCapacity(), directFile.getType().isContainer(), CrailConstants.SHADOW_REPLICATION, CrailConstants.BLOCK_SIZE, directFile.getModificationTime(), directFile.getModificationTime(), permission, CrailConstants.USER, CrailConstants.USER, path.makeQualified(this.getUri(), this.workingDir));
		return status;
	}

	@Override
	public BlockLocation[] getFileBlockLocations(Path path, long start, long len) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
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
	public FsStatus getFsStatus() throws AccessControlException, FileNotFoundException, IOException {
		return new FsStatus(1000000000, 1000, 1000000000 - 1000);
	}

	@Override
	public FileStatus[] listStatus(Path path) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
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
					FileStatus status = new FileStatus(directFile.getCapacity(), directFile.getType().isContainer(), CrailConstants.SHADOW_REPLICATION, CrailConstants.BLOCK_SIZE, directFile.getModificationTime(), directFile.getModificationTime(), permission, CrailConstants.USER, CrailConstants.USER, new Path(filepath).makeQualified(this.getUri(), workingDir));	
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
	public void setVerifyChecksum(boolean verifyChecksum) throws AccessControlException, IOException {
	}

}
