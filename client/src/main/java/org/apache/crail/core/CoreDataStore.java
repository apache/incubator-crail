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

package org.apache.crail.core;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.crail.CrailBlockLocation;
import org.apache.crail.CrailBuffer;
import org.apache.crail.CrailStore;
import org.apache.crail.CrailLocationClass;
import org.apache.crail.CrailNode;
import org.apache.crail.CrailNodeType;
import org.apache.crail.CrailResult;
import org.apache.crail.CrailStatistics;
import org.apache.crail.CrailStorageClass;
import org.apache.crail.Upcoming;
import org.apache.crail.conf.CrailConfiguration;
import org.apache.crail.conf.CrailConstants;
import org.apache.crail.memory.BufferCache;
import org.apache.crail.metadata.BlockInfo;
import org.apache.crail.metadata.DataNodeInfo;
import org.apache.crail.metadata.FileInfo;
import org.apache.crail.metadata.FileName;
import org.apache.crail.rpc.RpcClient;
import org.apache.crail.rpc.RpcConnection;
import org.apache.crail.rpc.RpcCreateFile;
import org.apache.crail.rpc.RpcDeleteFile;
import org.apache.crail.rpc.RpcDispatcher;
import org.apache.crail.rpc.RpcErrors;
import org.apache.crail.rpc.RpcFuture;
import org.apache.crail.rpc.RpcGetFile;
import org.apache.crail.rpc.RpcGetLocation;
import org.apache.crail.rpc.RpcPing;
import org.apache.crail.rpc.RpcRenameFile;
import org.apache.crail.storage.StorageClient;
import org.apache.crail.utils.BlockCache;
import org.apache.crail.utils.BufferCheckpoint;
import org.apache.crail.utils.CrailUtils;
import org.apache.crail.utils.EndpointCache;
import org.apache.crail.utils.NextBlockCache;
import org.apache.crail.utils.BlockCache.FileBlockCache;
import org.apache.crail.utils.NextBlockCache.FileNextBlockCache;
import org.slf4j.Logger;

public class CoreDataStore extends CrailStore {
	private static final Logger LOG = CrailUtils.getLogger();
	private static AtomicInteger fsCount = new AtomicInteger(0);

	//namenode operations
	private RpcClient rpcClient;
	private RpcConnection rpcConnection;

	//datanode operations
	private EndpointCache datanodeEndpointCache;

	private AtomicLong streamCounter;
	private ConcurrentHashMap<Long, CoreInputStream> openInputStreams;
	private ConcurrentHashMap<Long, CoreOutputStream> openOutputStreams;

	private BlockCache blockCache;
	private NextBlockCache nextBlockCache;
	private BufferCache bufferCache;
	private BufferCheckpoint bufferCheckpoint;
	private ConcurrentHashMap<String, String> locationMap;

	private boolean isOpen;
	private int fsId;
	private CrailLocationClass localClass;

	private CoreIOStatistics ioStatsIn;
	private CoreIOStatistics ioStatsOut;
	private CoreStreamStatistics streamStats;
	private CrailStatistics statistics;

	public CoreDataStore(CrailConfiguration conf) throws Exception {
		CrailConstants.updateConstants(conf);
		CrailConstants.printConf();
		CrailConstants.verify();

		this.bufferCache = BufferCache.createInstance(CrailConstants.CACHE_IMPL);
		this.statistics = new CrailStatistics();

		//Datanodes
		StringTokenizer tokenizer = new StringTokenizer(CrailConstants.STORAGE_TYPES, ",");
		LinkedList<StorageClient> dataNodeClients = new LinkedList<StorageClient>();
		while (tokenizer.hasMoreTokens()){
			String name = tokenizer.nextToken();
			StorageClient dataNode = StorageClient.createInstance(name);
			dataNode.init(statistics, bufferCache, conf, null);
			dataNode.printConf(LOG);
			dataNodeClients.add(dataNode);
		}
		this.datanodeEndpointCache = new EndpointCache(fsId, dataNodeClients);

		//Namenode
		InetSocketAddress nnAddr = CrailUtils.getNameNodeAddress();
		this.rpcClient = RpcClient.createInstance(CrailConstants.NAMENODE_RPC_TYPE);
		rpcClient.init(conf, null);
		rpcClient.printConf(LOG);
		ConcurrentLinkedQueue<InetSocketAddress> namenodeList = CrailUtils.getNameNodeList();
		ConcurrentLinkedQueue<RpcConnection> connectionList = new ConcurrentLinkedQueue<RpcConnection>();
		while(!namenodeList.isEmpty()){
			InetSocketAddress address = namenodeList.poll();
			RpcConnection connection = rpcClient.connect(address);
			connectionList.add(connection);
		}
		if (connectionList.size() == 1){
			this.rpcConnection = connectionList.poll();
		} else {
			this.rpcConnection = new RpcDispatcher(connectionList);
		}
		LOG.info("connected to namenode(s) " + rpcConnection);

		//Client
		this.fsId = fsCount.getAndIncrement();
		this.localClass = CrailUtils.getLocationClass();
		this.blockCache = new BlockCache();
		this.nextBlockCache = new NextBlockCache();
		this.openInputStreams = new ConcurrentHashMap<Long, CoreInputStream>();
		this.openOutputStreams = new ConcurrentHashMap<Long, CoreOutputStream>();
		this.streamCounter = new AtomicLong(0);
		this.isOpen = true;
		this.bufferCheckpoint = new BufferCheckpoint();
		this.locationMap = new ConcurrentHashMap<String, String>();
		CrailUtils.parseMap(CrailConstants.LOCATION_MAP, locationMap);

		this.ioStatsIn = new CoreIOStatistics("core/input");
		statistics.addProvider(ioStatsIn);
		this.ioStatsOut = new CoreIOStatistics("core/output");
		statistics.addProvider(ioStatsOut);
		this.streamStats = new CoreStreamStatistics();
		statistics.addProvider(streamStats);
		statistics.addProvider(bufferCache);
		statistics.addProvider(datanodeEndpointCache);
	}

	public Upcoming<CrailNode> create(String path, CrailNodeType type, CrailStorageClass storageClass, CrailLocationClass locationClass, boolean enumerable) throws Exception {
		FileName name = new FileName(path);

		if (CrailConstants.DEBUG){
			LOG.info("createNode: name " + path + ", type " + type + ", storageAffinity " + storageClass + ", locationAffinity " + locationClass);
		}

		RpcFuture<RpcCreateFile> fileRes = rpcConnection.createFile(name, type, storageClass.value(), locationClass.value(), enumerable);
		return new CreateNodeFuture(this, path, type, fileRes);
	}

	CoreNode _createNode(String path, CrailNodeType type, RpcCreateFile fileRes) throws Exception {
		if (fileRes.getError() == RpcErrors.ERR_PARENT_MISSING){
			throw new IOException("createNode: " + RpcErrors.messages[fileRes.getError()] + ", name " + path);
		} else if  (fileRes.getError() == RpcErrors.ERR_FILE_EXISTS){
			throw new IOException("createNode: " + RpcErrors.messages[fileRes.getError()] + ", name " + path);
		} else if (fileRes.getError() != RpcErrors.ERR_OK){
			LOG.info("createNode: " + RpcErrors.messages[fileRes.getError()] + ", name " + path);
			throw new IOException("createNode: " + RpcErrors.messages[fileRes.getError()] + ", error " + fileRes.getError());
		}

		FileInfo fileInfo = fileRes.getFile();
		FileInfo dirInfo = fileRes.getParent();
		if (fileInfo == null || dirInfo == null){
			throw new IOException("createFile: " + RpcErrors.messages[RpcErrors.ERR_UNKNOWN]);
		}
		if (fileInfo.getType() != type){
			throw new IOException("createFile: " + "file type mismatch");
		}

		blockCache.remove(fileInfo.getFd());
		nextBlockCache.remove(fileInfo.getFd());
		CoreNode node = CoreNode.create(this, fileInfo, path);

		BlockInfo fileBlock = fileRes.getFileBlock();
		getBlockCache(fileInfo.getFd()).put(CoreSubOperation.createKey(fileInfo.getFd(), 0), fileBlock);

		//write directory record is a directory slot has been assigned to the file
		if (fileInfo.getDirOffset() >= 0){
			BlockInfo dirBlock = fileRes.getDirBlock();
			getBlockCache(dirInfo.getFd()).put(CoreSubOperation.createKey(dirInfo.getFd(), fileInfo.getDirOffset()), dirBlock);
			CoreSyncOperation syncOperation = getSyncOperation(dirInfo, fileInfo, path, true);
			node.addSyncOperation(syncOperation);
		}

		if (CrailConstants.DEBUG){
			LOG.info("createFile: name " + path + ", success, fd " + fileInfo.getFd() + ", token " + fileInfo.getToken());
		}

		return node;
	}

	public Upcoming<CrailNode> lookup(String path) throws Exception {
		FileName name = new FileName(path);

		if (CrailConstants.DEBUG){
			LOG.info("lookupDirectory: path " + path);
		}

		RpcFuture<RpcGetFile> fileRes = rpcConnection.getFile(name, false);
		return new LookupNodeFuture(this, path, fileRes);
	}

	CoreNode _lookupNode(RpcGetFile fileRes, String path) throws Exception {
		if (fileRes.getError() == RpcErrors.ERR_GET_FILE_FAILED){
			return null;
		}
		else if (fileRes.getError() != RpcErrors.ERR_OK){
			LOG.info("lookupDirectory: " + RpcErrors.messages[fileRes.getError()]);
			return null;
		}

		FileInfo fileInfo = fileRes.getFile();

		CoreNode node = null;
		if (fileInfo != null){
			if (CrailConstants.DEBUG){
				LOG.info("lookup: name " + path + ", success, fd " + fileInfo.getFd());
			}
			BlockInfo fileBlock = fileRes.getFileBlock();
			getBlockCache(fileInfo.getFd()).put(CoreSubOperation.createKey(fileInfo.getFd(), 0), fileBlock);

			node = CoreNode.create(this, fileInfo, path);
		}
		return node;
	}


	public Upcoming<CrailNode> rename(String src, String dst) throws Exception {
		FileName srcPath = new FileName(src);
		FileName dstPath = new FileName(dst);

		if (CrailConstants.DEBUG){
			LOG.info("rename: srcname " + src + ", dstname " + dst);
		}

		RpcFuture<RpcRenameFile> renameRes = rpcConnection.renameFile(srcPath, dstPath);
		return new RenameNodeFuture(this, src, dst, renameRes);
	}

	CrailNode _rename(RpcRenameFile renameRes, String src, String dst) throws Exception {
		if (renameRes.getError() == RpcErrors.ERR_SRC_FILE_NOT_FOUND){
			LOG.info("rename: " + RpcErrors.messages[renameRes.getError()]);
			return null;
		}
		if (renameRes.getError() == RpcErrors.ERR_DST_PARENT_NOT_FOUND){
			LOG.info("rename: " + RpcErrors.messages[renameRes.getError()]);
			return null;
		}
		if (renameRes.getError() == RpcErrors.ERR_FILE_EXISTS){
			LOG.info("rename: " + RpcErrors.messages[renameRes.getError()]);
			return null;
		}
		if (renameRes.getError() != RpcErrors.ERR_OK){
			LOG.info("rename: " + RpcErrors.messages[renameRes.getError()]);
			throw new IOException(RpcErrors.messages[renameRes.getError()]);
		}
		if (renameRes.getDstParent().getCapacity() < renameRes.getDstFile().getDirOffset() + CrailConstants.DIRECTORY_RECORD){
			LOG.info("rename: parent capacity does not match dst file offset, capacity " + renameRes.getDstParent().getCapacity() + ", offset " + renameRes.getDstFile().getDirOffset());
		}

		FileInfo srcParent = renameRes.getSrcParent();
		FileInfo srcFile = renameRes.getSrcFile();
		FileInfo dstDir = renameRes.getDstParent();
		FileInfo dstFile = renameRes.getDstFile();

		BlockInfo srcBlock = renameRes.getSrcBlock();
		getBlockCache(srcParent.getFd()).put(CoreSubOperation.createKey(srcParent.getFd(), srcFile.getDirOffset()), srcBlock);
		BlockInfo dirBlock = renameRes.getDstBlock();
		getBlockCache(dstDir.getFd()).put(CoreSubOperation.createKey(dstDir.getFd(), dstFile.getDirOffset()), dirBlock);

		CoreSyncOperation syncOperationSrc = getSyncOperation(srcParent, srcFile, src, false);
		CoreSyncOperation syncOperationDst = getSyncOperation(dstDir, dstFile, dst, true);

		blockCache.remove(srcFile.getFd());

		if (CrailConstants.DEBUG){
			LOG.info("rename: srcname " + src + ", dstname " + dst + ", success");
		}

		CoreNode node = CoreNode.create(this, dstFile, dst);
		node.addSyncOperation(syncOperationSrc);
		node.addSyncOperation(syncOperationDst);
		return node;
	}

	public Upcoming<CrailNode> delete(String path, boolean recursive) throws Exception {
		FileName name = new FileName(path);

		if (CrailConstants.DEBUG){
			LOG.info("delete: name " + path + ", recursive " + recursive);
		}

		RpcFuture<RpcDeleteFile> fileRes = rpcConnection.removeFile(name, recursive);
		return new DeleteNodeFuture(this, path, recursive, fileRes);
	}

	CrailNode _delete(RpcDeleteFile fileRes, String path, boolean recursive) throws Exception {
		if (fileRes.getError() == RpcErrors.ERR_HAS_CHILDREN) {
			LOG.info("delete: " + RpcErrors.messages[fileRes.getError()]);
			throw new IOException(RpcErrors.messages[fileRes.getError()]);
		}
		if (fileRes.getError() != RpcErrors.ERR_OK) {
			LOG.info("delete: " + RpcErrors.messages[fileRes.getError()]);
			return null;
		}

		FileInfo fileInfo = fileRes.getFile();
		FileInfo dirInfo = fileRes.getParent();

		CoreSyncOperation syncOperation = getSyncOperation(dirInfo, fileInfo, path, false);

		blockCache.remove(fileInfo.getFd());

		if (CrailConstants.DEBUG){
			LOG.info("delete: name " + path + ", recursive " + recursive + ", success");
		}

		CoreNode node = CoreNode.create(this, fileInfo, path);
		node.addSyncOperation(syncOperation);
		return node;
	}

	public DirectoryInputStream listEntries(String name) throws Exception {
		return _listEntries(name, CrailConstants.DIRECTORY_RANDOMIZE);
	}

	public DirectoryInputStream _listEntries(String name, boolean randomize) throws Exception {
		FileName directory = new FileName(name);

		if (CrailConstants.DEBUG){
			LOG.info("getDirectoryList: " + name);
		}

		RpcGetFile fileRes = rpcConnection.getFile(directory, false).get(CrailConstants.RPC_TIMEOUT, TimeUnit.MILLISECONDS);
		if (fileRes.getError() != RpcErrors.ERR_OK) {
			LOG.info("getDirectoryList: " + RpcErrors.messages[fileRes.getError()]);
			throw new FileNotFoundException(RpcErrors.messages[fileRes.getError()]);
		}

		FileInfo dirInfo = fileRes.getFile();
		if (!dirInfo.getType().isContainer()){
			LOG.info("getDirectoryList: " + RpcErrors.messages[RpcErrors.ERR_FILE_IS_NOT_DIR]);
			throw new FileNotFoundException(RpcErrors.messages[RpcErrors.ERR_FILE_IS_NOT_DIR]);
		}

		CoreDirectory dirFile = new CoreDirectory(this, dirInfo, name);
		DirectoryInputStream inputStream = dirFile.getDirectoryInputStream(randomize);
		return inputStream;
	}

	public CrailBlockLocation[] getBlockLocations(String path, long start, long len) throws Exception {
		if (CrailConstants.DEBUG){
			LOG.info("location: path " + path + ", start " + start + ", len " + len);
		}

		if (path == null) {
			LOG.info("Path null");
			return null;
		}
		if (start < 0 || len < 0) {
			LOG.info("Start or len invalid");
			throw new IOException("Invalid start or len parameter");
		}


		FileName name = new FileName(path);
		long rangeStart = CrailUtils.blockStartAddress(start);
		long range = start + len - CrailUtils.blockStartAddress(start);
		long blockCount = range / CrailConstants.BLOCK_SIZE;
		if (range % CrailConstants.BLOCK_SIZE > 0){
			blockCount++;
		}
		CoreBlockLocation[] blockLocations = new CoreBlockLocation[(int) blockCount];
		HashMap<Long, DataNodeInfo> dataNodeSet = new HashMap<Long, DataNodeInfo>();
		HashMap<Long, DataNodeInfo> offset2DataNode = new HashMap<Long, DataNodeInfo>();

		for (long current = CrailUtils.blockStartAddress(start); current < start + len; current += CrailConstants.BLOCK_SIZE){
			RpcGetLocation getLocationRes = rpcConnection.getLocation(name, current).get(CrailConstants.RPC_TIMEOUT, TimeUnit.MILLISECONDS);
			if (getLocationRes.getError() != RpcErrors.ERR_OK) {
				LOG.info("location: " + RpcErrors.messages[getLocationRes.getError()]);
				throw new IOException(RpcErrors.messages[getLocationRes.getError()]);
			}

			DataNodeInfo dataNodeInfo = getLocationRes.getBlockInfo().getDnInfo();
			dataNodeSet.put(dataNodeInfo.key(), dataNodeInfo);
			CoreBlockLocation location = new CoreBlockLocation();
			location.setOffset(current);
			location.setLength(Math.min(start + len - current, CrailConstants.BLOCK_SIZE));
			long index = (current - rangeStart) / CrailConstants.BLOCK_SIZE;
			blockLocations[(int) index] = location;
			offset2DataNode.put(current, dataNodeInfo);
		}

		//asign an identifier to each data node
		ArrayList<DataNodeInfo> dataNodeArray = new ArrayList<DataNodeInfo>(dataNodeSet.size());
		int index = 0;
		for (DataNodeInfo dn : dataNodeSet.values()){
			dataNodeArray.add(index, dn);
			index++;
		}

		int locationSize = Math.min(CrailConstants.SHADOW_REPLICATION, dataNodeSet.size());
		int blockIndex = 0;
		for (int i = 0; i < blockLocations.length; i++){
			CoreBlockLocation location = blockLocations[i];
			String[] hosts = new String[locationSize];
			String[] names = new String[locationSize];
			String[] topology = new String[locationSize];
			int[] storageType = new int[locationSize];
			int[] storageClass = new int[locationSize];
			int[] locationTiers = new int[locationSize];

			DataNodeInfo dnInfo = offset2DataNode.get(location.getOffset());
			DataNodeInfo mainDataNode = dataNodeSet.get(dnInfo.key());
			InetSocketAddress address = CrailUtils.datanodeInfo2SocketAddr(mainDataNode);
			names[0] = getMappedLocation(address.getAddress().getCanonicalHostName()) + ":" + address.getPort();
			hosts[0] = getMappedLocation(address.getAddress().getCanonicalHostName());
			topology[0] = "/default-rack/" + names[0];
			storageType[0] = mainDataNode.getStorageType();
			storageClass[0] = mainDataNode.getStorageClass();
			locationTiers[0] = mainDataNode.getLocationClass();
			for (int j = 1; j < locationSize; j++){
				DataNodeInfo replicaDataNode = dataNodeArray.get(blockIndex);
				address = CrailUtils.datanodeInfo2SocketAddr(replicaDataNode);
				names[j] = getMappedLocation(address.getAddress().getCanonicalHostName()) + ":" + address.getPort();
				hosts[j] = getMappedLocation(address.getAddress().getCanonicalHostName());
				topology[j] = "/default-rack/" + names[j];
				storageType[j] = replicaDataNode.getStorageType();
				storageClass[j] = replicaDataNode.getStorageClass();
				locationTiers[j] = replicaDataNode.getLocationClass();
				blockIndex = (blockIndex + 1) % dataNodeArray.size();
			}
			location.setNames(names);
			location.setHosts(hosts);
			location.setTopologyPaths(topology);
			location.setStorageTypes(storageType);
			location.setStorageClasses(storageClass);
			location.setLocationAffinities(locationTiers);
		}

		return blockLocations;
	}

	public void dumpNameNode() throws Exception {
		rpcConnection.dumpNameNode().get(CrailConstants.RPC_TIMEOUT, TimeUnit.MILLISECONDS);
	}

	public void ping() throws Exception {
		RpcPing pingRes = rpcConnection.pingNameNode().get(CrailConstants.RPC_TIMEOUT, TimeUnit.MILLISECONDS);
		if (pingRes.getError() != RpcErrors.ERR_OK) {
			LOG.info("Ping: " + RpcErrors.messages[pingRes.getError()]);
			throw new IOException(RpcErrors.messages[pingRes.getError()]);
		}
	}

	public CrailBuffer allocateBuffer() throws IOException {
		return this.bufferCache.allocateBuffer();
	}

	public void freeBuffer(CrailBuffer buffer) throws IOException {
		this.bufferCache.freeBuffer(buffer);
	}

	public int getFsId() {
		return fsId;
	}

	public CrailLocationClass getLocationClass() {
		return localClass;
	}

	public BufferCheckpoint getBufferCheckpoint() {
		return bufferCheckpoint;
	}

	public CrailStatistics getStatistics(){
		return statistics;
	}

	public void closeFileSystem() throws Exception {
		if (!isOpen) {
			return;
		}

		LinkedList<CoreInputStream> tmpIn = new LinkedList<CoreInputStream>();
		for (CoreInputStream stream : openInputStreams.values()) {
			tmpIn.add(stream);
		}
		for (CoreInputStream stream : tmpIn) {
			stream.close();
		}

		LinkedList<CoreOutputStream> tmpOut = new LinkedList<CoreOutputStream>();
		LinkedList<CoreOutputStream> tmpOutDir = new LinkedList<CoreOutputStream>();
		for (CoreOutputStream stream : openOutputStreams.values()) {
			if (stream.getFile().getType().isContainer()){
				tmpOutDir.add(stream);
			} else {
				tmpOut.add(stream);
			}
		}
		for (CoreOutputStream stream : tmpOut) {
			stream.close();
		}
		for (CoreOutputStream stream : tmpOutDir) {
			stream.close();
		}

		bufferCache.close();
		datanodeEndpointCache.close();
		rpcConnection.close();
		rpcClient.close();
		this.isOpen = false;
	}

	public void closeFile(FileInfo fileInfo) throws Exception {
		if (fileInfo.getToken() > 0){
			rpcConnection.setFile(fileInfo, true).get(CrailConstants.RPC_TIMEOUT, TimeUnit.MILLISECONDS);
		}
	}

	public BufferCache getBufferCache() {
		return bufferCache;
	}

	public void purgeCache() {
		blockCache.purge();
		nextBlockCache.purge();
	}

	//-------------------------------------------------------------

	CoreOutputStream getOutputStream(CoreNode file, long writeHint) throws Exception {
		CoreOutputStream outputStream = new CoreOutputStream(file, streamCounter.incrementAndGet(), writeHint);
		openOutputStreams.put(outputStream.getStreamId(), outputStream);

		if (CrailConstants.STATISTICS){
			streamStats.incOpen();
			streamStats.incOpenOutput();
			streamStats.incCurrentOutput();
			streamStats.incMaxOutput();
			if (file.getType().isDirectory()){
				streamStats.incOpenOutputDir();
			}
		}
		return outputStream;
	}

	CoreInputStream getInputStream(CoreNode file, long readHint) throws Exception {
		CoreInputStream inputStream = new CoreInputStream(file, streamCounter.incrementAndGet(), readHint);
		openInputStreams.put(inputStream.getStreamId(), inputStream);

		if (CrailConstants.STATISTICS){
			streamStats.incOpen();
			streamStats.incOpenInput();
			streamStats.incCurrentInput();
			streamStats.incMaxInput();
			if (file.getType().isDirectory()){
				streamStats.incOpenInputDir();
			}
		}
		return inputStream;
	}

	CoreStream unregisterInputStream(CoreInputStream coreStream) {
		CoreStream stream = this.openInputStreams.remove(coreStream.getStreamId());
		if (stream != null && CrailConstants.STATISTICS){
			streamStats.incClose();
			streamStats.incCloseInput();
			this.ioStatsIn.mergeStatistics(stream.getCoreStatistics());
			streamStats.decCurrentInput();
			if (stream.getFile().getType().isDirectory()){
				streamStats.incCloseInputDir();
			}
		}

		return stream;
	}

	CoreStream unregisterOutputStream(CoreOutputStream coreStream) {
		CoreStream stream = this.openOutputStreams.remove(coreStream.getStreamId());
		if (stream != null && CrailConstants.STATISTICS){
			streamStats.incClose();
			streamStats.incCloseOutput();
			this.ioStatsOut.mergeStatistics(stream.getCoreStatistics());
			streamStats.decCurrentOutput();
			if (stream.getFile().getType().isDirectory()){
				streamStats.incCloseOutputDir();
			}
		}

		return stream;
	}

	FileBlockCache getBlockCache(long fd){
		return blockCache.getFileBlockCache(fd);
	}

	FileNextBlockCache getNextBlockCache(long fd){
		return nextBlockCache.getFileBlockCache(fd);
	}

	RpcConnection getNamenodeClientRpc() {
		return rpcConnection;
	}

	EndpointCache getDatanodeEndpointCache() {
		return datanodeEndpointCache;
	}

	String getMappedLocation(String hostname){
		String mappedValue = locationMap.get(hostname);
		return mappedValue != null ? mappedValue : hostname;
	}

	CoreSyncOperation getSyncOperation(FileInfo dirInfo, FileInfo fileInfo, String path, boolean valid) throws Exception{
		long adjustedCapacity = fileInfo.getDirOffset()*CrailConstants.DIRECTORY_RECORD + CrailConstants.DIRECTORY_RECORD;
		dirInfo.setCapacity(Math.max(dirInfo.getCapacity(), adjustedCapacity));
		CoreDirectory dirFile = new CoreDirectory(this, dirInfo, CrailUtils.getParent(path));
		DirectoryOutputStream stream = dirFile.getDirectoryOutputStream();
		DirectoryRecord record = new DirectoryRecord(valid, path);
		Future<CrailResult> future = stream.writeRecord(record, fileInfo.getDirOffset());
		CoreSyncOperation syncOperation = new CoreSyncOperation(stream, future);
		return syncOperation;
	}
}
