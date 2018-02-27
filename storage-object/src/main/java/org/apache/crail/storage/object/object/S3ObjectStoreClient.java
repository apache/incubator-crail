/*
 * Copyright (C) 2015-2018, IBM Corporation
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

package org.apache.crail.storage.object.object;

import com.amazonaws.*;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import org.apache.crail.CrailBuffer;
import org.apache.crail.conf.CrailConstants;
import org.apache.crail.storage.object.ObjectStoreConstants;
import org.apache.crail.storage.object.ObjectStoreUtils;
import org.slf4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.ConcurrentHashMap;

public class S3ObjectStoreClient {
	private static final Logger LOG = ObjectStoreUtils.getLogger();

	private final AmazonS3Client[] connections;
	//private final HashMap<String, String> metadata;
	private final ConcurrentHashMap<Long, ObjectMetadata> objectMetadata;

	public S3ObjectStoreClient() throws IOException {
		System.setProperty("com.amazonaws.services.s3.disableGetObjectMD5Validation", "true");

		AWSCredentials credentials = new BasicAWSCredentials(ObjectStoreConstants.S3_ACCESS,
				ObjectStoreConstants.S3_SECRET);
		LOG.debug("Using S3 credentials AccessKey={} SecretKey={}",
				ObjectStoreConstants.S3_ACCESS, ObjectStoreConstants.S3_SECRET);

		ClientConfiguration clientConf = new ClientConfiguration();
		clientConf.setProtocol(Protocol.valueOf(ObjectStoreConstants.S3_PROTOCOL));
		clientConf.withSignerOverride("NoOpSignerType");
		clientConf.setSocketBufferSizeHints(64 * 1024, 64 * 1024);
		clientConf.withUseExpectContinue(true);
		clientConf.withThrottledRetries(false);
		clientConf.setSocketTimeout(3000000);
		clientConf.setConnectionTimeout(3000000);
		ApacheHttpClientConfig httpConf = clientConf.getApacheHttpClientConfig();
		String[] endpoints = ObjectStoreConstants.S3_ENDPOINT.split(",");
		//clientConf.withSignerOverride("S3SignerType");
		//clientConf.withSignerOverride(null);
		//clientConf.setSignerOverride(null);
		connections = new AmazonS3Client[endpoints.length];
		int i = 0;
		for (String endpoint : endpoints) {
			connections[i] = new AmazonS3Client(credentials, clientConf);
			LOG.debug("Adding S3 connection to endpoint {}", endpoint);
			connections[i].setEndpoint(endpoint);
			if (ObjectStoreConstants.S3_REGION_NAME != null) {
				connections[i].setRegion(Region.getRegion(Regions.valueOf(ObjectStoreConstants.S3_REGION_NAME)));
			}
			//runTest(connections[i]);
			i++;
		}
		//S3ClientOptions opts = S3ClientOptions.builder().disableChunkedEncoding().build();
		//connection.setS3ClientOptions(opts);
		//metadata = new HashMap<>();
		//metadata.put("CrailClient", InetAddress.getLocalHost().getHostName());
		//metadata.put("Endpoint", ObjectStoreConstants.S3_ENDPOINT);
		objectMetadata = new ConcurrentHashMap<Long, ObjectMetadata>();
		LOG.debug("Successfully created S3Client");
	}

	public void runTest(AmazonS3Client conn) {
		LOG.info("--------------------------------------------------------------------------------------------------");
		LOG.info("Test S3 connection by writing and reading an object");
		LOG.info("--------------------------------------------------------------------------------------------------");
		try {
			ByteArrayInputStream buffer = new ByteArrayInputStream("Hello World!".getBytes());
			int length = buffer.available();
			ObjectMetadata md = new ObjectMetadata();
			md.setContentType("application/octet-stream");
			md.setContentLength(length);
			PutObjectRequest putReg = new PutObjectRequest(ObjectStoreConstants.S3_BUCKET_NAME, "Test", buffer, md);
			conn.putObject(putReg);
		} catch (Exception e) {
			LOG.error("putObject() got exception: ", e);
		}
		try {
			GetObjectRequest getReq = new GetObjectRequest(ObjectStoreConstants.S3_BUCKET_NAME, "Test");
			S3Object object = conn.getObject(getReq);
			System.out.println("Got back object Test=" + object.getObjectContent());
		} catch (Exception e) {
			LOG.error("getObject() got exception: ", e);
		}
	}

	public void init() {
		//createBucket(ObjectStoreConstants.S3_BUCKET_NAME);
	}

	public void close() {
		LOG.info("Closing S3 Client");
		//deleteBucket(ObjectStoreConstants.S3_BUCKET_NAME);
	}

	public InputStream getObject(String key) throws IOException {
		int endpointID = getEndpoit(key);
		AmazonS3 connection = connections[endpointID];
		GetObjectRequest objReq = new GetObjectRequest(ObjectStoreConstants.S3_BUCKET_NAME, key);
		S3Object object = connection.getObject(objReq);
		return object.getObjectContent();
	}

	private int getEndpoit(String key) {
		// select an endpoint based on the block ID
		int id = Integer.valueOf(key.split("-")[5]) % connections.length;
		return id;
	}

	public InputStream getObject(String key, long startOffset, long endOffset) throws IOException {
		int endpointID = getEndpoit(key);
		AmazonS3 connection = connections[endpointID];
		GetObjectRequest objReq = new GetObjectRequest(ObjectStoreConstants.S3_BUCKET_NAME, key);
		LOG.debug("TID {} : Getting object {}, start offset = {}, end offset = {} ",
				Thread.currentThread().getId(), key, startOffset, endOffset);
		if (startOffset > 0 || endOffset != CrailConstants.BLOCK_SIZE) {
			// NOTE: start and end offset are inclusive in the S3 API.
			LOG.debug("TID {} : Setting object range", Thread.currentThread().getId());
			objReq.withRange(startOffset, endOffset - 1);
		}
		S3Object object;
		if (ObjectStoreConstants.PROFILE) {
			long startTime = System.nanoTime();
			object = connection.getObject(objReq);
			long endTime = System.nanoTime();
			LOG.debug("TID {} : S3 endpoint {} getObject() initial response in {} (usec)",
					Thread.currentThread().getId(), endpointID, (endTime - startTime) / 1000.);
		} else {
			object = connection.getObject(objReq);
		}
		return object.getObjectContent();
	}

	public void putObject(String key, CrailBuffer buffer) throws IOException {
		int length = buffer.remaining();
		InputStream input = new ObjectStoreUtils.ByteBufferBackedInputStream(buffer);
		ObjectMetadata md = getObjectMetadata();
		md.setContentLength(length);
		PutObjectRequest request = new PutObjectRequest(ObjectStoreConstants.S3_BUCKET_NAME, key, input, md);
		//request.getRequestClientOptions().setReadLimit(buffer.remaining());
		int endpointID = getEndpoit(key);
		AmazonS3 connection = connections[endpointID];
		if (ObjectStoreConstants.PROFILE) {
			long startTime = System.nanoTime();
			connection.putObject(request);
			long endTime = System.nanoTime();
			LOG.debug("TID {} : S3 putObject of {} bytes took {} (usec) from endpoint {}",
					Thread.currentThread().getId(), length, (endTime - startTime) / 1000., endpointID);
		} else {
			connection.putObject(request);
		}
	}

	public ObjectMetadata getObjectMetadata() {
		long threadId = Thread.currentThread().getId();
		ObjectMetadata md = objectMetadata.get(threadId);
		if (md == null) {
			md = new ObjectMetadata();
			//md.setUserMetadata(metadata);
			md.setContentType("application/octet-stream");
			objectMetadata.put(threadId, md);
		}
		return md;
	}

	public void deleteObject(String key) throws IOException {
		int endpointID = getEndpoit(key);
		AmazonS3 connection = connections[endpointID];
		try {
			connection.deleteObject(new DeleteObjectRequest(ObjectStoreConstants.S3_BUCKET_NAME, key));
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	public boolean createBucket(String bucket) {
		try {
			if (!(connections[0].doesBucketExist(bucket))) {
				connections[0].createBucket(new CreateBucketRequest(bucket));
				String bucketLocation = connections[0].getBucketLocation(new GetBucketLocationRequest(bucket));
				LOG.debug("Created new bucket {} in location {}", bucket, bucketLocation);
			} else {
				LOG.debug("Bucket {} already exists", bucket);
			}
			return true;
		} catch (AmazonServiceException ase) {
			LOG.error("AmazonServiceException (the request to the Object Store was rejected with an error message):");
			LOG.error("Error Message:    " + ase.getMessage());
			LOG.error("HTTP Status Code: " + ase.getStatusCode());
			LOG.error("AWS Error Code:   " + ase.getErrorCode());
			LOG.error("Error Type:       " + ase.getErrorType());
			LOG.error("Request ID:       " + ase.getRequestId());
		} catch (AmazonClientException ace) {
			LOG.error("AmazonClientException (the client encountered an internal error while trying to " +
					"communicate with the Object Store):");
			LOG.error("Error Message: " + ace.getMessage());
		}
		return false;
	}

	public boolean deleteBucket(String bucket) {
		try {
			connections[0].deleteBucket(bucket);
			LOG.debug("Deleted bucket {}", bucket);
			return true;
		} catch (AmazonServiceException ase) {
			LOG.error("AmazonServiceException (the request to the Object Store was rejected with an error message):");
			LOG.error("Error Message:    " + ase.getMessage());
			LOG.error("HTTP Status Code: " + ase.getStatusCode());
			LOG.error("AWS Error Code:   " + ase.getErrorCode());
			LOG.error("Error Type:       " + ase.getErrorType());
			LOG.error("Request ID:       " + ase.getRequestId());
		} catch (AmazonClientException ace) {
			LOG.error("AmazonClientException (the client encountered an internal error while trying to " +
					"communicate with the Object Store):");
			LOG.error("Error Message: " + ace.getMessage());
		}
		return false;
	}
}
