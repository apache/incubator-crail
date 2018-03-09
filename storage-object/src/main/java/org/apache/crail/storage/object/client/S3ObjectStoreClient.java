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

package org.apache.crail.storage.object.client;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class S3ObjectStoreClient {
	private static final Logger LOG = ObjectStoreUtils.getLogger();

	private final AmazonS3Client[] connections;
	private final ConcurrentHashMap<Long, ObjectMetadata> objectMetadata;

	public S3ObjectStoreClient() {
		System.setProperty("com.amazonaws.services.s3.disableGetObjectMD5Validation", "true");
		AWSCredentials credentials = new BasicAWSCredentials(ObjectStoreConstants.S3_ACCESS,
				ObjectStoreConstants.S3_SECRET);
		LOG.debug("Using S3 credentials AccessKey={} SecretKey={}",
				ObjectStoreConstants.S3_ACCESS, ObjectStoreConstants.S3_SECRET);

		ClientConfiguration clientConf = new ClientConfiguration();
		clientConf.setProtocol(Protocol.valueOf(ObjectStoreConstants.S3_PROTOCOL));
		clientConf.setSocketBufferSizeHints(64 * 1024, 64 * 1024);
		clientConf.withUseExpectContinue(true);
		clientConf.withThrottledRetries(false);
		clientConf.setSocketTimeout(3000000);
		clientConf.setConnectionTimeout(3000000);
		String[] endpoints = ObjectStoreConstants.S3_ENDPOINT.split(",|;");
		if (ObjectStoreConstants.S3_SIGNER != null) {
			clientConf.setSignerOverride(ObjectStoreConstants.S3_SIGNER);
		}
		connections = new AmazonS3Client[endpoints.length];
		int i = 0;
		for (String endpoint : endpoints) {
			connections[i] = new AmazonS3Client(credentials, clientConf);
			LOG.debug("Creating new connection to S3 endpoint {}", endpoint);
			connections[i].setEndpoint(endpoint);
			if (ObjectStoreConstants.S3_REGION_NAME != null) {
				connections[i].setRegion(Region.getRegion(Regions.fromName(ObjectStoreConstants.S3_REGION_NAME)));
			}
			i++;
		}
		objectMetadata = new ConcurrentHashMap<>();
		LOG.debug("Successfully created S3Client");
	}

	public boolean runBasicTests(AmazonS3Client conn) {
		LOG.debug("--------------------------------------------------------------------------------------------------");
		LOG.debug("Test S3 connection by writing and reading an object");
		LOG.debug("--------------------------------------------------------------------------------------------------");
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
			return false;
		}
		try {
			GetObjectRequest getReq = new GetObjectRequest(ObjectStoreConstants.S3_BUCKET_NAME, "Test");
			S3Object object = conn.getObject(getReq);
			System.out.println("Got back object Test=" + object.getObjectContent());
		} catch (Exception e) {
			LOG.error("getObject() got exception: ", e);
			return false;
		}
		return true;
	}

	public void close() {
		LOG.info("Closing S3 Client");
	}

	private ObjectMetadata getObjectMetadata() {
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

	private int getEndpoit(String key) {
		// select an endpoint based on the block ID
		return  Integer.valueOf(key.split("-")[4]) % connections.length;
	}

	public InputStream getObject(String key) throws AmazonClientException {
		int endpointID = getEndpoit(key);
		AmazonS3 connection = connections[endpointID];
		GetObjectRequest objReq = new GetObjectRequest(ObjectStoreConstants.S3_BUCKET_NAME, key);
		S3Object object = connection.getObject(objReq);
		return object.getObjectContent();
	}

	public InputStream getObject(String key, long startOffset, long endOffset) throws AmazonClientException {
		int endpointID = getEndpoit(key);
		AmazonS3 connection = connections[endpointID];
		GetObjectRequest objReq = new GetObjectRequest(ObjectStoreConstants.S3_BUCKET_NAME, key);
		LOG.debug("TID {} : Getting object {}, start offset = {}, end offset = {} ",
				Thread.currentThread().getId(), key, startOffset, endOffset);
		if (startOffset > 0 || endOffset != CrailConstants.BLOCK_SIZE) {
			// NOTE: start and end offset are inclusive in the S3 API.
			objReq.withRange(startOffset, endOffset - 1);
		}
		S3Object object;
		if (ObjectStoreConstants.PROFILE) {
			long startTime = System.nanoTime();
			object = connection.getObject(objReq);
			long endTime = System.nanoTime();
			LOG.debug("TID {} : S3 endpoint {} getObject() initial response took {} usec",
					Thread.currentThread().getId(), endpointID, (endTime - startTime) / 1000.);
		} else {
			object = connection.getObject(objReq);
		}
		return object.getObjectContent();
	}

	public void putObject(String key, CrailBuffer buffer) throws AmazonClientException {
		int length = buffer.remaining();
		InputStream input = new ObjectStoreUtils.ByteBufferBackedInputStream(buffer);
		ObjectMetadata md = getObjectMetadata();
		md.setContentLength(length);
		PutObjectRequest request = new PutObjectRequest(ObjectStoreConstants.S3_BUCKET_NAME, key, input, md);
		int endpointID = getEndpoit(key);
		AmazonS3 connection = connections[endpointID];
		if (ObjectStoreConstants.PROFILE) {
			long startTime = System.nanoTime();
			connection.putObject(request);
			long endTime = System.nanoTime();
			LOG.debug("TID {} : S3 putObject() of {} bytes to endpoint {} took {} usec",
					Thread.currentThread().getId(), length, endpointID, (endTime - startTime) / 1000.);
		} else {
			connection.putObject(request);
		}
	}

	public boolean deleteObject(String key) {
		// The ability to delete an object is not critical for running the Crail object tier
		int endpointID = getEndpoit(key);
		AmazonS3 connection = connections[endpointID];
		try {
			connection.deleteObject(new DeleteObjectRequest(ObjectStoreConstants.S3_BUCKET_NAME, key));
		} catch (AmazonServiceException ase) {
			LOG.error("AmazonServiceException (the request to the Object Store was rejected with an error message):");
			LOG.error("Error Message:    " + ase.getMessage());
			LOG.error("HTTP Status Code: " + ase.getStatusCode());
			LOG.error("AWS Error Code:   " + ase.getErrorCode());
			LOG.error("Error Type:       " + ase.getErrorType());
			LOG.error("Request ID:       " + ase.getRequestId());
			LOG.error("Exception: ", ase);
			return false;
		} catch (AmazonClientException ace) {
			LOG.error("AmazonClientException (the client encountered an internal error while trying to " +
					"communicate with the Object Store):");
			LOG.error("Error Message: " + ace.getMessage());
			LOG.error("Exception: ", ace);
			return false;
		} catch (Exception e) {
			LOG.error("Got exception: ", e);
			return false;
		}
		return true;
	}

	public boolean createBucket(String bucketName) {
		// The ability to create a bucket is not critical for running the Crail object tier
		try {
			if (!(connections[0].doesBucketExist(bucketName))) {
				Bucket bucket = connections[0].createBucket(bucketName);
				String bucketLocation = "";
				try {
					bucketLocation = connections[0].getBucketLocation(bucketName);
				} catch (Exception e) {
					LOG.warn("Could not get bucket {} location", bucketName, e);
				}
				LOG.debug("Created new bucket {} in location {} on {}",
						bucket.getName(), bucketLocation, bucket.getCreationDate());
			} else {
				LOG.warn("Bucket {} already exists", bucketName);
			}
		} catch (AmazonServiceException ase) {
			LOG.error("AmazonServiceException (the request to the Object Store was rejected with an error message):");
			LOG.error("Error Message:    " + ase.getMessage());
			LOG.error("HTTP Status Code: " + ase.getStatusCode());
			LOG.error("AWS Error Code:   " + ase.getErrorCode());
			LOG.error("Error Type:       " + ase.getErrorType());
			LOG.error("Request ID:       " + ase.getRequestId());
			LOG.error("Exception: ", ase);
			return false;
		} catch (AmazonClientException ace) {
			LOG.error("AmazonClientException (the client encountered an internal error while trying to " +
					"communicate with the Object Store):");
			LOG.error("Error Message: " + ace.getMessage());
			LOG.error("Exception: ", ace);
			return false;
		} catch (Exception e) {
			LOG.error("Got exception: ", e);
			return false;
		}
		return true;
	}

	public boolean deleteBucket(String bucketName) {
		// The ability to delete a bucket is not critical for running the Crail object tier
		try {
			connections[0].deleteBucket(bucketName);
			LOG.debug("Deleted bucket {}", bucketName);
		} catch (AmazonServiceException ase) {
			LOG.error("AmazonServiceException (the request to the Object Store was rejected with an error message):");
			LOG.error("Error Message:    " + ase.getMessage());
			LOG.error("HTTP Status Code: " + ase.getStatusCode());
			LOG.error("AWS Error Code:   " + ase.getErrorCode());
			LOG.error("Error Type:       " + ase.getErrorType());
			LOG.error("Request ID:       " + ase.getRequestId());
			return false;
		} catch (AmazonClientException ace) {
			LOG.error("AmazonClientException (the client encountered an internal error while trying to " +
					"communicate with the Object Store):");
			LOG.error("Error Message: " + ace.getMessage());
		} catch (Exception e) {
			LOG.error("Got exception: ", e);
			return false;
		}
		return true;
	}

	public boolean deleteObjectsWithPrefix(String prefix) {
		// The ability to delete objects is not critical for running the Crail object tier
		final String bucket = ObjectStoreConstants.S3_BUCKET_NAME;
		LOG.debug("Deleting all objects in bucket {} with prefix {}", bucket, prefix);

		// Find all objects with prefix
		List<DeleteObjectsRequest.KeyVersion> keys = new ArrayList<>();
		int count = 0;
		try {
			ListObjectsRequest listReq = new ListObjectsRequest().
					withBucketName(ObjectStoreConstants.S3_BUCKET_NAME).
					withPrefix(prefix);
			ObjectListing objectListing;
			do {
				objectListing = connections[0].listObjects(listReq);
				for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
					keys.add(new DeleteObjectsRequest.KeyVersion(objectSummary.getKey()));
					count++;
				}
				listReq.setMarker(objectListing.getNextMarker());
			} while (objectListing.isTruncated());
		} catch (AmazonServiceException ase) {
			LOG.error("AmazonServiceException (the request to the Object Store was rejected with an error message):");
			LOG.error("Error Message:    " + ase.getMessage());
			LOG.error("HTTP Status Code: " + ase.getStatusCode());
			LOG.error("AWS Error Code:   " + ase.getErrorCode());
			LOG.error("Error Type:       " + ase.getErrorType());
			LOG.error("Request ID:       " + ase.getRequestId());
			return false;
		} catch (AmazonClientException ace) {
			LOG.error("AmazonClientException (the client encountered an internal error while trying to " +
					"communicate with the Object Store):");
			LOG.error("Error Message: " + ace.getMessage());
			return false;
		} catch (Exception e) {
			LOG.error("Got exception: ", e);
			return false;
		}

		// Delete objects
		count = 0;
		try {
			DeleteObjectsRequest delReq = new DeleteObjectsRequest(bucket);
			delReq.setKeys(keys);
			DeleteObjectsResult delObjRes = connections[0].deleteObjects(delReq);
			List<DeleteObjectsResult.DeletedObject> delObjs = delObjRes.getDeletedObjects();
			for (DeleteObjectsResult.DeletedObject o : delObjs) {
				count++;
			}
		} catch (AmazonServiceException ase) {
			LOG.error("AmazonServiceException (the request to the Object Store was rejected with an error message):");
			LOG.error("Error Message:    " + ase.getMessage());
			LOG.error("HTTP Status Code: " + ase.getStatusCode());
			LOG.error("AWS Error Code:   " + ase.getErrorCode());
			LOG.error("Error Type:       " + ase.getErrorType());
			LOG.error("Request ID:       " + ase.getRequestId());
			return false;
		} catch (AmazonClientException ace) {
			LOG.error("AmazonClientException (the client encountered an internal error while trying to " +
					"communicate with the Object Store):");
			LOG.error("Error Message: " + ace.getMessage());
			return false;
		} catch (Exception e) {
			LOG.error("Got exception: ", e);
			return false;
		}
		LOG.debug("Deleted {} objects", count);
		return true;
	}
}
