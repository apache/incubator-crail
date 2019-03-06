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

import org.apache.crail.conf.CrailConfiguration;
import org.apache.crail.conf.CrailConstants;
import org.apache.crail.memory.OffHeapBuffer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.Assert;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ThreadLocalRandom;

public class ClientTest {

	CrailStore fs;
	final String basePath = "/test";
	ThreadLocalRandom random = ThreadLocalRandom.current();

	@Before
	public void init() throws Exception {
		CrailConfiguration conf = CrailConfiguration.createConfigurationFromFile();
		fs = CrailStore.newInstance(conf);
		fs.create(basePath, CrailNodeType.DIRECTORY, CrailStorageClass.DEFAULT, CrailLocationClass.DEFAULT, true).get();
	}

	@After
	public void fini() throws Exception {
		fs.delete(basePath, true);
	}

	@Test
	public void testCreateFile() throws Exception {
		String filename = basePath + "/fooCreate";
		fs.create(filename,  CrailNodeType.DATAFILE, CrailStorageClass.DEFAULT, CrailLocationClass.DEFAULT, true).get();
		fs.lookup(filename).get().asFile();
	}

	@Test
	public void testDeleteFile() throws Exception {
		String filename = basePath + "/fooDelete";
		fs.create(filename, CrailNodeType.DATAFILE, CrailStorageClass.DEFAULT, CrailLocationClass.DEFAULT, true).get();
		fs.delete(filename, false).get();
		Assert.assertNull(fs.lookup(filename).get());
	}

	@Test
	public void testRenameFile() throws Exception {
		String srcname = basePath + "/fooRename";
		fs.create(srcname, CrailNodeType.DATAFILE, CrailStorageClass.DEFAULT, CrailLocationClass.DEFAULT, true).get();
		String dstname = basePath + "/barRename";
		fs.rename(srcname, dstname).get();
		Assert.assertNull(fs.lookup(srcname).get());
		fs.lookup(dstname).get().asFile();
	}

	@Test
	public void testlookupDirectory() throws Exception {
		fs.lookup(basePath).get().asDirectory();
	}

	@Test
	public void testCreateDirectory() throws Exception {
		String filename = basePath + "/fooDir";
		fs.create(filename, CrailNodeType.DIRECTORY, CrailStorageClass.DEFAULT, CrailLocationClass.DEFAULT, true).get();
		fs.lookup(filename).get().asDirectory();
	}

	void fillRandom(ByteBuffer buffer) {
		int position = buffer.position();
		byte[] byteBuffer = new byte[buffer.remaining()];
		random.nextBytes(byteBuffer);
		buffer.put(byteBuffer);
		buffer.position(position);
	}

	void skipToPosition(CrailOutputStream outputStream, int position) throws Exception {
		int toWrite = position - (int)outputStream.position();
		Assert.assertTrue(toWrite >= 0);
		if (toWrite != 0) {
			CrailBuffer outputBuffer = OffHeapBuffer.wrap(ByteBuffer.allocateDirect(toWrite));
			outputBuffer.limit(toWrite);
			fillRandom(outputBuffer.getByteBuffer());
			CrailResult result = outputStream.write(outputBuffer).get();
			Assert.assertEquals(toWrite, result.getLen());
			Assert.assertEquals(0, outputBuffer.remaining());
			Assert.assertEquals(position, outputStream.position());
		}
	}

	void directStream(int length, int position, int remoteOffset) throws Exception {
		CrailBuffer outputBuffer = OffHeapBuffer.wrap(ByteBuffer.allocateDirect(length + position));
		CrailBuffer inputBuffer = OffHeapBuffer.wrap(ByteBuffer.allocateDirect(outputBuffer.capacity()));

		System.err.println("DirectStream write/read with from buffer position = " +
				position + ", length = " + length + ", remoteOffset = " + remoteOffset);

		String filename = basePath + "/fooOutputStream" + length;
		CrailFile file = fs.create(filename,CrailNodeType.DATAFILE,  CrailStorageClass.DEFAULT, CrailLocationClass.DEFAULT, true).get().asFile();
		CrailOutputStream outputStream = file.getDirectOutputStream(0);
		CrailInputStream inputStream = file.getDirectInputStream(0);

		skipToPosition(outputStream, remoteOffset);

		outputBuffer.position(position);
		outputBuffer.limit(outputBuffer.position() + length);
		fillRandom(outputBuffer.getByteBuffer());
		CrailResult result = outputStream.write(outputBuffer).get();
		Assert.assertEquals(length, result.getLen());
		Assert.assertEquals(0, outputBuffer.remaining());
		Assert.assertEquals(remoteOffset + length, outputStream.position());


		if (inputStream.position() != remoteOffset) {
			inputStream.seek(remoteOffset);
			Assert.assertEquals(remoteOffset, inputStream.position());
		}

		inputBuffer.position(position);
		inputBuffer.limit(inputBuffer.position() + length);
		fillRandom(inputBuffer.getByteBuffer());
		result = inputStream.read(inputBuffer).get();
		Assert.assertEquals(length, result.getLen());
		Assert.assertEquals(0, inputBuffer.remaining());
		outputBuffer.position(position);
		inputBuffer.position(position);
		try {
			Assert.assertTrue(inputBuffer.getByteBuffer().compareTo(outputBuffer.getByteBuffer()) == 0);
		} catch (AssertionError e) {
			System.err.println("outputBuffer = " + outputBuffer + ", inputBuffer = " + inputBuffer);
			System.err.println("outputStream.position() = " + outputStream.position() +
					", inputStream.position() = " + inputStream.position());
			if (outputBuffer.remaining() == inputBuffer.remaining()) {
				for(int i = 0; outputBuffer.remaining() > 0; i++) {
					int a = outputBuffer.getByteBuffer().get();
					int b = inputBuffer.getByteBuffer().get();
					if (a != b) {
						System.err.println("outputBuffer[" + i + "] = " + Integer.toHexString(a) + " != " +
								"inputBuffer[" + i + "] = " + Integer.toHexString(b));
						break;
					}
				}
			}
			throw e;
		}

		fs.delete(filename, false);
	}

	@Test
	public void testDirectStream() throws Exception {
		int lengths[] = {
				(int)CrailConstants.BLOCK_SIZE, 							// full block write
				(int)CrailConstants.BLOCK_SIZE*8, 							// multiple block write
				random.nextInt((int)CrailConstants.BLOCK_SIZE - 1) + 1, 		// Unaligned block write
				random.nextInt((int)CrailConstants.BLOCK_SIZE*8 - 1) + 1		// Unaligned multiple block write
		};
		int positions[] = {
				0,
				random.nextInt((int)CrailConstants.BLOCK_SIZE - 1) + 1 		// Unaligned block offset
		};
		int remoteOffsets[] = {
				0,
				random.nextInt((int)CrailConstants.BLOCK_SIZE - 1) + 1,
				random.nextInt((int)CrailConstants.BLOCK_SIZE) + (int)CrailConstants.BLOCK_SIZE + 1
		};

		for (int length : lengths) {
			for (int position : positions) {
				for (int remoteOffset : remoteOffsets) {
					directStream(length, position, remoteOffset);
				}
			}
		}
	}

	@Test
	public void unalignedBufferStreamSimple() throws Exception {
		System.err.println("BufferedStream unaligned write after purge");

		String filename = basePath + "/fooOutputStream";
		CrailFile file = fs.create(filename,CrailNodeType.DATAFILE,  CrailStorageClass.DEFAULT, CrailLocationClass.DEFAULT, true).get().asFile();

		int totalBytesWritten = 0;
		int iterations = 4;
		ByteBuffer buffers[] = new ByteBuffer[iterations];
		buffers[0] = StandardCharsets.UTF_8.encode("Hello");
		buffers[1] = StandardCharsets.UTF_8.encode("World");
		buffers[2] = StandardCharsets.UTF_8.encode("Problem");
		buffers[3] = StandardCharsets.UTF_8.encode("Solved");
		for (int i = 0; i < iterations; i++) {
			totalBytesWritten += buffers[i].capacity();
			CrailBufferedOutputStream outputStream = file.getBufferedOutputStream(0);
			outputStream.write(buffers[i]);
			outputStream.purge().get();
			outputStream.close();
		}

		CrailBufferedInputStream inputStream = file.getBufferedInputStream(0);
		ByteBuffer buffer = ByteBuffer.allocateDirect(totalBytesWritten);
		int read = inputStream.read(buffer);
		Assert.assertEquals(totalBytesWritten, read);
		buffer.clear();
		Assert.assertEquals("HelloWorldProblemSolved", StandardCharsets.UTF_8.decode(buffer).toString());
	}

	@Test
	public void unalignedBufferStream() throws Exception {
		System.err.println("BufferedStream unaligned write after purge");

		String filename = basePath + "/fooOutputStream2";
		CrailFile file = fs.create(filename,CrailNodeType.DATAFILE,  CrailStorageClass.DEFAULT, CrailLocationClass.DEFAULT, true).get().asFile();

		int totalBytesWritten = 0;
		int iterations = 10;
		ByteBuffer buffers[] = new ByteBuffer[iterations];
		for (int i = 0; i < iterations; i++) {
			int unalignedSize;
			do {
				unalignedSize = random.nextInt(1,CrailConstants.BUFFER_SIZE);
			} while (unalignedSize % CrailConstants.SLICE_SIZE == 0);
			buffers[i] = ByteBuffer.allocateDirect(unalignedSize);
			fillRandom(buffers[i]);
			totalBytesWritten += buffers[i].capacity();
			buffers[i].clear();
			CrailBufferedOutputStream outputStream = file.getBufferedOutputStream(0);
			outputStream.write(buffers[i]);
			outputStream.purge().get();
			outputStream.close();
		}

		CrailBufferedInputStream inputStream = file.getBufferedInputStream(0);
		ByteBuffer buffer = ByteBuffer.allocateDirect(totalBytesWritten);
		int read = inputStream.read(buffer);
		Assert.assertEquals(totalBytesWritten, read);
		buffer.clear();
		for (ByteBuffer b : buffers) {
			b.clear();
			buffer.limit(buffer.position() + b.capacity());
			System.err.println("orgBuffer.capacity() = " + b.capacity());
			for(int i = 0; buffer.remaining() > 0; i++) {
				int x = buffer.get();
				int y = b.get();
				if (x != y) {
					System.err.println("buffer[" + (buffer.position() - 1) + "] = " + Integer.toHexString(x) + " != " +
							"orgBuffer[" + i + "] = " + Integer.toHexString(y));
					Assert.assertTrue(false);
				}
			}
		}
	}

	@Test
	public void alignBufferStreamPosition() throws Exception {
		System.err.println("BufferedStream test align position");

		String filename = basePath + "/fooOutputStream3";
		CrailFile file = fs.create(filename,CrailNodeType.DATAFILE,  CrailStorageClass.DEFAULT, CrailLocationClass.DEFAULT, true).get().asFile();
		fs.getStatistics().reset();
		CrailBufferedOutputStream outputStream = file.getBufferedOutputStream(0);
		ByteBuffer beginBuffer = ByteBuffer.allocateDirect(random.nextInt(1, CrailConstants.SLICE_SIZE));
		fillRandom(beginBuffer);
		outputStream.write(beginBuffer);
		Assert.assertEquals(0, outputStream.outputStream().position());
		outputStream.purge().get();
		Assert.assertEquals(beginBuffer.capacity(), outputStream.outputStream().position());
		outputStream.close();

		ByteBuffer middleBuffer = ByteBuffer.allocateDirect(CrailConstants.SLICE_SIZE);
		fillRandom(middleBuffer);
		outputStream = file.getBufferedOutputStream(0);
		outputStream.write(middleBuffer);
		Assert.assertEquals(CrailConstants.SLICE_SIZE, outputStream.outputStream().position());

		ByteBuffer endBuffer = ByteBuffer.allocateDirect(CrailConstants.SLICE_SIZE);
		fillRandom(endBuffer);
		outputStream.write(endBuffer);
		Assert.assertEquals(CrailConstants.SLICE_SIZE * 2, outputStream.outputStream().position());
		outputStream.purge().get();
		int totalSize = beginBuffer.capacity() + middleBuffer.capacity() + endBuffer.capacity();
		Assert.assertEquals(totalSize, outputStream.outputStream().position());
		outputStream.close();

		CrailBufferedInputStream inputStream = file.getBufferedInputStream(0);
		ByteBuffer buffer = ByteBuffer.allocateDirect(totalSize);
		inputStream.read(buffer);
		buffer.position(0);
		buffer.limit(beginBuffer.capacity());
		beginBuffer.clear();
		Assert.assertEquals(beginBuffer, buffer);
		buffer.position(buffer.limit());
		buffer.limit(buffer.position() + middleBuffer.capacity());
		middleBuffer.clear();
		Assert.assertEquals(middleBuffer, buffer);
		buffer.position(buffer.limit());
		buffer.limit(buffer.position() + endBuffer.capacity());
		endBuffer.clear();
		Assert.assertEquals(endBuffer, buffer);
	}
}
