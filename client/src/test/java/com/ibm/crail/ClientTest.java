package com.ibm.crail;

import com.ibm.crail.conf.CrailConfiguration;
import com.ibm.crail.conf.CrailConstants;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.Assert;
import java.nio.ByteBuffer;
import java.util.Random;

public class ClientTest {

	CrailFS fs;
	final String basePath = "/test";
	Random rand = new Random();

	@Before
	public void init() throws Exception {
		CrailConfiguration conf = new CrailConfiguration();
		fs = CrailFS.newInstance(conf);
		fs.create(basePath, CrailNodeType.DIRECTORY, 0, 0).get();
	}

	@After
	public void fini() throws Exception {
		fs.delete(basePath, true);
	}

	@Test
	public void testCreateFile() throws Exception {
		String filename = basePath + "/fooCreate"; 
		fs.create(filename,  CrailNodeType.DATAFILE, 0, 0).get();
		fs.lookup(filename).get().asFile();
	}

	@Test
	public void testDeleteFile() throws Exception {
		String filename = basePath + "/fooDelete"; 
		fs.create(filename, CrailNodeType.DATAFILE, 0, 0).get();
		fs.delete(filename, false).get();
		Assert.assertNull(fs.lookup(filename).get());
	}

	@Test
	public void testRenameFile() throws Exception {
		String srcname = basePath + "/fooRename";
		fs.create(srcname, CrailNodeType.DATAFILE, 0, 0).get();
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
		fs.create(filename, CrailNodeType.DIRECTORY, 0, 0).get();
		fs.lookup(filename).get().asDirectory();
	}

	void fillRandom(ByteBuffer buffer) {
		int position = buffer.position();
		byte[] byteBuffer = new byte[buffer.remaining()];
		rand.nextBytes(byteBuffer);
		buffer.put(byteBuffer);
		buffer.position(position);
	}

	void skipToPosition(CrailOutputStream outputStream, int position) throws Exception {
		int toWrite = position - (int)outputStream.position();
		Assert.assertTrue(toWrite >= 0);
		if (toWrite != 0) {
			ByteBuffer outputBuffer = ByteBuffer.allocateDirect(toWrite);
			outputBuffer.limit(toWrite);
			fillRandom(outputBuffer);
			CrailResult result = outputStream.write(outputBuffer).get();
			Assert.assertEquals(toWrite, result.getLen());
			Assert.assertEquals(0, outputBuffer.remaining());
			Assert.assertEquals(position, outputStream.position());
		}
	}

	void directStream(int length, int position, int remoteOffset) throws Exception {
		ByteBuffer outputBuffer = ByteBuffer.allocateDirect(length + position);
		ByteBuffer inputBuffer = ByteBuffer.allocateDirect(outputBuffer.capacity());

		System.err.println("DirectStream write/read with from buffer position = " +
				position + ", length = " + length + ", remoteOffset = " + remoteOffset);

		String filename = basePath + "/fooOutputStream" + length;
		CrailFile file = fs.create(filename,CrailNodeType.DATAFILE,  0, 0).get().asFile();
		CrailOutputStream outputStream = file.getDirectOutputStream(0);
		CrailInputStream inputStream = file.getDirectInputStream(0);

		skipToPosition(outputStream, remoteOffset);

		outputBuffer.position(position);
		outputBuffer.limit(outputBuffer.position() + length);
		fillRandom(outputBuffer);
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
		fillRandom(inputBuffer);
		result = inputStream.read(inputBuffer).get();
		Assert.assertEquals(length, result.getLen());
		Assert.assertEquals(0, inputBuffer.remaining());
		outputBuffer.position(position);
		inputBuffer.position(position);
		try {
			Assert.assertTrue(inputBuffer.compareTo(outputBuffer) == 0);
		} catch (AssertionError e) {
			System.err.println("outputBuffer = " + outputBuffer + ", inputBuffer = " + inputBuffer);
			System.err.println("outputStream.position() = " + outputStream.position() +
					", inputStream.position() = " + inputStream.position());
			if (outputBuffer.remaining() == inputBuffer.remaining()) {
				for(int i = 0; outputBuffer.remaining() > 0; i++) {
					int a = outputBuffer.get();
					int b = inputBuffer.get();
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
				rand.nextInt((int)CrailConstants.BLOCK_SIZE - 1) + 1, 		// Unaligned block write
				rand.nextInt((int)CrailConstants.BLOCK_SIZE*8 - 1) + 1		// Unaligned multiple block write
		};
		int positions[] = {
				0,
				rand.nextInt((int)CrailConstants.BLOCK_SIZE - 1) + 1 		// Unaligned block offset
		};
		int remoteOffsets[] = {
				0,
				rand.nextInt((int)CrailConstants.BLOCK_SIZE - 1) + 1,
				rand.nextInt((int)CrailConstants.BLOCK_SIZE) + (int)CrailConstants.BLOCK_SIZE + 1
		};

		for (int length : lengths) {
			for (int position : positions) {
				for (int remoteOffset : remoteOffsets) {
					directStream(length, position, remoteOffset);
				}
			}
		}
	}
}
