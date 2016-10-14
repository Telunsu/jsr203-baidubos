package com.novelbio.jsr203.bos;

import static org.junit.Assert.assertNotNull;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;

import org.assertj.core.api.SoftAssertions;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.aliyun.oss.OSSClient;
import com.aliyun.oss.model.GetObjectRequest;
import com.aliyun.oss.model.OSSObject;

public class TestObjectSeekableByteStream {

	ObjectSeekableByteStream objectSeekableByteStream;
	
	static String key = "arabidopsis_rna_2.fq";
	static OSSClient client = OssInitiator.getClient();
	
	@BeforeClass
	public static void before() {
		client.putObject(PathDetailOs.getBucket(), key, new File("/home/novelbio/git/jsr203-aliyun/src/test/resources/testFile/big.bam"));
	}

	@Test
	public void testObjectSeekableByteStream() {
		objectSeekableByteStream = new ObjectSeekableByteStream(PathDetailOs.getBucket(), key);
		assertNotNull(objectSeekableByteStream);
	}

	@Test
	public void testRead() {
		String content = null;
		try {
			objectSeekableByteStream = new ObjectSeekableByteStream(PathDetailOs.getBucket(), key);
			ByteBuffer byteBuffer = ByteBuffer.allocate(9);
			int i = objectSeekableByteStream.read(byteBuffer);
			Assert.assertTrue(i > 0);
			content = new String(byteBuffer.array());
			System.out.println("testRead1=" + content);
		} catch (Exception e) {
			e.printStackTrace();
		}

		try {
			OSSClient client = OssInitiator.getClient();
			GetObjectRequest getObjectRequest = new GetObjectRequest(PathDetailOs.getBucket(), key);
			// 获取文件部分内容.如果指定值超出文件大小会抛异常.
			getObjectRequest.setRange(0, 8);
			OSSObject object = client.getObject(getObjectRequest);
			InputStream is = object.getObjectContent();
			BufferedReader bfR = new BufferedReader(new InputStreamReader(is));
			String lineStr = null;
			StringBuffer content2 = new StringBuffer();
			while ((lineStr = bfR.readLine()) != null) {
				if (content2.length() > 0) {
					content2.append("\n");
				}
				content2.append(lineStr);
			}
			System.out.println("testRead2=" + content2);
			SoftAssertions soft = new SoftAssertions();
			soft.assertThat(content).isEqualTo(content2.toString());
			soft.assertAll();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	 @Test
	 public void testPositionLong() {
		 this.objectSeekableByteStream = new ObjectSeekableByteStream(PathDetailOs.getBucket(), key);
		 try {
			 ObjectSeekableByteStream objectSeekableByteStream = (ObjectSeekableByteStream) this.objectSeekableByteStream.position(10);
			 assertNotNull(objectSeekableByteStream);
			 Assert.assertTrue(objectSeekableByteStream.position() == 10);
		} catch (IOException e) {
			e.printStackTrace();
		}
	 }
	
	 @Test
	public void testSize() {
		try {
			objectSeekableByteStream = new ObjectSeekableByteStream(PathDetailOs.getBucket(), key);
			Assert.assertTrue(objectSeekableByteStream.size() > 0);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	 
	 @AfterClass
	 public static void after() {
		 client.deleteObject(PathDetailOs.getBucket(), key);
	 }

}
