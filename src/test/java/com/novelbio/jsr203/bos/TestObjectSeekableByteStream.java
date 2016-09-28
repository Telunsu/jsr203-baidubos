package com.novelbio.jsr203.bos;

import static org.junit.Assert.assertNotNull;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;

import org.junit.Assert;
import org.junit.Test;

import com.aliyun.oss.OSSClient;
import com.aliyun.oss.model.GetObjectRequest;
import com.aliyun.oss.model.OSSObject;

public class TestObjectSeekableByteStream {

	ObjectSeekableByteStream objectSeekableByteStream;
	
	String Key = "arabidopsis_rna_2.fq";

	@Test
	public void testObjectSeekableByteStream() {
		objectSeekableByteStream = new ObjectSeekableByteStream(OssConfig.getBucket(), Key);
		assertNotNull(objectSeekableByteStream);
	}

	@Test
	public void testRead() {
		String content = null;
		try {
			objectSeekableByteStream = new ObjectSeekableByteStream(OssConfig.getBucket(), Key);
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
			GetObjectRequest getObjectRequest = new GetObjectRequest(OssConfig.getBucket(), Key);
			// 获取文件部分内容.如果指定值超出文件大小会抛异常.
			getObjectRequest.setRange(0, 8);
			OSSObject object = client.getObject(getObjectRequest);
			InputStream is = object.getObjectContent();
			BufferedReader bfR = new BufferedReader(new InputStreamReader(is));
			String content2 = bfR.readLine();
			System.out.println("testRead2=" + content2);
			Assert.assertEquals(content, content2);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	 @Test
	 public void testPositionLong() {
		 this.objectSeekableByteStream = new ObjectSeekableByteStream(OssConfig.getBucket(), Key);
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
			objectSeekableByteStream = new ObjectSeekableByteStream(OssConfig.getBucket(), Key);
			Assert.assertTrue(objectSeekableByteStream.size() > 0);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
