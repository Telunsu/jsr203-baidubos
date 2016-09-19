package com.novelbio.jsr203.bos;

import static org.junit.Assert.assertNotNull;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;

import org.junit.Assert;
import org.junit.Test;

import com.baidubce.services.bos.BosClient;
import com.baidubce.services.bos.BosObjectInputStream;
import com.baidubce.services.bos.model.BosObject;
import com.baidubce.services.bos.model.GetObjectRequest;

public class TestObjectSeekableByteStream {

	ObjectSeekableByteStream objectSeekableByteStream;

	@Test
	public void testObjectSeekableByteStream() {
		objectSeekableByteStream = new ObjectSeekableByteStream(PathDetail.getBucket(), "fansTest/test1/1");
		assertNotNull(objectSeekableByteStream);
	}

	@Test
	public void testRead() {
		try {
			// 该文件大小71字节
			objectSeekableByteStream = new ObjectSeekableByteStream(PathDetail.getBucket(), "fansTest/test1/1");
			ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
			int i = objectSeekableByteStream.read(byteBuffer);
			System.out.println(i);
			Assert.assertTrue(i > 0);
			System.out.println("testRead1=" + new String(byteBuffer.array()));
		} catch (Exception e) {
			e.printStackTrace();
		}

		try {
			BosClient client = BosInitiator.getClient();
			GetObjectRequest getObjectRequest = new GetObjectRequest(PathDetail.getBucket(), "q/DownFile2016-08-29.xls");
			// 获取文件部分内容.如果指定值超出文件大小会抛异常.
			getObjectRequest.setRange(0, 12);
			BosObject object = client.getObject(getObjectRequest);
			BosObjectInputStream is = object.getObjectContent();
			BufferedReader bfR = new BufferedReader(new InputStreamReader(is));
			String content = bfR.readLine();
			System.out.println("testRead2=" + content);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	 @Test
	 public void testPositionLong() {
		 objectSeekableByteStream = new ObjectSeekableByteStream(PathDetail.getBucket(), "fansTest/test1/1");
		 try {
			 objectSeekableByteStream = (ObjectSeekableByteStream) objectSeekableByteStream.position(10);
			 assertNotNull(objectSeekableByteStream);
			 Assert.assertTrue(objectSeekableByteStream.position == 10);
		} catch (IOException e) {
			e.printStackTrace();
		}
	 }
	
	 @Test
	public void testSize() {
		try {
			objectSeekableByteStream = new ObjectSeekableByteStream(PathDetail.getBucket(), "fansTest/test1/1");
			Assert.assertTrue(objectSeekableByteStream.size() > 0);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
