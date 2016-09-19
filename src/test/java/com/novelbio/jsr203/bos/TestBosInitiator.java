package com.novelbio.jsr203.bos;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.baidubce.BceServiceException;
import com.baidubce.services.bos.BosClient;
import com.baidubce.services.bos.model.BosObject;
import com.baidubce.services.bos.model.BosObjectSummary;
import com.baidubce.services.bos.model.DeleteObjectRequest;
import com.baidubce.services.bos.model.GetObjectRequest;
import com.baidubce.services.bos.model.ListObjectsRequest;
import com.baidubce.services.bos.model.ListObjectsResponse;
import com.baidubce.services.bos.model.PutObjectRequest;
import com.baidubce.services.bos.model.PutObjectResponse;

public class TestBosInitiator {

	String bucket = PathDetail.getBucket();
	String basePath = "/fansTest/";
	BosClient client;

	@Before
	public void Before() {
		client = BosInitiator.getClient();
		Assert.assertNotNull(client);
	}

	@Test
	public void testGetClient() {
		try {
			// 是否存在bucket.
			Assert.assertTrue(client.doesBucketExist(bucket));

			// 遍历所有Object
			ListObjectsResponse lsObjs = client.listObjects(bucket);
			for (BosObjectSummary objectSummary : lsObjs.getContents()) {
				System.out.println("ObjectKey: " + objectSummary.getKey());
			}

			// 上传文件
			String key1 = basePath + "myfile/test";
			String key2 = basePath + "myfile/test/";
			String key3 = basePath + "myfile/test2/";
			
			File bigFile = new File("/home/novelbio/git/jsr203-baidubos/src/test/resources/testFile/big.bam");
			File smallFile = new File("/home/novelbio/git/jsr203-baidubos/src/test/resources/testFile/small.txt");
			Assert.assertTrue(bigFile.exists());
			Assert.assertTrue(smallFile.exists());
			
			client.putObject(bucket, key1, "");
			client.putObject(bucket, key2, "");
			client.putObject(bucket, key3, smallFile);
			PutObjectRequest request = new PutObjectRequest("novelbio", "testfile/test.log", smallFile);
			// XXX 不明白什么意思.
			request.withStorageClass(BosClient.STORAGE_CLASS_STANDARD_IA);

//			lsViewAllFile();

			client.deleteObject(bucket, key1);
			client.deleteObject(bucket, key2);
			client.deleteObject(bucket, key3);

			try {
				String key4 = basePath + "file";
				client.putObject(bucket, key4, smallFile);
				ListObjectsRequest listObjectsRequest = new ListObjectsRequest(bucket);

				// "/" 为文件夹的分隔符
				listObjectsRequest.setDelimiter("/");
				// 列出basePath目录下的所有文件和文件夹
				listObjectsRequest.setPrefix(basePath);

				ListObjectsResponse listing = client.listObjects(listObjectsRequest);
				// 拿到的指定路径下的文件信息
//				Assert.assertTrue(listing.getContents().size() > 0);
				// 拿到的指定路径下的文件夹
//				Assert.assertTrue(listing.getCommonPrefixes().size() > 0);
				
				for (String prefix : listing.getCommonPrefixes()) {
					System.out.println(prefix);
				}
				
				// 列出根目录下的所有文件和文件夹
				listObjectsRequest.setPrefix("");

				listing = client.listObjects(listObjectsRequest);
				// 拿不到文件名称和内容
				Assert.assertTrue(listing.getContents().size() > 0);
				// 拿到的都是文件前缀
				Assert.assertTrue(listing.getCommonPrefixes().size() > 0);
				
				for (String prefix : listing.getCommonPrefixes()) {
					System.out.println(prefix);
				}

				DeleteObjectRequest deleteObjectRequest = new DeleteObjectRequest(bucket, key3);
				client.deleteObject(deleteObjectRequest);
			} catch (BceServiceException e) {
				if (!"NoSuchKey".equals(e.getErrorCode())) {
					throw e;
				} else {
					System.out.println("no such key");
				}
			}

			// System.out.println(response.getETag());
			// 上传两次,第二次的会覆盖第一次的内容.
			String key4 = basePath + "test1/1";
			PutObjectResponse response = client.putObject(bucket, key4, "er");
			System.out.println(response.getETag());
			response = client.putObject(bucket, key4, smallFile);
			System.out.println(response.getETag());

			GetObjectRequest getObjectRequest = new GetObjectRequest(bucket, "fansTest/bigFile.fq");
			getObjectRequest.setRange(0, 9);
			BosObject object = client.getObject(getObjectRequest);
			InputStream is = object.getObjectContent();
			byte[] buffer = new byte[128];
			is.read(buffer, 0, 9);
			String content = new String(buffer);
			System.out.println(content);
			
			InputStream is2 = Files.newInputStream(new File("/home/novelbio/data/small.txt").toPath());
			is2.read(buffer, 0, 9);
			String content2 = new String(buffer);
			System.out.println(content);
			Assert.assertEquals(content, content2);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Test
	public void lsViewAllFile() {
		try {
			//上传的时候指定的key,前边加一个/或两个/,最终都会被去掉.后面拿到的key,最前边的/都被去掉了.
			String key4 = "/path/readFile";
			File smallFile = new File("/home/novelbio/git/jsr203-baidubos/src/test/resources/testFile/small.txt");
			client.putObject(bucket, key4, smallFile);
			
			ListObjectsResponse listing = client.listObjects(bucket);
			System.out.println("===========所有文件列表 start===============");
			for (BosObjectSummary bos : listing.getContents()) {
				System.out.println(bos.getKey() + ":" + bos.getSize());
			}
			System.out.println("===========所有文件列表 end===============");
			
			listing = client.listObjects(bucket, basePath);
			for (BosObjectSummary bos : listing.getContents()) {
				System.out.println(bos.getKey() + ":" + bos.getSize());
			}
			if (listing.getCommonPrefixes() != null) {
				for (String prefix : listing.getCommonPrefixes()) {
					System.out.println(prefix);
				}
			}
			
			listing = client.listObjects(bucket, "fans");
			for (BosObjectSummary bos : listing.getContents()) {
				System.out.println(bos.getKey() + ":" + bos.getSize());
			}
			if (listing.getCommonPrefixes() != null) {
				for (String prefix : listing.getCommonPrefixes()) {
					System.out.println(prefix);
				}
			}
			
			listing = client.listObjects(bucket, "luwei_DownFile2016-08-29.xls");
			for (BosObjectSummary bos : listing.getContents()) {
				System.out.println(bos.getKey() + ":" + bos.getSize());
			}

//			DeleteObjectRequest deleteObjectRequest = new DeleteObjectRequest(bucket, basePath);
//			client.deleteObject(deleteObjectRequest);
		} catch (BceServiceException e) {
			if (!"NoSuchKey".equals(e.getErrorCode())) {
				throw e;
			} else {
				System.out.println("no such key");
			}
		}
	}

}
