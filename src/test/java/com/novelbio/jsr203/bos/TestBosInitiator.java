package com.novelbio.jsr203.bos;

import java.io.File;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.baidubce.BceServiceException;
import com.baidubce.services.bos.BosClient;
import com.baidubce.services.bos.model.BosObjectSummary;
import com.baidubce.services.bos.model.DeleteObjectRequest;
import com.baidubce.services.bos.model.ListObjectsRequest;
import com.baidubce.services.bos.model.ListObjectsResponse;
import com.baidubce.services.bos.model.PutObjectRequest;

public class TestBosInitiator {

	String bucket = PathDetail.getBucket();
	String basePath = "fansTest/";
	BosClient client;
	
	@Before
	public void Before() {
		client = BosInitiator.getClient();
		Assert.assertNotNull(client);
	}
	
	@Test
	public void testGetClient() {
		try {
			Assert.assertTrue(client.doesBucketExist(bucket));
			/*
			 * 测试,这个需购买后才能使用.
				List<BucketSummary> buckets = client.listBuckets().getBuckets();
				// 遍历Bucket
				for (BucketSummary bucket : buckets) {
					System.out.println(bucket.getName());
				}
			 */
			
			// 遍历所有Object
			ListObjectsResponse lsObjs = client.listObjects(bucket);
			for (BosObjectSummary objectSummary : lsObjs.getContents()) {
				System.out.println("ObjectKey: " + objectSummary.getKey());
			}
			
			String key1 = basePath + "myfile/test";
			String key2 = basePath + "myfile/test/";
			String key3 = basePath + "myfile/test2/";
			client.putObject(bucket, key1, "");
			client.putObject(bucket, key2, "");
			client.putObject(bucket, key3, new File("/home/novelbio/data/object相关接口快速参考卡片_打印版.pdf"));
//			File file = new File("testFile/test.txt");
//			PutObjectRequest request = new PutObjectRequest("novelbio", "testfile/test.log", file);
//			// XXX 
//			request.withStorageClass(BosClient.STORAGE_CLASS_STANDARD_IA);
			
			lsViewAllFile();
			
			client.deleteObject(bucket, key1);
			client.deleteObject(bucket, key2);
			
//		try {
//			ListObjectsRequest listObjectsRequest = new ListObjectsRequest("novelbio");
//
//			// "/" 为文件夹的分隔符
//			listObjectsRequest.setDelimiter("/");
//
//			// 列出fun目录下的所有文件和文件夹
//			listObjectsRequest.setPrefix("test/dir/");
//
//			ListObjectsResponse listing = client.listObjects(listObjectsRequest);		
//			for (BosObjectSummary bos : listing.getContents()) {
//				System.out.println(bos.getKey());
//			}
//			for (String prefix : listing.getCommonPrefixes()) {
//				System.out.println(prefix);
//			}
//			System.out.println();
//			
//			DeleteObjectRequest deleteObjectRequest = new DeleteObjectRequest();
//			deleteObjectRequest.
//			client.deleteObject(request);
//		} catch (BceServiceException e) {
//			if (!"NoSuchKey".equals(e.getErrorCode())) {
//				throw e;
//			} else {
//				System.out.println("no such key");
//			}
//		}
			
//		    
//		    System.out.println(response.getETag());
//		    
//		    response = client.putObject("novelbio", "test1/1", "er");
//		    
//		    System.out.println(response.getETag());
//		    response = client.putObject("novelbio", "test1/1", "er");
//		    
//		    System.out.println(response.getETag());
//			GetObjectRequest getObjectRequest = new GetObjectRequest("novelbio", "test/dir/fse");
//			BosObject bosObject = client.getObject(getObjectRequest);
//			System.out.println(bosObject.getKey());
			
//			client.deleteObject(bucketName, key);
			
//		    InputStream is = object.getObjectContent();
//		    BufferedReader bfR = new BufferedReader(new InputStreamReader(is));
//		    String content = "";
//		    while ((content = bfR.readLine()) != null) {
//				System.out.println(content);
//			}
//		    
//			getObjectRequest.setRange(6000, 6100);
//			object = client.getObject(getObjectRequest);
//			is = object.getObjectContent();
//			bfR = new BufferedReader(new InputStreamReader(is));
//		    content = "";
//		    while ((content = bfR.readLine()) != null) {
//				System.out.println(content);
//			}
//		    
//			getObjectRequest.setRange(7000, 7100);
//			object = client.getObject(getObjectRequest);
//			is = object.getObjectContent();
//			bfR = new BufferedReader(new InputStreamReader(is));
//		    content = "";
//		    while ((content = bfR.readLine()) != null) {
//				System.out.println(content);
//			}
//		    // 获取ObjectMeta
//		    System.out.println();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private void lsViewAllFile() {
		try {
			/*
			 * FIXME 这个有问题
			ListObjectsRequest listObjectsRequest = new ListObjectsRequest(bucket);
			// "/" 为文件夹的分隔符
			listObjectsRequest.setDelimiter("/");
			// 列出fun目录下的所有文件和文件夹
			listObjectsRequest.setPrefix(basePath);
			 */

			ListObjectsResponse listing = client.listObjects(bucket, basePath);		
			for (BosObjectSummary bos : listing.getContents()) {
				System.out.println(bos.getKey() + ":" + bos.getSize());
			}
			for (String prefix : listing.getCommonPrefixes()) {
				System.out.println(prefix);
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
