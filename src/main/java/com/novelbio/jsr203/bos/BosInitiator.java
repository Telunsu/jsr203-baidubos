package com.novelbio.jsr203.bos;

import java.io.IOException;

import com.baidubce.BceServiceException;
import com.baidubce.auth.DefaultBceCredentials;
import com.baidubce.services.bos.BosClient;
import com.baidubce.services.bos.BosClientConfiguration;
import com.baidubce.services.bos.model.BosObject;
import com.baidubce.services.bos.model.BosObjectSummary;
import com.baidubce.services.bos.model.DeleteObjectRequest;
import com.baidubce.services.bos.model.GetObjectRequest;
import com.baidubce.services.bos.model.ListObjectsRequest;
import com.baidubce.services.bos.model.ListObjectsResponse;
import com.baidubce.services.moladb.model.DeleteItemRequest;

public class BosInitiator {
//	  String ACCESS_KEY_ID = <your-access-key-id>;                   // 用户的Access Key ID
//	    String SECRET_ACCESS_KEY = <your-secret-access-key>;
	    
	public static String ACCESS_KEY_ID = PathDetail.getAccessKey();
	public static String SECRET_ACCESS_KEY = PathDetail.getAccessKeySecret();
	public static String endpoint = PathDetail.getEndpoint();
    static BosClient client; 
	
    public static void main(String[] args) throws IOException {
		BosClient client = BosInitiator.getClient();
//	    client.createBucket("novelbio");                               //指定Bucket名称
//
//		 List<BucketSummary> buckets = client.listBuckets().getBuckets();
//
//		    // 遍历Bucket
//		    for (BucketSummary bucket : buckets) {
//		        System.out.println(bucket.getName());
//		    }
//		    ListObjectsResponse listing = client.listObjects("novelbrain");
//
//		    // 遍历所有Object
//		    for (BosObjectSummary objectSummary : listing.getContents()) {
//		        System.out.println("ObjectKey: " + objectSummary.getKey());
//		    }
		    
//		    File file = new File("/home/novelbio/software/yarn-novelbio-resourcemanager-novelbio170.log");
//		    PutObjectRequest request = new PutObjectRequest("novelbio", "testfile/test.log", file);
//		    request.withStorageClass(BosClient.STORAGE_CLASS_STANDARD_IA);
//		    PutObjectResponse response = client.deleteObject(bucketName, key);
		
			client.putObject("novelbio", "myfile/test/", "");
			client.putObject("novelbio", "myfile/test", "");

//			client.deleteObject("novelbio", "myfile/test/");
		
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
		    
    }
    
    static {
    	initial();
    }
	
	public static void initial() {
	    BosClientConfiguration config = new BosClientConfiguration();
    	client = new BosClient(config);
	    config.setCredentials(new DefaultBceCredentials(ACCESS_KEY_ID, SECRET_ACCESS_KEY));
	    config.setMaxConnections(10);
	    config.setEndpoint(endpoint);
	    config.setConnectionTimeoutInMillis(5000);
	    config.setSocketTimeoutInMillis(2000);
	}
	
	public static BosClient getClient() {
		return client;
	}
}
