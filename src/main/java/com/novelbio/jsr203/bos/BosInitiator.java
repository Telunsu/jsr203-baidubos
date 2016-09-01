package com.novelbio.jsr203.bos;

import java.io.IOException;
import java.util.List;

import com.baidubce.BceServiceException;
import com.baidubce.auth.DefaultBceCredentials;
import com.baidubce.services.bos.BosClient;
import com.baidubce.services.bos.BosClientConfiguration;
import com.baidubce.services.bos.model.BosObjectSummary;
import com.baidubce.services.bos.model.GetObjectRequest;
import com.baidubce.services.bos.model.ListObjectsResponse;
import com.baidubce.services.bos.model.PutObjectResponse;

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
		try {
		    client.deleteObject(PathDetail.getBucket(), "testfile/test.log");
		} catch (BceServiceException e) {
			if (!"NoSuchKey".equals(e.getErrorCode())) {
				throw e;
			} else {
				System.out.println("no such key");
			}
		}
	    client.putObject("novelbio", "testfile/test.log2", "2");

//		    
//		    System.out.println(response.getETag());
//		    
//		    response = client.putObject("novelbio", "test1/1", "er");
//		    
//		    System.out.println(response.getETag());
//		    response = client.putObject("novelbio", "test1/1", "er");
//		    
//		    System.out.println(response.getETag());
//			GetObjectRequest getObjectRequest = new GetObjectRequest("novelbio", "testfile/test.log22");
//			getObjectRequest.setRange(5000, 5100);
//			
//			ListObjectsResponse lsRes = client.listObjects("novelbio", "testfile/test.log");
//		    List<BosObjectSummary> lsBos = lsRes.getContents();
//		    for (BosObjectSummary bosObjectSummary : lsBos) {
//				System.out.println(bosObjectSummary.getKey());
//			}
//		    System.out.println();
		
		
		

		
		
		
		
		
		
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
