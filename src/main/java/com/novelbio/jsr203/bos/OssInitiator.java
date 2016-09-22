package com.novelbio.jsr203.bos;

import com.aliyun.oss.OSSClient;

/**
 * 百度bos基础配置信息等.
 * 
 * @author novelbio
 *
 */
public class OssInitiator {
	    
	private static String ACCESS_KEY_ID = PathDetail.getAccessKey();
	private static String SECRET_ACCESS_KEY = PathDetail.getAccessKeySecret();
	private static String endpoint = PathDetail.getEndpoint();
	private static OSSClient client; 
	
    static {
    	initial();
    }
	
	private static void initial() {
    	 client = new OSSClient(endpoint, ACCESS_KEY_ID, SECRET_ACCESS_KEY);
	}
	
	public static OSSClient getClient() {
		return client;
	}
}
