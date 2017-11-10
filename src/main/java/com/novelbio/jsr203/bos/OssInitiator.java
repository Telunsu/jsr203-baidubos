package com.novelbio.jsr203.bos;

import com.aliyun.oss.OSSClient;

/**
 * 百度bos基础配置信息等.
 * 
 * @author novelbio
 *
 */
public class OssInitiator {
	    
	private static String ACCESS_KEY_ID = PathDetailOs.getAccessKey();
	private static String SECRET_ACCESS_KEY = PathDetailOs.getAccessKeySecret();
	private static String endpoint = PathDetailOs.getEndpoint();
	private static OSSClient client; 
	
    static {
    	initial();
    }
	
	private static void initial() {
		String ossHost = System.getenv("BATCH_COMPUTE_OSS_HOST");
		if (ossHost != null && !"".equals(ossHost)) {
			// ossHost不为空,说明是运行在批量计算的VM中.这时取环境变量中的参数.
			endpoint = ossHost;
		}
    	client = new OSSClient(endpoint, ACCESS_KEY_ID, SECRET_ACCESS_KEY);
	}
	
	public static OSSClient getClient() {
		return client;
	}
	
}
