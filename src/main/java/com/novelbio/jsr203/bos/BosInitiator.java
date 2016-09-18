package com.novelbio.jsr203.bos;

import com.baidubce.auth.DefaultBceCredentials;
import com.baidubce.services.bos.BosClient;
import com.baidubce.services.bos.BosClientConfiguration;

/**
 * 百度bos基础配置信息等.
 * 
 * @author novelbio
 *
 */
public class BosInitiator {
	    
	private static String ACCESS_KEY_ID = PathDetail.getAccessKey();
	private static String SECRET_ACCESS_KEY = PathDetail.getAccessKeySecret();
	private static String endpoint = PathDetail.getEndpoint();
	private static BosClient client; 
	
    static {
    	initial();
    }
	
	private static void initial() {
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
