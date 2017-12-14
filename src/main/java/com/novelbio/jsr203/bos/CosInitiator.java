package com.novelbio.jsr203.bos;

import com.qcloud.cos.COSClient;
import com.qcloud.cos.ClientConfig;
import com.qcloud.cos.auth.BasicCOSCredentials;
import com.qcloud.cos.auth.COSCredentials;
import com.qcloud.cos.region.Region;

/**
 * 百度bos基础配置信息等.
 * 
 * @author novelbio
 *
 */
public class CosInitiator {
	// PathDetailOS和COSConfig作用的区别是啥没看懂
	private static String ACCESS_KEY_ID = COSConfig.getAccessKey();
	private static String SECRET_ACCESS_KEY = COSConfig.getAccessKeySecret();
	// 不清楚这个endpoint的具体含义
	private static String endpoint = PathDetailOs.getEndpoint();
	private static String REGION = COSConfig.getRegion();
	private static COSClient client;
	
    static {
    	initial();
    }
	
	private static void initial() {
		String cosHost = System.getenv("BATCH_COMPUTE_OSS_HOST");
		if (cosHost != null && !"".equals(cosHost)) {
			// cosHost,说明是运行在批量计算的VM中.这时取环境变量中的参数.
			endpoint = cosHost;
		}

		// 1 初始化用户身份信息(accessKey, secretKey), 到控制台获取 https://console.cloud.tencent.com/capi
		COSCredentials cred = new BasicCOSCredentials(ACCESS_KEY_ID, SECRET_ACCESS_KEY);

		// 2 设置bucket的区域, COS地域的简称请参照 https://www.qcloud.com/document/product/436/6224
		ClientConfig clientConfig = new ClientConfig(new Region(REGION));

    	client = new COSClient(cred, clientConfig);
	}
	
	public static COSClient getClient() {
		return client;
	}

	public static COSClient getClient(String region, String access_key, String secret_key) {
    	if (access_key.isEmpty()) {
    		access_key = ACCESS_KEY_ID;
		}

		if (secret_key.isEmpty()) {
    		secret_key = SECRET_ACCESS_KEY;
		}

		// 1 初始化用户身份信息(accessKey, secretKey), 到控制台获取 https://console.cloud.tencent.com/capi
		COSCredentials cred = new BasicCOSCredentials(access_key, secret_key);

		// 2 设置bucket的区域, COS地域的简称请参照 https://www.qcloud.com/document/product/436/6224
		ClientConfig clientConfig = new ClientConfig(new Region(region));

		return new COSClient(cred, clientConfig);
	}
}
