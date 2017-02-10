package com.novelbio.jsr203.bos;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Properties;

public class PathDetailOs {
	private static Properties properties;
	
	private static String OBJECT_SAVE_SERVICE = null;
	private static String STS_DOMAIN = null;
	private static String ACCESS_KEY_ID = null;
	private static String ACCESS_KEY_SECRET = null;
	private static String ENDPOINT = null;
	private static String REGION_ID = null;
	private static String STS_API_VERSION = null;
	private static String ROLE_ARN = null;
	private static String CLIENT_BUCKET = null;
	
	static {
		initial();
		
		OBJECT_SAVE_SERVICE = properties.getProperty("CLIENT_OBJECT_SAVE_SERVICE");
		STS_DOMAIN = properties.getProperty("CLIENT_STS_DOMAIN");
		ACCESS_KEY_ID = properties.getProperty("CLIENT_ACCESS_KEY_ID");
		ACCESS_KEY_SECRET = properties.getProperty("CLIENT_ACCESS_KEY_SECRET");
		ENDPOINT = properties.getProperty("CLIENT_ENDPOINT");
		REGION_ID = properties.getProperty("CLIENT_REGION_ID");
		STS_API_VERSION = properties.getProperty("CLIENT_STS_API_VERSION");
		ROLE_ARN = properties.getProperty("CLIENT_ROLE_ARN");
		CLIENT_BUCKET = properties.getProperty("CLIENT_BUCKET");
	}
	private static void initial() {
		String configPath = "configoss.properties";
		InputStream in = PathDetailOs.class.getClassLoader().getResourceAsStream(configPath);
		properties = new Properties();
		try {
			properties.load(in);
		} catch (IOException e1) {
			e1.printStackTrace();
			throw new RuntimeException(e1);
		} finally{
			try {
				if(in != null){
					in.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	
	}
	

	public static String getRegionId() {
		return properties.getProperty("regionId");
	}

	/**
	 * 获取阿里云镜像id
	 * 
	 * @return
	 */
	public static String getImageId() {
		return properties.getProperty("imageId");
	}
	
	/** 
	 * 阿里云实例名称
	 * @return
	 */
	public static String getInstanceType() {
		return properties.getProperty("instanceType");
	}

	public static int getVmCounts() {
		try {
			return Integer.valueOf(properties.getProperty("vmCounts"));
		} catch (Exception e) {
			e.printStackTrace();
		}
		return 5;
	}
	
	public static String getDockerRegistry() {
		return properties.getProperty("dockerRegistry");
	}
	
	/**
	 * 获取对象存储服务供应类型[ACS_OSS,BCE_BOS]，阿里云还是百度云
	 * 
	 * @return OBJECT_SAVE_SERVICE
	 */
	public static String getClientObjectSaveService() {
		return OBJECT_SAVE_SERVICE;
	}
	
	/**
	 * 获取STS_DOMAIN
	 * 
	 * @return STS_DOMAIN
	 */
	public static String getClientStsDomain() {
		return STS_DOMAIN;
	}
	
	/**
	 * 获取ACCESS_KEY_ID
	 * 
	 * @return ACCESS_KEY_ID
	 */
	public static String getClientAccessKeyID() {
		return ACCESS_KEY_ID;
	}
	
	/**
	 * 获取ACCESS_KEY_SECRET
	 * 
	 * @return ACCESS_KEY_SECRET
	 */
	public static String getClientAccessKeySecret() {
		return ACCESS_KEY_SECRET;
	}
	
	/** 当前 STS API 版本 */
	public static String getClientStsApiVersion() {
		return STS_API_VERSION;
	}

	/** 目前只有"cn-hangzhou"这个region可用, 不要使用填写其他region的值 */
	public static String getClientRegionId() {
		return REGION_ID;
	}
	
	public static String getClientRoleArn() {
		return ROLE_ARN;
	}

	public static String getClientEndpoint() {
		return ENDPOINT;
	}
	
	/** OSS上的命名空间 */
	public static String getClientBucket() {
		return CLIENT_BUCKET;
	}

	/** BOS上的命名空间 */
	public static String getBucket() {
		return properties.getProperty("bucket");
	}
	/** 用户的Access Key ID */
	public static String getAccessKey() {
		return properties.getProperty("ACCESS_KEY_ID");
	}
	/**  用户的Secret Access Key */
	public static String getAccessKeySecret() {
		return properties.getProperty("SECRET_ACCESS_KEY");
	}
	/** 用户自己指定的域名 */
	public static String getEndpoint() {
		return properties.getProperty("endpoint");
	}
	
	public static String getOsSymbol() {
		return properties.getProperty("ossymbol");
	}
	
	public static String getOsMountPathWithSep() {
		return properties.getProperty("osmount") + "/";
	}

	public static String getTopicName() {
		return properties.getProperty("topicName");
	}
	
	public static String getMnsEndpoint() {
		return properties.getProperty("mnsEndpoint");
	}
	
	
	
	
	/**
	 * 将文件开头的"//"这种多个的去除
	 * 
	 * @param fileName
	 * @param keepOne
	 *            是否保留一个“/”
	 * @return
	 */
	public static String removeSplashHead(String fileName, boolean keepOne) {
		String head = "//";
		if (!keepOne) {
			head = "/";
		}
		String fileNameThis = fileName;
		while (fileNameThis.startsWith(head)) {
			fileNameThis = fileNameThis.substring(1);
		}
		return fileNameThis;
	}
	
	public static String changeOsToLocal(String ossPath) {
		String osHead = OssFileSystemProvider.SCHEME + "://";
		if (!ossPath.startsWith(osHead)) {
			return ossPath;
		}
		try {
			return  getOsMountPathWithSep() + ((OssPath)new OssFileSystemProvider().getPath(new URI(ossPath))).getInternalPath();
		} catch (Exception e) {
			throw new RuntimeException("changeOsToLocal error.ossPath=" + ossPath, e);
		}
	}


}
