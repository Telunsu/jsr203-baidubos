package com.novelbio.jsr203.bos;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class COSConfig {
	private static Properties properties;
	static {
		initial();
	}
	private static void initial() {
		String configPath = "configoss.properties";
		InputStream in = COSConfig.class.getClassLoader().getResourceAsStream(configPath);
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
	public static String getRegion() {
		return properties.getProperty("region");
	}
}
