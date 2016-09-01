package com.novelbio.jsr203.bos;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;

public class PathDetail {
	private static Properties properties;

	static {
		initial();
	}
	private static void initial() {
		String configPath = "configbos.properties";
		InputStream in = PathDetail.class.getClassLoader().getResourceAsStream(configPath);
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

	public static String getBucket() {
		return properties.getProperty("bucket");
	}
	public static String getAccessKey() {
		return properties.getProperty("ACCESS_KEY_ID");
	}
	public static String getAccessKeySecret() {
		return properties.getProperty("SECRET_ACCESS_KEY");
	}
	public static String getEndpoint() {
		return properties.getProperty("endpoint");
	}
}
