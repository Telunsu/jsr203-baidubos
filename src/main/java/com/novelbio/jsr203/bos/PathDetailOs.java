package com.novelbio.jsr203.bos;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PathDetailOs {
	private static Properties properties;
	static {
		initial();
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
	
	public static String changeOsToLocal(String osPath) {
		String osHead = getOsSymbol() + ":";
		if (!osPath.startsWith(osHead) && !osPath.startsWith("/" + osHead)) {
			return osPath;
		}
		osPath = osPath.replace(osHead, "/");
		osPath = getOsMountPathWithSep() + removeSplashHead(osPath, false);
		return osPath;
	}
	
}
