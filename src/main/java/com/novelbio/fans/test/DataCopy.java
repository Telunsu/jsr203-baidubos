package com.novelbio.fans.test;

import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;

import com.aliyun.oss.OSSClient;
import com.aliyun.oss.model.ListObjectsRequest;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.ObjectListing;
import com.novelbio.jsr203.bos.AliyunOSSCopy;
import com.novelbio.jsr203.bos.FileCopyer;
import com.novelbio.jsr203.bos.OssInitiator;
import com.novelbio.jsr203.bos.PathDetailOs;

public class DataCopy {

	private static OSSClient ossClient = OssInitiator.getClient();
	
	/** 500M */
	public static final long FILE_SIZE = 500l << 20;
	
	public static void main(String[] args) {
		
		Options opts = new Options();
		opts.addOption("source", true, "source");
		opts.addOption("target", true, "target");
		
		CommandLine cliParser = null;
		try {
			cliParser = new GnuParser().parse(opts, args);
		} catch (Exception e) {
			System.out.println("error params!");
			System.exit(1);
		}
		
		String source = cliParser.getOptionValue("source");
		String target = cliParser.getOptionValue("target");
		
		final int maxKeys = 1000;
		String nextMarker = null;
		ObjectListing objectListing;
		long counts = 0l;
		
//		String source = "nbCloud/public/nbcplatform/genome/Database/PolyphenDatabase/precomputed/";
//		String target = "nbCloud/public/taskdatabase/polyphen2/precomputed/";
		do {
			System.out.println("nextMarker=" + nextMarker);
		    objectListing = ossClient.listObjects(new ListObjectsRequest(PathDetailOs.getBucket()).withPrefix(source).withMarker(nextMarker).withMaxKeys(maxKeys));
		    List<OSSObjectSummary> sums = objectListing.getObjectSummaries();
		    for (OSSObjectSummary s : sums) {
		    	counts++;
		    	String targetKey = s.getKey().replace(source, target);
		    	if (s.getSize() < FILE_SIZE) {
		    		System.out.println(s.getKey() + "->" + targetKey);
		    		ossClient.copyObject(AliyunOSSCopy.SOURCE_BUCKET, s.getKey(), AliyunOSSCopy.TARGET_BUCKET, targetKey);
				} else {
					FileCopyer.fileCopy(s.getKey(), targetKey);
				}
		    }
		    nextMarker = objectListing.getNextMarker();
		} while (objectListing.isTruncated());
		
		System.out.println("总文件数量=" + counts);
		
	}
	
//	protected static void createDirectory(String path) {
//		if(!path.endsWith("/")) {
//			path = path + "/";
//		}
//		
//		if (setKeys.contains(path)) {
////			setKeys.add(path);
//			return;
//		}
//		
//		setKeys.add(path);
//		if (!ossClient.doesObjectExist(PathDetailOs.getBucket(), path)) {
//			System.out.println("add object " + path);
//			ossClient.putObject(PathDetailOs.getBucket(), path, new ByteArrayInputStream(new byte[]{}));
//		}
//		// add by fans.fan 170110 递归添加文件夹
//		path = path.substring(0, path.length() -1);
//		int index = path.lastIndexOf("/");
//		if (index > 0) {
//			createDirectory(path.substring(0, index));
//		} else {
//			createDirectory(path);
//		}
//		// end by fans.fan 
//	}

}
