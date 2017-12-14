package com.novelbio.fans.test;

import java.io.File;
import java.util.List;

import com.novelbio.jsr203.bos.*;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;

import com.qcloud.cos.COSClient;
import com.qcloud.cos.model.ListObjectsRequest;
import com.qcloud.cos.model.COSObjectSummary;
import com.qcloud.cos.model.ObjectListing;

public class DataCopy {

	private static COSClient cosClient = CosInitiator.getClient();
	
	/** 500M */
	public static final long FILE_SIZE = 500l << 20;
	
	public static void main(String[] args) {
		/**
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
		String target = cliParser.getOptionValue("target");*/
		
		final int maxKeys = 1000;
		String nextMarker = null;
		ObjectListing objectListing;
		long counts = 0l;

		/*
		String source = "multi_form";
		String target = "sevenyou/";
		System.out.println("bucket=" + PathDetailOs.getBucket());
		do {
		    objectListing = cosClient.listObjects(new ListObjectsRequest().withBucketName(PathDetailOs.getBucket()).withPrefix(source).withMarker(nextMarker).withMaxKeys(maxKeys));
		    List<COSObjectSummary> sums = objectListing.getObjectSummaries();
		    for (COSObjectSummary s : sums) {
		    	counts++;
		    	String targetKey = s.getKey().replace(source, target);
				System.out.println(s.getKey() + "->" + targetKey);
		    	if (s.getSize() < FILE_SIZE) {
		    		cosClient.copyObject(TencentCOSCopy.SOURCE_BUCKET, s.getKey(), TencentCOSCopy.DEST_BUCKET, targetKey);
				} else {
		    		// 这里演示的是同地域复制

				}
		    }
		    nextMarker = objectListing.getNextMarker();
		} while (objectListing.isTruncated());


		FileCopyer.filePartCopy("ap-guangzhou", "sevenyousouth-1251668577",
				"seven_10G.tmp", TencentCOSCopy.DEST_BUCKET, "sevenyou_10G.tmp.copy2", 10);
*/
		File file = new File("D:\\vim.tar");
		FileUploader.fileUpload(file, "sevenyoutest-1251668577", "sevenyou.vim");
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
//		if (!cosClient.doesObjectExist(PathDetailOs.getBucket(), path)) {
//			System.out.println("add object " + path);
//			cosClient.putObject(PathDetailOs.getBucket(), path, new ByteArrayInputStream(new byte[]{}));
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
