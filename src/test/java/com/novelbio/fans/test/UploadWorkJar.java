package com.novelbio.fans.test;

import com.aliyun.oss.OSSClient;
import com.aliyun.oss.model.DownloadFileRequest;
import com.aliyun.oss.model.ObjectListing;
import com.aliyun.oss.model.UploadFileRequest;
import com.novelbio.jsr203.bos.OssInitiator;
import com.novelbio.jsr203.bos.PathDetailOs;

public class UploadWorkJar {

	// 创建OSSClient实例
	protected static OSSClient client = OssInitiator.getClient();

	public static void main(String[] args) {
		// downloadKey();
		uploadWorkJar();
		System.out.println("finish");
	}

	public static void deleteKey() {
		ObjectListing objectListing = client.listObjects(PathDetailOs.getBucket(), "log-count/");
		objectListing.getObjectSummaries().forEach(ossSummary -> {
			client.deleteObject(PathDetailOs.getBucket(), ossSummary.getKey());
			System.out.println("delete file=" + ossSummary.getKey());
		});
	}

	public static void downloadKey() {
		try {
			String key = "nbCloud/public/tasklogs/A__2016-10/project_5807135b0cf24f861c424f64/582ad9610cf219b5dd2845ce/stderr.job-0000000058183DD4000006FD0004910F.task582ad9610cf219b5dd2845ce-2.0";
			DownloadFileRequest downloadFileRequest = new DownloadFileRequest(PathDetailOs.getBucket(), key);
			downloadFileRequest.setDownloadFile("/home/novelbio/tmp/err.txt");
			client.downloadFile(downloadFileRequest);
		} catch (Throwable e) {
			e.printStackTrace();
		}
	}

	public static void uploadWorkJar() {
		String key = "work-jar/appYarn.tar.gz";
		client.deleteObject(PathDetailOs.getBucket(), key);
		UploadFileRequest request = new UploadFileRequest(PathDetailOs.getBucket(), key);
		request.setUploadFile("/home/novelbio/deploy/aliyun/161223/appYarn.tar.gz");
		try {
			client.uploadFile(request);
		} catch (Exception e) {
			e.printStackTrace();
		} catch (Throwable e) {
			e.printStackTrace();
		}
	}

}
