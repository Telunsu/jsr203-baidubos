package com.novelbio.jsr203.bos;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.oss.OSSClient;
import com.aliyun.oss.model.DownloadFileRequest;
import com.aliyun.oss.model.OSSObject;
import com.aliyun.oss.model.ObjectListing;
import com.aliyun.oss.model.PartETag;
import com.aliyun.oss.model.UploadFileRequest;

public class FileUploader {

	private static Logger logger = LoggerFactory.getLogger(FileCopyer.class);

	// 创建OSSClient实例
	protected static OSSClient client = OssInitiator.getClient();

	public static void fileUpload(File file, String key) {
		logger.debug("upload file " + file.getName());
		
		// 创建一个可重用固定线程数的线程池。若同一时间线程数大于10，则多余线程会放入队列中依次执行
		ExecutorService executorService = Executors.newFixedThreadPool(3);
		CompletionService<PartETag> completionService = new ExecutorCompletionService<PartETag>(executorService);
		
		try {
			String uploadId = AliyunOSSUpload.claimUploadId(PathDetailOs.getBucket(), key);
			// 设置分块大小
			final long partSize = ObjectSeekableByteStream.UPLOAD_PART_SIZE;
			// 计算分块数目
			long fileLength = file.length();
			int partCount = (int) (fileLength / partSize);
			if (fileLength % partSize != 0) {
				partCount++;
			}

			// 分块 号码的范围是1~10000。如果超出这个范围，OSS将返回InvalidArgument的错误码。
			if (partCount > 10000) {
				throw new RuntimeException("文件过大(分块大小不能超过10000)");
			} else {
				logger.info("一共分了 " + partCount + " 块");
			}

			/**
			 * 将分好的文件块加入到list集合中
			 */
			for (int i = 0; i < partCount; i++) {
				long startPos = i * partSize;
				long curPartSize = (i + 1 == partCount) ? (fileLength - startPos) : partSize;
				
				// 线程执行。将分好的文件块加入到list集合中
				completionService.submit(new AliyunOSSUpload(file, startPos, curPartSize, i + 1, uploadId, key, false));
			}

//			/**
//			 * 等待所有分片完毕
//			 */
//			// 关闭线程池（线程池不马上关闭），执行以前提交的任务，但不接受新任务。
//			executorService.shutdown();
//			// 如果关闭后所有任务都已完成，则返回 true。
//			while (!executorService.isTerminated()) {
//				try {
//					// 用于等待子线程结束，再继续执行下面的代码
//					executorService.awaitTermination(5, TimeUnit.SECONDS);
//				} catch (InterruptedException e) {
//					e.printStackTrace();
//				}
//			}

//			/**
//			 * partETags(上传块的ETag与块编号（PartNumber）的组合) 如果校验与之前计算的分块大小不同，则抛出异常
//			 */
//			System.out.println(AliyunOSSUpload.partETags.size()  +" -----   "+partCount);
//			if (AliyunOSSUpload.partETags.size() != partCount) {
//				throw new IllegalStateException("OSS分块大小与文件所计算的分块大小不一致");
//			} else {
//				logger.info("将要上传的文件名  " + key + "\n");
//			}

			List<PartETag> lsPartETags = new ArrayList<>();
			for (int i = 0; i < partCount; i++) {
				try {
					PartETag partETag = completionService.take().get();
					logger.info("线程{}运行结束", i);
					lsPartETags.add(partETag);
				} catch (InterruptedException | ExecutionException e) {
					System.err.printf("并发处理异常：%s\n", e.getMessage());
					logger.error("execute executeUpload error.", e);
					// XXX 一旦并发异常，程序就卡死在这里，需要处理
				}
			}
			
			/*
			 * 列出文件所有的分块清单并打印到日志中，该方法仅仅作为输出使用
			AliyunOSSUpload.listAllParts(uploadId, key);
			 */

			/*
			 * 完成分块上传
			 */
			AliyunOSSUpload.completeMultipartUpload(file.getName(), lsPartETags, uploadId);
			
			// 返回上传文件的URL地址
//			return endpoint + "/" + bucketName + "/" + client.getObject(bucketName, key).getKey();

		} catch (Exception e) {
			logger.error("上传失败！", e);
			throw e;
//			return "上传失败！";
		} finally {
			executorService.shutdown();
//			if (client != null) {
//				client.shutdown();
//			}
		}
	}
}