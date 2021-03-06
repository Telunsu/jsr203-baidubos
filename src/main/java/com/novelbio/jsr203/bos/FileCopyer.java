package com.novelbio.jsr203.bos;

import java.io.IOException;
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
import com.aliyun.oss.model.ObjectMetadata;
import com.aliyun.oss.model.PartETag;

public class FileCopyer {

	private static Logger logger = LoggerFactory.getLogger(FileUploader.class);

	/** 大文件拷贝分块.100M */
	public static final long PART_SIZE_UNIT = 100l << 20;
	// 创建OSSClient实例
	protected static OSSClient client = OssInitiator.getClient();
	
	public static void fileCopy(String source, String target) {
		logger.debug("copy file " + source + " to " + target);
		
		// 创建一个可重用固定线程数的线程池。若同一时间线程数大于10，则多余线程会放入队列中依次执行
		ExecutorService executorService = Executors.newFixedThreadPool(10);
		CompletionService<PartETag> completionService = new ExecutorCompletionService<PartETag>(executorService);
		
		ObjectMetadata ossObjectMetadata = null;
		try {
			String uploadId = AliyunOSSCopy.claimUploadId(AliyunOSSCopy.TARGET_BUCKET, target);
			
			ossObjectMetadata = client.getObjectMetadata(AliyunOSSCopy.SOURCE_BUCKET, source);
			
			long length = ossObjectMetadata.getContentLength();

			/**
			 * 将分好的文件块加入到list集合中
			 */
			int partCount = 1;
			while (true) {
				long uploadedSize = PART_SIZE_UNIT * (partCount - 1);
				long partSize = length - uploadedSize > PART_SIZE_UNIT ? PART_SIZE_UNIT : length - uploadedSize;
				// 线程执行。将分好的文件块加入到list集合中
				completionService.submit(new AliyunOSSCopy(source, target, partSize, partCount, uploadId));
				if (partSize < PART_SIZE_UNIT) {
					break;
				}
				partCount++;
			}

			List<PartETag> lsPartETags = new ArrayList<>();
			for (int i = 0; i < partCount; i++) {
				try {
					PartETag partETag = completionService.take().get();
					logger.debug("thread {} finished.", i);
					lsPartETags.add(partETag);
				} catch (InterruptedException | ExecutionException e) {
					logger.error("execute executeUpload error.", e);
					// XXX 一旦并发异常，程序就卡死在这里，需要处理
				}
			}
			
			/*
			 * 列出文件所有的分块清单并打印到日志中，该方法仅仅作为输出使用
			AliyunOSSCopy.listAllParts(uploadId, target);
			 */

			/*
			 * 完成分块上传
			 */
			AliyunOSSCopy.completeMultipartUpload(lsPartETags, uploadId, target);
			
			// 返回上传文件的URL地址
//			return endpoint + "/" + bucketName + "/" + client.getObject(bucketName, key).getKey();

		} catch (Exception e) {
			logger.error("upload failed！", e);
			throw e;
		} finally {
			executorService.shutdown();
		}
	}
}