package com.novelbio.jsr203.bos;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.qcloud.cos.exception.CosClientException;
import com.qcloud.cos.model.CopyResult;
import com.qcloud.cos.transfer.Copy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.qcloud.cos.model.CopyObjectRequest;
import com.qcloud.cos.region.Region;
import com.qcloud.cos.exception.CosServiceException;
import com.qcloud.cos.COSClient;
import com.qcloud.cos.model.ObjectMetadata;
import com.qcloud.cos.model.PartETag;
import com.qcloud.cos.transfer.TransferManager;

public class FileCopyer {

	private static Logger logger = LoggerFactory.getLogger(FileUploader.class);

	/** 大文件拷贝分块.100M */
	public static final long PART_SIZE_UNIT = 100l << 20;
	// 创建COSClient实例
	protected static COSClient client = CosInitiator.getClient();


	/**
	 * 封装COS提供的高级接口, 内部自动选择最优Copy方式(Upload Part Copy/ Put Object Copy)
	 *
	 * @param srcRegion 源文件所在Region
	 * @param srcBucket 源文件所在Bucket
	 * @param srcKey	源文件的Key
	 * @param destBucket 目标Bucket
	 * @param destKey    目标Key
	 * @param threadNum  线程池大小
	 */
	public static void fileCopy(String srcRegion, String srcBucket, String srcKey,
								String destBucket, String destKey, int threadNum) {
		logger.debug("copy file from (" + srcRegion + "," + srcBucket + "," + srcKey + ") to " +
				"(" + client.getClientConfig().getRegion() + "," + destBucket + "," + destKey + ")");

		ExecutorService threadPool = Executors.newFixedThreadPool(threadNum == 0 ? 10 : threadNum);
		TransferManager transferManager = new TransferManager(client, threadPool);

		Region srcBucketRegion = new Region(srcRegion);
		CopyObjectRequest copyObjectRequest = new CopyObjectRequest(srcBucketRegion, srcBucket,
													srcKey, destBucket, destKey);
		try {
			Copy copy = transferManager.copy(copyObjectRequest);

			// 返回一个异步结果copy, 可同步的调用waitForCopyResult等待copy结束, 成功返回CopyResult, 失败抛出异常.
			CopyResult copyResult = copy.waitForCopyResult();
		} catch (CosServiceException e) {
			e.printStackTrace();
		} catch (CosClientException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		transferManager.shutdownNow();
	}

	public static void fileCopy(String srcKey, String destKey) {
		fileCopy(client.getClientConfig().getRegion().getRegionName(), TencentCOSCopy.DEST_BUCKET, srcKey,
				TencentCOSCopy.DEST_BUCKET, destKey, 10);
	}

	/**
	 * 封装COS提供的Upload part Copy, 分块大小为PART_SIZE_UNIT
	 *
	 * @param srcRegion 源文件所在Region
	 * @param srcBucket 源文件所在Bucket
	 * @param srcKey	源文件的Key
	 * @param destBucket 目标Bucket
	 * @param destKey    目标Key
	 * @param threadNum  线程池大小
	 */
	public static void filePartCopy(String srcRegion, String srcBucket, String srcKey,
								String destBucket, String destKey, int threadNum) {
		logger.debug("copy file from (" + srcRegion + "," + srcBucket + "," + srcKey + ") to " +
				"(" + client.getClientConfig().getRegion() + "," + destBucket + "," + destKey + ")");
		System.out.println("Begin copy file.");
		// 创建一个可重用固定线程数的线程池。若同一时间线程数大于10，则多余线程会放入队列中依次执行
		ExecutorService executorService = Executors.newFixedThreadPool(threadNum == 0 ? 10 : threadNum);
		CompletionService<PartETag> completionService = new ExecutorCompletionService<PartETag>(executorService);
		
		ObjectMetadata ossObjectMetadata = null;
		try {
			String uploadId = TencentCOSCopy.claimUploadId(destBucket, destKey);


			COSClient temp_client = CosInitiator.getClient(srcRegion, "", "");
			ossObjectMetadata = temp_client.getObjectMetadata(srcBucket, srcKey);
			
			long length = ossObjectMetadata.getContentLength();

			/**
			 * 将分好的文件块加入到list集合中
			 */
			int partCount = 1;
			while (true) {
				long uploadedSize = PART_SIZE_UNIT * (partCount - 1);
				long partSize = length - uploadedSize > PART_SIZE_UNIT ? PART_SIZE_UNIT : length - uploadedSize;
				System.out.println("part Size=." + partSize);
				// 线程执行。将分好的文件块加入到list集合中
				completionService.submit(new TencentCOSCopy(srcRegion, srcBucket, srcKey,
						destBucket, destKey, partSize, partCount, uploadId));
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
			System.out.println("2222.");
			/*
			 * 列出文件所有的分块清单并打印到日志中，该方法仅仅作为输出使用
			AliyunOSSCopy.listAllParts(uploadId, target);
			 */

			/*
			 * 完成分块上传
			 */
			TencentCOSCopy.completeMultipartUpload(lsPartETags, uploadId, destKey);
			
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