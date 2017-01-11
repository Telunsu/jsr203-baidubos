package com.novelbio.jsr203.bos;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.oss.OSSClient;
import com.aliyun.oss.model.CompleteMultipartUploadRequest;
import com.aliyun.oss.model.InitiateMultipartUploadRequest;
import com.aliyun.oss.model.InitiateMultipartUploadResult;
import com.aliyun.oss.model.ListPartsRequest;
import com.aliyun.oss.model.PartETag;
import com.aliyun.oss.model.PartListing;
import com.aliyun.oss.model.PartSummary;
import com.aliyun.oss.model.UploadPartCopyRequest;
import com.aliyun.oss.model.UploadPartCopyResult;

public class AliyunOSSCopy implements Callable<PartETag> {
	private static Logger logger = LoggerFactory.getLogger(AliyunOSSCopy.class);
	
	public static String SOURCE_BUCKET = PathDetailOs.getBucket();
	public static String TARGET_BUCKET = PathDetailOs.getBucket();
	protected static OSSClient client = OssInitiator.getClient();

	private String sourceKey;
	private String targetKey;
	private long partSize;
	private int partNumber;
	private String uploadId;

	/**
	 * 创建构造方法
	 * 
	 * @param sourceKey
	 *            要上传的文件
	 * @param startPos
	 *            每个文件块的开始
	 * @param partSize
	 * @param partNumber
	 * @param uploadId
	 *            作为块的标识
	 * @param key
	 *            上传到OSS后的文件名
	 */
	public AliyunOSSCopy(String sourceKey, String targetKey, long partSize, int partNumber, String uploadId) {
		this.sourceKey = sourceKey;
		this.targetKey = targetKey;
		this.partSize = partSize;
		this.partNumber = partNumber;
		this.uploadId = uploadId;
//		AliyunOSSUpload.bucketName = bucketName;
	}

	/**
	 * 分块上传核心方法(将文件分成按照每个5M分成N个块，并加入到一个list集合中)
	 */
	@Override
	public PartETag call() throws Exception {
		try {

			// 创建UploadPartRequest，上传分块
			UploadPartCopyRequest uploadPartCopyRequest = new UploadPartCopyRequest(SOURCE_BUCKET, sourceKey, TARGET_BUCKET, targetKey);
			uploadPartCopyRequest.setUploadId(uploadId);
			uploadPartCopyRequest.setBeginIndex(FileCopyer.PART_SIZE_UNIT * (partNumber - 1));
			uploadPartCopyRequest.setPartSize(partSize);
			uploadPartCopyRequest.setPartNumber(partNumber);

			UploadPartCopyResult uploadPartResult = client.uploadPartCopy(uploadPartCopyRequest);
			logger.info("Part#" + this.partNumber + " done\n");
			
			return uploadPartResult.getPartETag();
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}
	}
	
	/**
	 * 初始化分块上传事件并生成uploadID，用来作为区分分块上传事件的唯一标识
	 * 
	 * @return
	 */
	protected static String claimUploadId(String bucketName, String key) {
		InitiateMultipartUploadRequest request = new InitiateMultipartUploadRequest(bucketName, key);
		InitiateMultipartUploadResult result = client.initiateMultipartUpload(request);
		logger.info(result.getUploadId());
		return result.getUploadId();
	}

	/**
	 * 将文件分块进行升序排序并执行文件上传。
	 * 
	 * @param uploadId
	 */
	protected static void completeMultipartUpload(List<PartETag> partETags, String uploadId, String sourcekey) {
		// 将文件分块按照升序排序
		Collections.sort(partETags, new Comparator<PartETag>() {
			@Override
			public int compare(PartETag p1, PartETag p2) {
				return p1.getPartNumber() - p2.getPartNumber();
			}
		});

		CompleteMultipartUploadRequest completeMultipartUploadRequest = new CompleteMultipartUploadRequest(TARGET_BUCKET,
				sourcekey, uploadId, partETags);
		// 完成分块上传
		client.completeMultipartUpload(completeMultipartUploadRequest);
	}

	/**
	 * 列出文件所有分块的清单
	 * 
	 * @param uploadId
	 */
	protected static void listAllParts(String uploadId, String key) {
		ListPartsRequest listPartsRequest = new ListPartsRequest(TARGET_BUCKET, key, uploadId);
		// 获取上传的所有分块信息
		PartListing partListing = client.listParts(listPartsRequest);

		// 获取分块的大小
		int partCount = partListing.getParts().size();
		// 遍历所有分块
		for (int i = 0; i < partCount; i++) {
			PartSummary partSummary = partListing.getParts().get(i);
			logger.info("分块编号 " + partSummary.getPartNumber() + ", ETag=" + partSummary.getETag());
		}
	}

}
