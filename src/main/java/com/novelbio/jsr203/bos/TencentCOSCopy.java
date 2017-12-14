package com.novelbio.jsr203.bos;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.qcloud.cos.COSClient;
import com.qcloud.cos.model.CompleteMultipartUploadRequest;
import com.qcloud.cos.model.InitiateMultipartUploadRequest;
import com.qcloud.cos.model.InitiateMultipartUploadResult;
import com.qcloud.cos.model.ListPartsRequest;
import com.qcloud.cos.model.PartETag;
import com.qcloud.cos.model.PartListing;
import com.qcloud.cos.model.PartSummary;
import com.qcloud.cos.model.CopyPartRequest;
import com.qcloud.cos.model.CopyPartResult;
import com.qcloud.cos.region.Region;

public class TencentCOSCopy implements Callable<PartETag> {
	private static Logger logger = LoggerFactory.getLogger(TencentCOSCopy.class);

	public static String SOURCE_BUCKET_REGION = PathDetailOs.getRegionId();
	public static String SOURCE_BUCKET = PathDetailOs.getBucket();
	public static String DEST_BUCKET = PathDetailOs.getBucket();
	protected static COSClient client = CosInitiator.getClient();

	private String sourceRegion;
	private String sourceBucket;
	private String sourceKey;
	private String destBucket;
	private String destKey;
	private long partSize;
	private int partNumber;
	private String uploadId;

	public String getSourceRegion() {
		return sourceRegion;
	}

	public void setSourceRegion(String sourceRegion) {
		this.sourceRegion = sourceRegion;
	}

	public String getSourceBucket() {
		return sourceBucket;
	}

	public void setSourceBucket(String sourceBucket) {
		this.sourceBucket = sourceBucket;
	}

	public String getSourceKey() {
		return sourceKey;
	}

	public void setSourceKey(String sourceKey) {
		this.sourceKey = sourceKey;
	}

	public String getDestBucket() {
		return destBucket;
	}

	public void setDestBucket(String destBucket) {
		this.destBucket = destBucket;
	}

	public String getDestKey() {
		return destKey;
	}

	public void setDestKey(String destKey) {
		this.destKey = destKey;
	}

	public long getPartSize() {
		return partSize;
	}

	public void setPartSize(long partSize) {
		this.partSize = partSize;
	}

	public int getPartNumber() {
		return partNumber;
	}

	public void setPartNumber(int partNumber) {
		this.partNumber = partNumber;
	}

	public String getUploadId() {
		return uploadId;
	}

	public void setUploadId(String uploadId) {
		this.uploadId = uploadId;
	}

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
	 *            上传到COS后的文件名
	 */
	public TencentCOSCopy(String sourceRegion, String sourceBucket, String sourceKey,
						  String destBucket, String destKey, long partSize, int partNumber, String uploadId) {
		this.sourceRegion = sourceRegion;
		this.sourceBucket = sourceBucket;
		this.sourceKey = sourceKey;
		this.destBucket = destBucket;
		this.destKey = destKey;
		this.partSize = partSize;
		this.partNumber = partNumber;
		this.uploadId = uploadId;
	}

	/**
	 * 分块上传核心方法(将文件分成按照每个5M分成N个块，并加入到一个list集合中)
	 */
	@Override
	public PartETag call() throws Exception {
		try {
			// 创建UploadPartRequest，上传分块
			CopyPartRequest uploadPartCopyRequest = new CopyPartRequest();
			// 要拷贝的源文件所在的region
			uploadPartCopyRequest.setSourceBucketRegion(new Region(sourceRegion));
			// 要拷贝的源文件的bucket名称
			uploadPartCopyRequest.setSourceBucketName(sourceBucket);
			// 要拷贝的源文件的路径
			uploadPartCopyRequest.setSourceKey(sourceKey);
			// 指定要拷贝的源文件的数据范围(类似content-range)
			uploadPartCopyRequest.setFirstByte(FileCopyer.PART_SIZE_UNIT * (partNumber - 1));
			uploadPartCopyRequest.setLastByte(FileCopyer.PART_SIZE_UNIT * (partNumber - 1) + partSize);
            // 指定UploadID
			uploadPartCopyRequest.setUploadId(uploadId);
			// 指定PartNumber
			uploadPartCopyRequest.setPartNumber(partNumber);
			// 目的bucket名称
			uploadPartCopyRequest.setDestinationBucketName(DEST_BUCKET);
			// 目的路径名称
			uploadPartCopyRequest.setDestinationKey(destKey);

			CopyPartResult uploadPartResult = client.copyPart(uploadPartCopyRequest);
			logger.debug("Part# {} done", this.partNumber);
			
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

		CompleteMultipartUploadRequest completeMultipartUploadRequest = new CompleteMultipartUploadRequest(DEST_BUCKET,
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
		ListPartsRequest listPartsRequest = new ListPartsRequest(DEST_BUCKET, key, uploadId);
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
