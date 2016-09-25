package com.novelbio.jsr203.bos;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
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
import com.aliyun.oss.model.UploadPartRequest;
import com.aliyun.oss.model.UploadPartResult;

public class AliyunOSSUpload implements Callable<PartETag> {
	private static Logger logger = LoggerFactory.getLogger(AliyunOSSUpload.class);

	protected static OSSClient client = OssInitiator.getClient();
	private static String bucketName = PathDetailOs.getBucket();

	private File localFile;
	private long startPos;
	private long partSize;
	private int partNumber;
	private String uploadId;
	private String key;
	private boolean isTempFile;

	/**
	 * 创建构造方法
	 * 
	 * @param localFile
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
	public AliyunOSSUpload(File localFile, long startPos, long partSize, int partNumber, String uploadId, String key, boolean isTempFile) {
		this.localFile = localFile;
		this.startPos = startPos;
		this.partSize = partSize;
		this.partNumber = partNumber;
		this.uploadId = uploadId;
		this.key = key;
		this.isTempFile = isTempFile;
//		AliyunOSSUpload.bucketName = bucketName;
	}

	/**
	 * 分块上传核心方法(将文件分成按照每个5M分成N个块，并加入到一个list集合中)
	 */
	@Override
	public PartETag call() throws Exception {
		InputStream instream = null;
		try {
			// 获取文件流
			instream = new FileInputStream(localFile);
			// 跳到每个分块的开头
			instream.skip(this.startPos);

			// 创建UploadPartRequest，上传分块
			UploadPartRequest uploadPartRequest = new UploadPartRequest();
			uploadPartRequest.setBucketName(bucketName);
			uploadPartRequest.setKey(key);
			uploadPartRequest.setUploadId(uploadId);
			uploadPartRequest.setInputStream(instream);
			uploadPartRequest.setPartSize(partSize);
			uploadPartRequest.setPartNumber(partNumber);

			UploadPartResult uploadPartResult = client.uploadPart(uploadPartRequest);
			logger.info("Part#" + partNumber + " done\n");
			if (isTempFile && localFile.length() == partSize) {
				localFile.delete();
			}
			
			return uploadPartResult.getPartETag();
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		} finally {
			if (instream != null) {
				try {
					// 关闭文件流
					instream.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	/**
	 * 初始化分块上传事件并生成uploadID，用来作为区分分块上传事件的唯一标识
	 * 
	 * @return
	 */
	protected static String claimUploadId(String bucketName, String key) {
		InitiateMultipartUploadRequest request = new InitiateMultipartUploadRequest(bucketName, key);
		InitiateMultipartUploadResult result = OssInitiator.getClient().initiateMultipartUpload(request);
		logger.info(result.getUploadId());
		return result.getUploadId();
	}

	/**
	 * 将文件分块进行升序排序并执行文件上传。
	 * 
	 * @param uploadId
	 */
	protected static void completeMultipartUpload(String key, List<PartETag> partETags, String uploadId) {
		// 将文件分块按照升序排序
		Collections.sort(partETags, new Comparator<PartETag>() {
			@Override
			public int compare(PartETag p1, PartETag p2) {
				return p1.getPartNumber() - p2.getPartNumber();
			}
		});

		CompleteMultipartUploadRequest completeMultipartUploadRequest = new CompleteMultipartUploadRequest(bucketName,
				key, uploadId, partETags);
		// 完成分块上传
		client.completeMultipartUpload(completeMultipartUploadRequest);
	}

	/**
	 * 列出文件所有分块的清单
	 * 
	 * @param uploadId
	 */
	protected static void listAllParts(String uploadId, String key) {
		ListPartsRequest listPartsRequest = new ListPartsRequest(bucketName, key, uploadId);
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
