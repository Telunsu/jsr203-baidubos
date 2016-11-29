package com.novelbio.jsr203.bos;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
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
import com.aliyun.oss.model.GetObjectRequest;
import com.aliyun.oss.model.OSSObject;
import com.aliyun.oss.model.PartETag;

/**
 * 用于阿里云oss的随机读功能
 * 
 * @author zong0jie
 * @data 20160827
 */
public class ObjectSeekableByteStream implements SeekableByteChannel {
	private static final Logger logger = LoggerFactory.getLogger(ObjectSeekableByteStream.class);
	
	/** 
	 * 上传文件单位分块大小,默认1G
	 * TODO 这个在本地远程测试效果一般,但考虑将来在阿里patch中每秒60M+的传输速率.特设置这么大,后续可根据需要调整.
	 */
	public static final long UPLOAD_PART_SIZE = 100l << 20;
	
	OSSClient client = OssInitiator.getClient();
	// 创建一个可重用固定线程数的线程池。若同一时间线程数大于100，则多余线程会放入队列中依次执行
	ExecutorService executorService = null;
	CompletionService<PartETag> completionService = null;

	long position = 0;
	String bucketName;
	String fileName;
	OutputStream outputStream;
	/** 本地临时缓存文件 */
	File tempFile = null;
	/** 写入的流的长度 */
	long length;
	/** 块编号 */
	int partNum = 0;
	/** 上传id */
	String uploadId;
	boolean ossFileExist = false;

	public ObjectSeekableByteStream(String bucketName, String fileName) {
		this.bucketName = bucketName;
		this.fileName = fileName;
		ossFileExist = client.doesObjectExist(PathDetailOs.getBucket(), fileName);
	}

	@Override
	public boolean isOpen() {
		return true;
	}

	@Override
	public void close() throws IOException {
		if (length > 0) {
			uploadPart(true);
		}
		
		finish();
		
		if (executorService != null) {
			executorService.shutdown();
		}
	}

	@Override
	public int read(ByteBuffer dst) throws IOException {
		int num;
		try {
			int dstPos = dst.position();
			GetObjectRequest getObjectRequest = new GetObjectRequest(bucketName, fileName);
			long start = position, end = position + dst.capacity();
			getObjectRequest.setRange(start, end);
			OSSObject object = client.getObject(getObjectRequest);
			InputStream is = object.getObjectContent();
			num = is.read(dst.array(), dst.position(), dst.limit());
			position = position + dst.limit() - dstPos;
			is.close();
		} catch (Exception e) {
			num = -1;
		}
		return num;
	}

	@Override
	public int write(ByteBuffer src) throws IOException {
//		if (ossFileExist) {
//			throw new RuntimeException("file exist. please delete first! file=" + fileName);
//		}
		
		int len = src.remaining();
		src.position(src.position() + len);

		if (outputStream == null) {
			tempFile = File.createTempFile(fileName, ".tmp");
			outputStream = new FileOutputStream(tempFile);
			uploadId = AliyunOSSUpload.claimUploadId(bucketName, fileName);
			
			executorService = Executors.newFixedThreadPool(100);
			completionService = new ExecutorCompletionService<PartETag>(executorService);
		}
		
		outputStream.write(src.array(), 0, len);
		length = length + len;
		if (length >= UPLOAD_PART_SIZE) {
			uploadPart(false);
		}
		
		
		return len;
	}
	
	private void uploadPart(boolean isEnd) throws IOException {
		outputStream.flush();
		outputStream.close();
		partNum++;
		// 线程执行。将分好的文件块加入到list集合中
		completionService.submit(new AliyunOSSUpload(tempFile, 0, length, partNum, uploadId, fileName, true));
		length = 0;
		if (!isEnd) {
			tempFile = File.createTempFile(fileName, ".tmp");
			outputStream = new FileOutputStream(tempFile);
		}
	}
	
	private synchronized void finish() {
		if (partNum <= 0) {
			return;
		}
		List<PartETag> lsPartETags = new ArrayList<>();
		logger.info("fileName={},partNum={}", fileName, partNum);
		int parts = partNum;
		partNum = 0;
		for (int i = 0; i < parts; i++) {
			try {
				PartETag partETag = completionService.take().get();
				logger.info("thread{}finished", i);
				lsPartETags.add(partETag);
			} catch (InterruptedException | ExecutionException e) {
				System.err.printf("error：%s\n", e.getMessage());
				logger.error("execute executeUpload error.", e);
				// XXX 一旦并发异常，程序就卡死在这里，需要处理
			}
		}

		logger.info("upload file name= " + fileName + "\n");
		/*
		 * 列出文件所有的分块清单并打印到日志中，该方法仅仅作为输出使用
		AliyunOSSUpload.listAllParts(uploadId, fileName);
		 */
		
		/*
		 * 完成分块上传
		 */
		AliyunOSSUpload.completeMultipartUpload(fileName, lsPartETags, uploadId);
		
	}
	
	@Override
	public long position() throws IOException {
		return position;
	}

	@Override
	public SeekableByteChannel position(long newPosition) throws IOException {
//		ObjectSeekableByteStream seekableByteStream = new ObjectSeekableByteStream(bucketName, fileName);
//		this.client = client;
		this.position = newPosition;
		return this;
	}

	@Override
	public long size() throws IOException {
		GetObjectRequest getObjectRequest = new GetObjectRequest(bucketName, fileName);
		OSSObject object = client.getObject(getObjectRequest);
		return object.getObjectMetadata().getContentLength();
	}

	@Override
	public SeekableByteChannel truncate(long size) throws IOException {
		throw new RuntimeException("method doesn't support");
	}

}
