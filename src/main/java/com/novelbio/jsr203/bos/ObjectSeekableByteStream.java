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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.print.attribute.standard.Finishings;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.oss.OSSClient;
import com.aliyun.oss.model.GetObjectRequest;
import com.aliyun.oss.model.OSSObject;
import com.aliyun.oss.model.UploadPartRequest;

/**
 * 用于百度bos的随机读功能
 * 
 * @author zong0jie
 * @data 20160827
 */
public class ObjectSeekableByteStream implements SeekableByteChannel {
	private static final Logger logger = LoggerFactory.getLogger(ObjectSeekableByteStream.class);
	
	OSSClient client = OssInitiator.getClient();

	long position = 0;
	String bucketName;
	String fileName;
	OutputStream outputStream;
	/** 本地临时缓存文件 */
	File tempFile = null;
	List<String> lsTempFile = new ArrayList<>();
	/** 写入的流的长度 */
	long length;
	/** 分块大小,默认1G */
	long partSize = 1 << 30;
	/** 块编号 */
	int partNum = 0;
	/** 上传id */
	String uploadId;
	// 创建一个可重用固定线程数的线程池。若同一时间线程数大于10，则多余线程会放入队列中依次执行
	ExecutorService executorService = Executors.newFixedThreadPool(50);

	public ObjectSeekableByteStream(String bucketName, String fileName) {
		this.bucketName = bucketName;
		this.fileName = fileName;
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
	}

	@Override
	public int read(ByteBuffer dst) throws IOException {
		int num;
		try {
			int dstPos = dst.position();
			GetObjectRequest getObjectRequest = new GetObjectRequest(bucketName, fileName);
			getObjectRequest.setRange(position, position + dst.capacity());
			OSSObject object = client.getObject(getObjectRequest);
			InputStream is = object.getObjectContent();
			num = is.read(dst.array(), dst.position(), dst.limit());
			dst.position(dst.limit());
			position = position + dst.limit() - dstPos;
			is.close();
			
		} catch (Exception e) {
			num = -1;
		}
		return num;
	}

	@Override
	public int write(ByteBuffer src) throws IOException {
		int len = src.remaining();
		src.position(src.position() + len);

		if (outputStream == null) {
			tempFile = File.createTempFile(fileName, ".tmp");
			outputStream = new FileOutputStream(tempFile);
			uploadId = AliyunOSSUpload.claimUploadId(bucketName, fileName);
		}
		
		outputStream.write(src.array(), 0, len);
		length = length + len;
		if (length >= partSize) {
			uploadPart(false);
		}
		
		
		return len;
	}
	
	private void uploadPart(boolean isEnd) throws IOException {
		outputStream.flush();
		outputStream.close();
		partNum++;
		// 线程执行。将分好的文件块加入到list集合中
		executorService.execute(new AliyunOSSUpload(tempFile, 0, length, partNum, uploadId, fileName, bucketName));
		length = 0;
		lsTempFile.add(tempFile.getAbsolutePath());
		if (!isEnd) {
			tempFile = File.createTempFile(fileName, ".tmp");
			outputStream = new FileOutputStream(tempFile);
		}
	}
	
	private void finish() {
		/**
		 * 等待所有分片完毕
		 */
		// 关闭线程池（线程池不马上关闭），执行以前提交的任务，但不接受新任务。
		executorService.shutdown();
		// 如果关闭后所有任务都已完成，则返回 true。
		while (!executorService.isTerminated()) {
			try {
				// 用于等待子线程结束，再继续执行下面的代码
				executorService.awaitTermination(5, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		/**
		 * partETags(上传块的ETag与块编号（PartNumber）的组合) 如果校验与之前计算的分块大小不同，则抛出异常
		 */
		System.out.println(AliyunOSSUpload.partETags.size()  +" -----   " + partNum );
		if (AliyunOSSUpload.partETags.size() != partNum) {
			throw new IllegalStateException("OSS分块大小与文件所计算的分块大小不一致");
		} else {
			logger.info("将要上传的文件名  " + fileName + "\n");
		}

		/*
		 * 列出文件所有的分块清单并打印到日志中，该方法仅仅作为输出使用
		 */
		AliyunOSSUpload.listAllParts(uploadId);
		
		lsTempFile.forEach(filePath -> {
			new File(filePath).delete();
		});

		/*
		 * 完成分块上传
		 */
		AliyunOSSUpload.completeMultipartUpload(uploadId);
		
		// 返回上传文件的URL地址
		String path = PathDetail.getEndpoint() + "/" + bucketName + "/" + fileName;
	}
	
	@Override
	public long position() throws IOException {
		return position;
	}

	@Override
	public SeekableByteChannel position(long newPosition) throws IOException {
		ObjectSeekableByteStream seekableByteStream = new ObjectSeekableByteStream(bucketName, fileName);
		seekableByteStream.client = client;
		seekableByteStream.position = newPosition;
		return seekableByteStream;
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
