package com.novelbio.jsr203.bos;

import java.io.ByteArrayInputStream;
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
import com.aliyun.oss.model.ObjectMetadata;
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
	 * 上传文件单位分块大小,默认1G TODO
	 * 这个在本地远程测试效果一般,但考虑将来在阿里patch中每秒60M+的传输速率.特设置这么大,后续可根据需要调整.
	 */
	public static final long UPLOAD_PART_SIZE = 1l << 30;

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

		int index = this.fileName.lastIndexOf("/");
		if (index > 0) {
			createDirectory(this.fileName.substring(0, index));
		}
	}

	@Override
	public int read(ByteBuffer dst) throws IOException {
		int num = 0;
		InputStream is = null;
		long start = position, end = position + dst.remaining();
		OSSObject object = null;
		try {
			// int dstPos = dst.position();
			GetObjectRequest getObjectRequest = new GetObjectRequest(bucketName, fileName);
			getObjectRequest.setRange(start, end - 1);
			object = client.getObject(getObjectRequest);
			/*
			 *  FIXME 这里要注意,阿里云oss如果指定的Range无效(比如开始位置、结束位置为负数，大于文件大小)，则会下载整个文件；
			 *  所以这里读取时，判断一下，如果返回的大小和range指定的不一致,那就是返回整个文件了,需重新设置,重新获取.
			 */
			long fileLength = object.getObjectMetadata().getContentLength();
			if ((end - start) != fileLength) {
				if (start > fileLength) {
					start = fileLength -1;
				}
				end = fileLength;
				getObjectRequest.setRange(start, end - 1);
				object.getObjectContent().close();
				object = client.getObject(getObjectRequest);
			}
			is = object.getObjectContent();
			int len = dst.remaining() > 1024 ? 1024 : dst.remaining();
			byte[] buf = new byte[len];
			for (int n = 0; n != -1;) {
				n = is.read(buf, 0, buf.length);
				if (n != -1) {
					num = num + n;
					dst.put(buf, 0, n);
				} 
			}
			position = position + num;
		} catch (Exception e) {
			logger.error("position=" + position+ ", dst=" + dst.capacity() + ", num=" + num + ", start=" + start + ", end=" + end, e);
			num = -1;
		} finally {
			object.getObjectContent().close();
			is.close();
		}
		return num;
	}

	@Override
	public int write(ByteBuffer src) throws IOException {
//		if (ossFileExist) {
//		throw new RuntimeException("file exist. please delete first! file=" + fileName);
//	}

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
		ObjectMetadata objectMetadata = client.getObjectMetadata(getObjectRequest);
		return objectMetadata.getContentLength();
	}

	@Override
	public SeekableByteChannel truncate(long size) throws IOException {
		throw new RuntimeException("method doesn't support");
	}

	/**
	 * 创建文件夹
	 * 
	 * @param path
	 */
	protected void createDirectory(String path) {
		if (!path.endsWith("/")) {
			path = path + "/";
		}

		if (client.doesObjectExist(PathDetailOs.getBucket(), path)) {
			return;
		}

		client.putObject(PathDetailOs.getBucket(), path, new ByteArrayInputStream(new byte[] {}));
		// add by fans.fan 170110 递归添加文件夹
		path = path.substring(0, path.length() - 1);
		int index = path.lastIndexOf("/");
		if (index > 0) {
			createDirectory(path.substring(0, index));
		}
		// end by fans.fan
	}
}
