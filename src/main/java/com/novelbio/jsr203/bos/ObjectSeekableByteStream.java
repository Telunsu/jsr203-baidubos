package com.novelbio.jsr203.bos;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;

import com.baidubce.BceServiceException;
import com.baidubce.services.bos.BosClient;
import com.baidubce.services.bos.model.BosObject;
import com.baidubce.services.bos.model.GetObjectRequest;

/**
 * 用于百度bos的随机读功能
 * 
 * @author zong0jie
 * @data 20160827
 */
public class ObjectSeekableByteStream implements SeekableByteChannel {
	BosClient client = BosInitiator.getClient();

	long position = 0;
	// TODO 这个感觉可以从文件名中直接获取
	String bucketName;
	String fileName;

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
	}

	@Override
	public int read(ByteBuffer dst) throws IOException {
		int num;
		try {
			int dstPos = dst.position();
			GetObjectRequest getObjectRequest = new GetObjectRequest(bucketName, fileName);
			getObjectRequest.setRange(position, position + dst.capacity());
			BosObject object = client.getObject(getObjectRequest);
			InputStream is = object.getObjectContent();
			num = is.read(dst.array(), dst.position(), dst.limit());
			dst.position(dst.limit());
			position = position + dst.limit() - dstPos;
			object.close();
			is.close();
			
		} catch (BceServiceException e) {
			num = -1;
		}
		return num;
	}

	@Override
	public int write(ByteBuffer src) throws IOException {
		throw new RuntimeException("method doesn't support");
	}

	@Override
	public long position() throws IOException {
		return position;
	}

	@Override
	public SeekableByteChannel position(long newPosition) throws IOException {
		ObjectSeekableByteStream seekableByteStream = new ObjectSeekableByteStream(bucketName, fileName);
		seekableByteStream.client = client;
		seekableByteStream.position = position;
		return seekableByteStream;
	}

	@Override
	public long size() throws IOException {
		GetObjectRequest getObjectRequest = new GetObjectRequest(bucketName, fileName);
		BosObject object = client.getObject(getObjectRequest);
		return object.getObjectMetadata().getInstanceLength();
	}

	@Override
	public SeekableByteChannel truncate(long size) throws IOException {
		throw new RuntimeException("method doesn't support");
	}

}
