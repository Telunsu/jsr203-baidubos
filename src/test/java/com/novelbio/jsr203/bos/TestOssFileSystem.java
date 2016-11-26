package com.novelbio.jsr203.bos;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.aliyun.oss.OSSClient;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.model.OSSObject;
import com.aliyun.oss.model.ObjectListing;


public class TestOssFileSystem {
	
	OSSClient client = OssInitiator.getClient();
	
	@Test
	public void testCreateDirectory() throws URISyntaxException {
		OssFileSystem ossFileSystem = new OssFileSystem(new OssFileSystemProvider(), new URI(""));
		ossFileSystem.createDirectory("test/dir/", null);
		OSSObject obj = client.getObject(PathDetailOs.getBucket(), "test/dir/");
		Assert.assertEquals("test/dir/", obj.getKey());
		client.deleteObject(PathDetailOs.getBucket(), "test/dir/");
	}
	
	@Test
	public void testDeleteFile() throws URISyntaxException {
		
		String file = "small.txt";
		client.putObject(PathDetailOs.getBucket(), file, new File("/home/novelbio/git/jsr203-aliyun/src/test/resources/testFile/small.txt"));
		OSSException exception = null;
		OssFileSystemProvider ossFileSystemProvider = new OssFileSystemProvider();
		OssFileSystem ossFileSystem = new OssFileSystem(ossFileSystemProvider, new URI(""));
		ossFileSystem.deleteFile((OssPath) ossFileSystemProvider.getPath(new URI("oss://" + PathDetailOs.getBucket() + "/" + file)));
		Assert.assertFalse(client.doesObjectExist(PathDetailOs.getBucket(), file));
		try {
			OSSObject obj = client.getObject(PathDetailOs.getBucket(), "test/dir/.exist");
		} catch (Exception e) {
			exception = (OSSException) e;
		}
		
		Assert.assertNotNull(exception);
		Assert.assertEquals("NoSuchKey", exception.getErrorCode());
	}
	
	
}
