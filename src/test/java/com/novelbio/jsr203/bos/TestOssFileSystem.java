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
	@Rule //暂时不清楚这个annotation有什么用，待查证
	public ExpectedException thrown = ExpectedException.none();
	
//	@Test
	public void testCreateDirectory() throws URISyntaxException {
		OssFileSystem ossFileSystem = new OssFileSystem(new OssFileSystemProvider(), new URI(""));
		ossFileSystem.createDirectory("oss:/test/dir", null);
		OSSObject obj = client.getObject("novelbio", "test/dir/.exist");
		Assert.assertEquals("test/dir/.exist", obj.getKey());
	}
	
	@Test
	public void testDeleteFile() throws URISyntaxException {
		ObjectListing objectListing = client.listObjects(PathDetailOs.getBucket());
		objectListing.getObjectSummaries().forEach(objsum -> System.out.println(objsum.getKey()));
		
		client.putObject("novelbio", "test/dir/exist", new File("/home/novelbio/git/jsr203-aliyun/src/test/resources/testFile/small.txt"));
		client.putObject("novelbio", "test/dir/exist2", new File("/home/novelbio/git/jsr203-aliyun/src/test/resources/testFile/big.bam"));
		OSSException exception = null;
		OssFileSystem ossFileSystem = new OssFileSystem(new OssFileSystemProvider(), new URI(""));
		String file = "small.txt";
		ossFileSystem.deleteFile(new File("http://" + PathDetailOs.getBucket() + "." + PathDetailOs.getEndpoint() + "/" + file).toPath());
		try {
			OSSObject obj = client.getObject("novelbio", "test/dir/.exist");
		} catch (Exception e) {
			exception = (OSSException) e;
		}
		
		Assert.assertNotNull(exception);
		Assert.assertEquals("NoSuchKey", exception.getErrorCode());
	}
	
	
}
