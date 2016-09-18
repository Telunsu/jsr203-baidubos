package com.novelbio.jsr203.bos;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.baidubce.BceServiceException;
import com.baidubce.services.bos.BosClient;
import com.baidubce.services.bos.model.BosObject;


public class TestBosFileSystem {
	
	BosClient client = BosInitiator.getClient();
	@Rule //暂时不清楚这个annotation有什么用，待查证
	public ExpectedException thrown = ExpectedException.none();
	
	@Test
	public void testCreateDirectory() {
		BosFileSystem bosFileSystem = new BosFileSystem(new BosFileSystemProvider());
		bosFileSystem.createDirectory("bos:/test/dir", null);
		BosObject obj = client.getObject("novelbio", "test/dir/.exist");
		Assert.assertEquals("test/dir/.exist", obj.getKey());
	}
	
	@Test
	public void testDeleteFile() {
		BceServiceException exception = null;
		client.putObject("novelbio", "test/dir/.exist", new byte[]{1});

		BosFileSystem bosFileSystem = new BosFileSystem(new BosFileSystemProvider());
		bosFileSystem.deleteFile("bos:/test/dir/.exist");
		try {
			BosObject obj = client.getObject("novelbio", "test/dir/.exist");

		} catch (BceServiceException e) {
			exception = e;
		}
		
		Assert.assertNotNull(exception);
		Assert.assertEquals("NoSuchKey", exception.getErrorCode());
	}
}
