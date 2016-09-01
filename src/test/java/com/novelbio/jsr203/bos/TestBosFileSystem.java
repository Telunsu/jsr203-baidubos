package com.novelbio.jsr203.bos;

import org.junit.Assert;
import org.junit.Test;

import com.baidubce.services.bos.BosClient;
import com.baidubce.services.bos.model.BosObject;
import com.novelbio.jsr203.bos.BosFileSystem;


public class TestBosFileSystem {
	BosClient client = BosInitiator.getClient();
	
	@Test
	public void testCreateDirectory() {
		BosFileSystem bosFileSystem = new BosFileSystem();
		bosFileSystem.createDirectory("bos:/novelbio/test/dir", null);
		BosObject obj = client.getObject("novelbio", "test/dir/.exist");
		Assert.assertEquals("test/dir/.exist", obj.getKey());
	}
	
	@Test
	public void testDeleteFile() {
		client.putObject("novelbio", "test/dir/.exist", new byte[]{1});

		BosFileSystem bosFileSystem = new BosFileSystem();
		bosFileSystem.deleteFile("bos:/novelbio/test/dir/.exist");
		Assert.assertEquals("test/dir/.exist", obj.getKey());
	}
}
