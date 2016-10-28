package com.novelbio.jsr203.bos;

import org.junit.Assert;
import org.junit.Test;

public class TestPathDetalOs {
	
	@Test
	public void testChangeOsToLocal() {
		String path = PathDetailOs.getOsSymbol() + "://bucket/mypath/file";
		String local = PathDetailOs.changeOsToLocal(path);
		Assert.assertEquals(PathDetailOs.getOsMountPathWithSep() + "mypath/file", local);
		
		path = "/home/novelbio/test";
		local = PathDetailOs.changeOsToLocal(path);
		Assert.assertEquals(path, local);
	}
	
}
