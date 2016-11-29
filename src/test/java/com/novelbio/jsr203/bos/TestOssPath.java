package com.novelbio.jsr203.bos;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;

import org.junit.Assert;
import org.junit.Test;

public class TestOssPath {

	@Test
	public void testGetParent() throws URISyntaxException {
		String pathStr = "oss://novelbio/nbCloud/public/rawData/A__2016-09/project_57ea175c45ce95f1d60f8af5/small.txt";
		Path path = new OssFileSystemProvider().getPath(new URI(pathStr));
		path = path.getParent();
		Assert.assertEquals("oss://novelbio/nbCloud/public/rawData/A__2016-09/project_57ea175c45ce95f1d60f8af5/", path.toString());
		for (int i = 0; i < 10; i++) {
			path = path.getParent();
		}
		Assert.assertEquals("oss://novelbio/", path.toString());
		
	}

}
