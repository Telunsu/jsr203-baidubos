package com.novelbio.jsr203.bos;

import java.nio.file.attribute.GroupPrincipal;

public class OssGroupPrincipal implements GroupPrincipal {

	public OssGroupPrincipal(OssFileSystem bosFileSystem, String group) {
	}

	@Override
	public String getName() {
		return null;
	}

}
