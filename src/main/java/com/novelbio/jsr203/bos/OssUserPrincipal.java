package com.novelbio.jsr203.bos;

import java.nio.file.attribute.UserPrincipal;

public class OssUserPrincipal implements UserPrincipal {

	public OssUserPrincipal(OssFileSystem bosFileSystem, String name) {
	}

	@Override
	public String getName() {
		return null;
	}

}
