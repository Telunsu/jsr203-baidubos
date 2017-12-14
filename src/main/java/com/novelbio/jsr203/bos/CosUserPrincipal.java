package com.novelbio.jsr203.bos;

import java.nio.file.attribute.UserPrincipal;

public class CosUserPrincipal implements UserPrincipal {

	public CosUserPrincipal(CosFileSystem bosFileSystem, String name) {
	}

	@Override
	public String getName() {
		return null;
	}

}
