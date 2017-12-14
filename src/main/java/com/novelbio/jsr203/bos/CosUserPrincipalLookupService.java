package com.novelbio.jsr203.bos;

import java.io.IOException;
import java.nio.file.attribute.GroupPrincipal;
import java.nio.file.attribute.UserPrincipal;
import java.nio.file.attribute.UserPrincipalLookupService;

/**
 * 百度bos 用户查找服务
 * 
 * @author novelbio
 *
 */
public class CosUserPrincipalLookupService extends UserPrincipalLookupService {

	private CosFileSystem bosFileSystem;
	
	public CosUserPrincipalLookupService(CosFileSystem bosFileSystem) {
		this.bosFileSystem = bosFileSystem;
	}
	
	@Override
	public UserPrincipal lookupPrincipalByName(String name) throws IOException {
//		return new BosUserPrincipal(this.bosFileSystem, name);
		return null;
	}

	@Override
	public GroupPrincipal lookupPrincipalByGroupName(String group) throws IOException {
//		return new BosGroupPrincipal(this.bosFileSystem, group);
		return null;
	}

}
