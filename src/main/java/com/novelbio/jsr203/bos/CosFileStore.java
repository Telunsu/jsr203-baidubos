package com.novelbio.jsr203.bos;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.attribute.FileStoreAttributeView;

public class CosFileStore extends FileStore {
	
	private CosFileSystem cosFileSystem;

	public CosFileStore(CosPath cosPath) {
		this.cosFileSystem = cosPath.getFileSystem();
	}
	
	@Override
	public String name() {
		 return PathDetailOs.getEndpoint();
	}

	@Override
	public String type() {
		  return "cos";
	}

	@Override
	public boolean isReadOnly() {
		return this.cosFileSystem.isReadOnly();
	}

	@Override
	public long getTotalSpace() throws IOException {
		return Long.MAX_VALUE;
	}

	@Override
	public long getUsableSpace() throws IOException {
//		return 0;
		throw new RuntimeException("unsupported");
	}

	@Override
	public long getUnallocatedSpace() throws IOException {
		return Long.MAX_VALUE;
	}

	/* 
	 * 检测文件系统是否支持自定义属性  
	 * @see java.nio.file.FileStore#supportsFileAttributeView(java.lang.Class)
	 */
	@Override
	public boolean supportsFileAttributeView(Class<? extends FileAttributeView> type) {
		return false;
	}

	@Override
	public boolean supportsFileAttributeView(String name) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public <V extends FileStoreAttributeView> V getFileStoreAttributeView(Class<V> type) {
		// TODO 
		return null;
	}

	@Override
	public Object getAttribute(String attribute) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

}
