package com.novelbio.jsr203.bos;

import java.io.IOException;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;

import com.aliyun.oss.model.OSSObject;
import com.aliyun.oss.model.ObjectMetadata;


public class OssFileAttrbuteView implements BasicFileAttributeView {

	private final OssPath path;
	
	private ObjectMetadata objectMetadata = null;
	
	public OssFileAttrbuteView(OssPath bosPath) {
		this.path = bosPath;
	}
	
	@Override
	public String name() {
		return OssFileSystemProvider.SCHEME;
	}

	/* 
	 * 读取文件的属性
	 * @see java.nio.file.attribute.BasicFileAttributeView#readAttributes()
	 */
	@Override
	public BasicFileAttributes readAttributes() throws IOException {
		if (objectMetadata == null) {
			objectMetadata = path.getFileSystem().getOss().getObjectMetadata(PathDetailOs.getBucket(), path.getInternalPath());
			if (objectMetadata == null && !path.getInternalPath().endsWith("/")) {
				objectMetadata = path.getFileSystem().getOss().getObjectMetadata(PathDetailOs.getBucket(), path.getInternalPath() + "/");
			}
		}
		
		return new OssFileAttributes(path.getInternalPath(), objectMetadata);
	}

	/* 
	 * 设置文件或路径的最后修改时间,最后访问时间,创建时间
	 * @see java.nio.file.attribute.BasicFileAttributeView#setTimes(java.nio.file.attribute.FileTime, java.nio.file.attribute.FileTime, java.nio.file.attribute.FileTime)
	 */
	@Override
	public void setTimes(FileTime lastModifiedTime, FileTime lastAccessTime, FileTime createTime) throws IOException {
		// oss的不能设置,oos会自动处理.
	}

}
