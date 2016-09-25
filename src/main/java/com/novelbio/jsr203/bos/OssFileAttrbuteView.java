package com.novelbio.jsr203.bos;

import java.io.IOException;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;

import com.aliyun.oss.model.OSSObject;


public class OssFileAttrbuteView implements BasicFileAttributeView {

	private final OssPath path;
	
	public OssFileAttrbuteView(OssPath bosPath) {
		this.path = bosPath;
	}
	
	@Override
	public String name() {
		return "oss";
	}

	/* 
	 * 读取文件的属性
	 * @see java.nio.file.attribute.BasicFileAttributeView#readAttributes()
	 */
	@Override
	public BasicFileAttributes readAttributes() throws IOException {
//		String key = path.toFile().getAbsolutePath();
//		GenericRequest getObjMetadata = new GenericRequest(PathDetail.getBucket(), key);
//		ObjectListing lsObjResponse = path.getFileSystem().getBos().listObjects(PathDetail.getBucket(), key);
//		
//		Optional<OSSObjectSummary> bosObjSummary = null;
//		if (lsObjResponse.getObjectSummaries() != null) {
//			bosObjSummary = lsObjResponse.getObjectSummaries().stream().findFirst();
//		}
//		ObjectMetadata ObjectMetadata = path.getFileSystem().getBos().getObjectMetadata(getObjMetadata);
		
		OSSObject ossObject = OssInitiator.getClient().getObject(PathDetailOs.getBucket(), path.toString());
		
		return new OssFileAttributes(ossObject);
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
