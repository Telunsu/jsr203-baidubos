package com.novelbio.jsr203.bos;

import java.io.IOException;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.Optional;

import com.baidubce.services.bos.model.BosObjectSummary;
import com.baidubce.services.bos.model.GetObjectMetadataRequest;
import com.baidubce.services.bos.model.ListObjectsResponse;
import com.baidubce.services.bos.model.ObjectMetadata;


public class BosFileAttrbuteView implements BasicFileAttributeView {

	private final BosPath path;
	
	public BosFileAttrbuteView(BosPath bosPath) {
		this.path = bosPath;
	}
	
	@Override
	public String name() {
		return "bos";
	}

	/* 
	 * 读取文件的属性
	 * @see java.nio.file.attribute.BasicFileAttributeView#readAttributes()
	 */
	@Override
	public BasicFileAttributes readAttributes() throws IOException {
		String key = path.toFile().getAbsolutePath();
		GetObjectMetadataRequest getObjMetadata = new GetObjectMetadataRequest(PathDetail.getBucket(), key);
		ListObjectsResponse lsObjResponse = path.getFileSystem().getBos().listObjects(PathDetail.getBucket(), key);
		
		Optional<BosObjectSummary> bosObjSummary = null;
		if (lsObjResponse.getContents() != null) {
			bosObjSummary = lsObjResponse.getContents().stream().findFirst();
		}
		ObjectMetadata ObjectMetadata = path.getFileSystem().getBos().getObjectMetadata(getObjMetadata);
		
		return new BosFileAttributes(bosObjSummary.get(), ObjectMetadata);
	}

	/* 
	 * 设置文件或路径的最后修改时间,最后访问时间,创建时间
	 * @see java.nio.file.attribute.BasicFileAttributeView#setTimes(java.nio.file.attribute.FileTime, java.nio.file.attribute.FileTime, java.nio.file.attribute.FileTime)
	 */
	@Override
	public void setTimes(FileTime lastModifiedTime, FileTime lastAccessTime, FileTime createTime) throws IOException {
		// bos的不能设置,bos会自动处理.
	}

}
